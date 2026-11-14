#!/usr/bin/env python3
"""Build a simple Taiwan-stock day-trade pool (TWSE + TPEX) from official open data.

Design goals: simple, robust, never all-or-nothing.
- Every source is fetched independently (3 attempts, backoff, timeout, size cap).
- A source that fails falls back to its last good copy in data/cache/<ID>.json
  and is reported as "stale_cache"; one failing source never crashes the run.
- Output: pool/<target>.json and pool/latest.json (schema SIMPLE_POOL_V1).

Python 3.12 standard library only.
"""

from __future__ import annotations

import html
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
POOL_DIR = ROOT / "pool"
CACHE_DIR = ROOT / "data" / "cache"
TAIPEI = timezone(timedelta(hours=8))  # Taiwan has no DST.
SCHEMA = "SIMPLE_POOL_V1"

ATTEMPTS = 3
BACKOFF_SECONDS = (20, 40)  # sleep before attempt 2 and attempt 3
TIMEOUT_SECONDS = 45
RUN_BUDGET_SECONDS = 14 * 60  # after this, no more retries (use cache instead)
KEEP_POOL_FILES = 30  # dated pool files kept in pool/ (git history keeps the rest)

MIN_CLOSE = 20.0
MAX_CLOSE = 600.0
MIN_VOLUME_SHARES = 500_000

URLS = {
    "TWSE_HOLIDAY": "https://openapi.twse.com.tw/v1/holidaySchedule/holidaySchedule",
    "TWSE_COMPANY": "https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
    "TWSE_DAILY": "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
    "TWSE_CHANGED": "https://openapi.twse.com.tw/v1/exchangeReport/TWT85U",
    "TWSE_DISPOSITION": "https://openapi.twse.com.tw/v1/announcement/punish",
    "TWSE_HALT": "https://openapi.twse.com.tw/v1/exchangeReport/TWTAWU",
    "TWSE_DAYTRADE": "https://openapi.twse.com.tw/v1/exchangeReport/TWTB4U",
    "TPEX_COMPANY": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O",
    "TPEX_DAILY": "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes",
    "TPEX_STATUS": "https://www.tpex.org.tw/openapi/v1/tpex_cmode",
    "TPEX_DISPOSITION": "https://www.tpex.org.tw/openapi/v1/tpex_disposal_information",
    "TPEX_HALT": "https://www.tpex.org.tw/openapi/v1/tpex_spendi_history",
    "TPEX_DAYTRADE": "https://www.tpex.org.tw/www/zh-tw/intraday/list?date={date}&response=json",
    "TPEX_ISIN": "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4",
}
# www.twse.com.tw fallbacks, used only when the OpenAPI copy is older than needed.
TWSE_WWW_DAILY = (
    "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={ymd}&type=ALLBUT0999&response=json",
)
TWSE_WWW_DAYTRADE = "https://www.twse.com.tw/rwd/zh/dayTrading/TWTB4U?date={ymd}&selectType=All&response=json"
# Margin / short balances of data_date (optional: a failure only leaves "margin_rows" empty with a status and a warning).
# Units: lots (張). TWSE publishes in the evening; the morning runs (06:40 / 07:40) always see it.
MARGIN_URLS = {
    "TWSE": ("https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={ymd}&selectType=ALL&response=json",),
    "TPEX": ("https://www.tpex.org.tw/www/zh-tw/margin/balance?date={slash}&response=json",
             "https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=json&d={roc}"),
}
# Only 4-digit codes (optionally with a letter suffix) are evaluated; warrants,
# bonds and 5/6-digit ETFs are out of scope and not listed in "excluded".
STOCK_SCOPE = re.compile(r"\d{4}[A-Z]?")
SIZE_CAP = {"TPEX_ISIN": 48 * 1024 * 1024}
DEFAULT_SIZE_CAP = 24 * 1024 * 1024
MIN_ROWS = {
    "TWSE_HOLIDAY": 5,
    "TWSE_COMPANY": 300,
    "TWSE_DAILY": 300,
    "TWSE_DAYTRADE": 100,
    "TPEX_COMPANY": 300,
    "TPEX_DAILY": 300,
    "TPEX_DAYTRADE": 100,
    "TPEX_ISIN": 300,
}
TPEX_STATUS_FIELDS = ("AlteredTrading", "PeriodicTrading", "ManagedStock", "SuspensionOfTrading")

_DEADLINE = [time.monotonic() + RUN_BUDGET_SECONDS]
_LOG_LOCK = threading.Lock()


def log(message: str) -> None:
    stamp = datetime.now(TAIPEI).strftime("%H:%M:%S")
    with _LOG_LOCK:
        print(f"[{stamp}] {message}", flush=True)


# ---------------------------------------------------------------- parsing ---

def parse_date(value) -> date | None:
    """Normalize ROC 7-digit (1151105), AD 8-digit (20261105) or separated dates."""
    text = str(value or "").strip()
    if not text:
        return None
    groups = re.findall(r"\d+", text)
    try:
        if len(groups) == 1:
            g = groups[0]
            if len(g) == 7:
                y, m, d = int(g[:3]), int(g[3:5]), int(g[5:7])
            elif len(g) == 8:
                y, m, d = int(g[:4]), int(g[4:6]), int(g[6:8])
            else:
                return None
        elif len(groups) >= 3:
            y, m, d = int(groups[0]), int(groups[1]), int(groups[2])
        else:
            return None
        if y < 1911:
            y += 1911
        return date(y, m, d)
    except ValueError:
        return None


def parse_period(value) -> tuple[date, date] | None:
    """Parse '115/11/03~115/11/14' (or similar) into (start, end)."""
    tokens = re.findall(r"\d{2,4}[/.\-]\d{1,2}[/.\-]\d{1,2}|\d{7,8}", str(value or ""))
    dates = [d for d in (parse_date(t) for t in tokens) if d]
    if not dates:
        return None
    return dates[0], dates[-1]


def parse_number(value) -> float | None:
    text = str(value if value is not None else "").replace(",", "").strip()
    if not text or set(text) <= {"-", "－", "X", "x", " "}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def is_marked(value) -> bool:
    text = str(value or "").strip().replace("Ｙ", "Y").replace("＊", "*").upper()
    return text in {"Y", "*", "V", "YES", "是"}


def clean_code(value) -> str:
    return str(value or "").strip().upper()


def fmt(d: date | None) -> str | None:
    return d.isoformat() if d else None


# ------------------------------------------------------------ calendar ------

def holidays_from_rows(rows: list[dict]) -> set[date]:
    result = set()
    for row in rows:
        text = f"{row.get('Name', '')}{row.get('Description', '')}"
        if any(k in text for k in ("開始交易", "最後交易", "照常交易")):
            continue  # these entries are trading days
        d = parse_date(row.get("Date"))
        if d:
            result.add(d)
    return result


def is_trading_day(d: date, holidays: set[date]) -> bool:
    return d.weekday() < 5 and d not in holidays


def next_trading_day(d: date, holidays: set[date]) -> date:
    d += timedelta(days=1)
    while not is_trading_day(d, holidays):
        d += timedelta(days=1)
    return d


def previous_trading_day(d: date, holidays: set[date]) -> date:
    d -= timedelta(days=1)
    while not is_trading_day(d, holidays):
        d -= timedelta(days=1)
    return d


def resolve_dates(now: datetime, holidays: set[date]) -> tuple[date, date]:
    """target = next session to trade; data_date = trading day before target."""
    today = now.astimezone(TAIPEI).date()
    if now.astimezone(TAIPEI).hour < 14 and is_trading_day(today, holidays):
        target = today
    else:
        target = next_trading_day(today, holidays)
    return target, previous_trading_day(target, holidays)


# ------------------------------------------------------------- fetching -----

def http_get(url: str, cap: int) -> bytes:
    request = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; tw-daytrade-pool/1.0; +https://github.com/ks162496-netizen/tpex-disposition-relay)",
        "Accept": "application/json, text/html;q=0.9, */*;q=0.5",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.5",
        "Accept-Encoding": "identity",
    })
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        status = response.getcode()
        if status != 200:
            raise RuntimeError(f"HTTP {status}")
        body = response.read(cap + 1)
    if len(body) > cap:
        raise RuntimeError(f"body larger than cap {cap} bytes")
    return body


def with_retries(label: str, func):
    """Run func() up to ATTEMPTS times with backoff; return (value, error_text)."""
    last_error = None
    for attempt in range(1, ATTEMPTS + 1):
        try:
            return func(), None
        except Exception as error:  # noqa: BLE001 - every failure is retried/reported
            last_error = f"{type(error).__name__}: {error}"[:300]
            log(f"{label}: attempt {attempt}/{ATTEMPTS} failed: {last_error}")
            if attempt >= ATTEMPTS:
                break
            pause = BACKOFF_SECONDS[min(attempt - 1, len(BACKOFF_SECONDS) - 1)]
            if time.monotonic() + pause > _DEADLINE[0]:
                log(f"{label}: run budget exhausted, no more retries")
                break
            time.sleep(pause)
    return None, last_error


def decode_json(raw: bytes):
    text = raw.decode("utf-8-sig")
    if text.lstrip()[:1] == "<":
        raise RuntimeError("got HTML instead of JSON")
    return json.loads(text)


def json_rows(raw: bytes) -> list[dict]:
    data = decode_json(raw)
    if not isinstance(data, list):
        raise RuntimeError(f"expected JSON array, got {type(data).__name__}")
    return [row for row in data if isinstance(row, dict)]


# Normalizers: raw source rows -> small cached rows with only needed fields.

def norm_holiday(rows):
    return [{"date": fmt(parse_date(r.get("Date"))), "name": str(r.get("Name", "")).strip(),
             "description": str(r.get("Description", "")).strip()} for r in rows if parse_date(r.get("Date"))]


def norm_twse_company(rows):
    return [{"code": clean_code(r.get("公司代號")), "name": str(r.get("公司簡稱", "")).strip(),
             "industry": str(r.get("產業別", "")).strip(), "date": fmt(parse_date(r.get("出表日期")))}
            for r in rows if clean_code(r.get("公司代號"))]


def norm_twse_daily(rows):
    return [{"code": clean_code(r.get("Code")), "name": str(r.get("Name", "")).strip(),
             "date": fmt(parse_date(r.get("Date"))), "close": parse_number(r.get("ClosingPrice")),
             "volume": parse_number(r.get("TradeVolume"))} for r in rows if STOCK_SCOPE.fullmatch(clean_code(r.get("Code")))]


def norm_code_only(rows, key="Code"):
    return [{"code": clean_code(r.get(key))} for r in rows if clean_code(r.get(key))]


def norm_disposition(rows, key):
    out = []
    for r in rows:
        period = parse_period(r.get("DispositionPeriod"))
        if clean_code(r.get(key)):
            out.append({"code": clean_code(r.get(key)), "start": fmt(period[0]) if period else None,
                        "end": fmt(period[1]) if period else None,
                        "raw_period": str(r.get("DispositionPeriod", "")).strip()})
    return out


def norm_halt(rows, key, halt_key, resume_key):
    return [{"code": clean_code(r.get(key)), "halt": fmt(parse_date(r.get(halt_key))),
             "resume": fmt(parse_date(r.get(resume_key)))} for r in rows if clean_code(r.get(key))]


def norm_twse_daytrade(rows):
    return [{"code": clean_code(r.get("Code")), "date": fmt(parse_date(r.get("Date"))),
             "sell_first_suspended": is_marked(r.get("Suspension"))} for r in rows if clean_code(r.get("Code"))]


def norm_tpex_company(rows):
    return [{"code": clean_code(r.get("SecuritiesCompanyCode")), "name": str(r.get("CompanyAbbreviation", "")).strip(),
             "industry": str(r.get("SecuritiesIndustryCode", "")).strip(), "date": fmt(parse_date(r.get("Date")))}
            for r in rows if clean_code(r.get("SecuritiesCompanyCode"))]


def norm_tpex_daily(rows):
    return [{"code": clean_code(r.get("SecuritiesCompanyCode")), "name": str(r.get("CompanyName", "")).strip(),
             "date": fmt(parse_date(r.get("Date"))), "close": parse_number(r.get("Close")),
             "volume": parse_number(r.get("TradingShares"))} for r in rows
            if STOCK_SCOPE.fullmatch(clean_code(r.get("SecuritiesCompanyCode")))]


def norm_tpex_status(rows):
    out = []
    for r in rows:
        code = clean_code(r.get("SecuritiesCompanyCode"))
        if code:
            flags = sorted(f for f in TPEX_STATUS_FIELDS if is_marked(r.get(f)))
            out.append({"code": code, "date": fmt(parse_date(r.get("Date"))), "flags": flags})
    return out


def parse_tpex_daytrade(raw: bytes, list_date: date) -> list[dict] | None:
    """Return rows, or None when the list for that date is not published (empty)."""
    data = decode_json(raw)
    if not isinstance(data, dict):
        raise RuntimeError("TPEX day-trade response is not a JSON object")
    if str(data.get("stat", "")).lower() != "ok":
        return None
    out = []
    for table in data.get("tables") or []:
        fields = [str(f).strip() for f in table.get("fields") or []]
        rows = table.get("data") or []
        if "證券代號" not in fields:
            continue
        total = table.get("totalCount")
        if total is not None and int(total) != len(rows):
            raise RuntimeError(f"TPEX day-trade totalCount {total} != rows {len(rows)}")
        i_code = fields.index("證券代號")
        i_name = fields.index("證券名稱") if "證券名稱" in fields else None
        i_flag = next((i for i, f in enumerate(fields) if "暫停現股賣出" in f), None)
        for row in rows:
            if not isinstance(row, list) or len(row) <= i_code:
                continue
            code = clean_code(html.unescape(re.sub(r"<[^>]+>", "", str(row[i_code]))))
            if not code:
                continue
            out.append({"code": code, "date": list_date.isoformat(),
                        "name": str(row[i_name]).strip() if i_name is not None and i_name < len(row) else "",
                        "sell_first_suspended": is_marked(row[i_flag]) if i_flag is not None and i_flag < len(row) else False})
    return out or None


def parse_isin_html(raw: bytes) -> list[dict]:
    """Parse ISIN strMode=4 (TPEX) page; return common-stock rows only."""
    text = raw.decode("cp950", errors="replace")
    group = ""
    out = []
    for row_html in re.split(r"<tr[^>]*>", text, flags=re.I)[1:]:
        cells = [html.unescape(re.sub(r"<[^>]+>", "", c)).replace("\xa0", " ").strip()
                 for c in re.findall(r"<td[^>]*>(.*?)(?=<td|$)", row_html, flags=re.S | re.I)]
        if len(cells) == 1:
            group = cells[0].strip()
            continue
        if len(cells) != 7:
            continue
        first = cells[0]
        if "　" not in first:
            continue
        code, _, name = first.partition("　")
        code = clean_code(code)
        market, industry, cfi = cells[3].strip(), cells[4].strip(), cells[5].strip()
        if group == "股票" and cfi == "ESVUFR" and market == "上櫃" and code:
            out.append({"code": code, "name": name.strip(), "industry": industry})
    return out


def source_date_of(rows: list[dict]) -> str | None:
    dates = [r.get("date") for r in rows if r.get("date")]
    return max(dates) if dates else None


def via_json(normalize, *args):
    """raw bytes -> JSON array -> normalized rows; a non-empty array that yields
    no usable rows means the field names changed, which is an error."""
    def run(raw: bytes) -> list[dict]:
        rows = json_rows(raw)
        out = normalize(rows, *args)
        if rows and not out:
            raise RuntimeError(f"{len(rows)} rows but none usable; fields: {sorted(rows[0])[:12]}")
        return out
    return run


SPECS = {
    # ID: function raw bytes -> normalized rows
    "TWSE_HOLIDAY": via_json(norm_holiday),
    "TWSE_COMPANY": via_json(norm_twse_company),
    "TWSE_DAILY": via_json(norm_twse_daily),
    "TWSE_CHANGED": via_json(norm_code_only, "Code"),
    "TWSE_DISPOSITION": via_json(norm_disposition, "Code"),
    "TWSE_HALT": via_json(norm_halt, "Code", "TradingHaltDate", "TradingResumptionDate"),
    "TWSE_DAYTRADE": via_json(norm_twse_daytrade),
    "TPEX_COMPANY": via_json(norm_tpex_company),
    "TPEX_DAILY": via_json(norm_tpex_daily),
    "TPEX_STATUS": via_json(norm_tpex_status),
    "TPEX_DISPOSITION": via_json(norm_disposition, "SecuritiesCompanyCode"),
    "TPEX_HALT": via_json(norm_halt, "SecuritiesCompanyCode", "DateOfSuspendedTrading", "DateOfResumedTrading"),
    "TPEX_ISIN": parse_isin_html,
}


def twse_www_tables(obj: dict) -> list[tuple[list[str], list]]:
    """All (fields, data) tables in a www.twse.com.tw rwd JSON response."""
    found = []
    if isinstance(obj.get("fields"), list) and isinstance(obj.get("data"), list):
        found.append((obj["fields"], obj["data"]))
    for table in obj.get("tables") or []:
        if isinstance(table, dict) and isinstance(table.get("fields"), list) and isinstance(table.get("data"), list):
            found.append((table["fields"], table["data"]))
    for key, value in obj.items():
        m = re.fullmatch(r"fields(\d+)", key)
        if m and isinstance(value, list) and isinstance(obj.get(f"data{m.group(1)}"), list):
            found.append((value, obj[f"data{m.group(1)}"]))
    return [([re.sub(r"<[^>]+>", "", str(f)).strip() for f in fields], data) for fields, data in found]


def cell(row: list, index: int | None) -> str:
    if index is None or index >= len(row):
        return ""
    return html.unescape(re.sub(r"<[^>]+>", "", str(row[index]))).strip()


def twse_www_payload(raw: bytes, want: date) -> dict | None:
    """Parsed response if stat OK and dated `want`; None when not published."""
    obj = decode_json(raw)
    if not isinstance(obj, dict):
        raise RuntimeError("TWSE www response is not a JSON object")
    if str(obj.get("stat", "")).upper() != "OK" or parse_date(obj.get("date")) != want:
        return None
    return obj


def parse_twse_www_daily(raw: bytes, want: date) -> list[dict]:
    obj = twse_www_payload(raw, want)
    if obj is None:
        return []
    for fields, data in twse_www_tables(obj):
        if {"證券代號", "收盤價", "成交股數"} <= set(fields):
            i_code, i_close, i_vol = fields.index("證券代號"), fields.index("收盤價"), fields.index("成交股數")
            i_name = fields.index("證券名稱") if "證券名稱" in fields else None
            return [{"code": clean_code(cell(r, i_code)), "name": cell(r, i_name), "date": want.isoformat(),
                     "close": parse_number(cell(r, i_close)), "volume": parse_number(cell(r, i_vol))}
                    for r in data if isinstance(r, list) and STOCK_SCOPE.fullmatch(clean_code(cell(r, i_code)))]
    return []


def parse_twse_www_daytrade(raw: bytes, want: date) -> list[dict]:
    obj = twse_www_payload(raw, want)
    if obj is None:
        return []
    for fields, data in twse_www_tables(obj):
        if "證券代號" in fields:
            i_code = fields.index("證券代號")
            i_flag = next((i for i, f in enumerate(fields) if "暫停現股賣出" in f), None)
            return [{"code": clean_code(cell(r, i_code)), "date": want.isoformat(),
                     "sell_first_suspended": is_marked(cell(r, i_flag))}
                    for r in data if isinstance(r, list) and clean_code(cell(r, i_code))]
    return []


def fetch_twse_www(source_id: str, urls, parser, want: date) -> tuple[list[dict] | None, dict | None]:
    """Try www.twse.com.tw URLs for date `want`; return (rows, status) or (None, None)."""
    for template in urls:
        url = template.format(ymd=want.strftime("%Y%m%d"))
        time.sleep(3)  # www.twse.com.tw rate-limits bursts
        rows, error = with_retries(f"{source_id}[www {want}]", lambda u=url: parser(http_get(u, DEFAULT_SIZE_CAP), want))
        if rows and len(rows) >= MIN_ROWS.get(source_id, 0):
            fetched_at = now_text()
            save_cache(source_id, rows, want.isoformat(), fetched_at)
            log(f"{source_id}: fresh from www.twse.com.tw for {want}, {len(rows)} rows")
            return rows, {"status": "fresh", "source_date": want.isoformat(), "rows": len(rows),
                          "fetched_at": fetched_at, "error": None, "note": f"www fallback: {url}"}
        log(f"{source_id}: www {want} not usable ({error or f'{len(rows or [])} rows'})")
    return None, None


def parse_margin_tables(obj: dict, want: date) -> list[dict]:
    """TWSE / TPEX rwd-style margin tables -> rows [code, margin_today, margin_prev, short_today, short_prev] (lots).
    The two '今日餘額' columns are margin then short (TWSE order: 代號, 名稱, 融資 6 cols, 融券 6 cols, 資券互抵, 註記)."""
    for fields, data in twse_www_tables(obj):
        code_i = next((i for i, f in enumerate(fields) if f in ("代號", "股票代號", "證券代號")), None)
        today = [i for i, f in enumerate(fields) if "今日餘額" in f or f.startswith(("資餘額", "券餘額"))]
        prev = [i for i, f in enumerate(fields) if "前日餘額" in f or f.startswith(("前資餘額", "前券餘額"))]
        if code_i is None or len(today) < 2 or len(prev) < 2:
            continue
        out = []
        for r in data:
            if not isinstance(r, list):
                continue
            code = clean_code(cell(r, code_i))
            if not STOCK_SCOPE.fullmatch(code):
                continue
            vals = [parse_number(cell(r, i)) for i in (today[0], prev[0], today[1], prev[1])]
            if any(v is None for v in vals):
                continue
            out.append({"code": code, "date": want.isoformat(), "m": int(vals[0]), "m_prev": int(vals[1]), "s": int(vals[2]), "s_prev": int(vals[3])})
        if out:
            return out
    return []


def margin_payload(raw: bytes, want: date) -> dict | None:
    obj = decode_json(raw)
    if not isinstance(obj, dict):
        raise RuntimeError("margin response is not a JSON object")
    stat = str(obj.get("stat", obj.get("status", "ok"))).lower()
    when = parse_date(obj.get("date") or obj.get("reportDate") or "")
    if stat not in ("ok", "") or (when is not None and when != want):
        return None
    if "aaData" in obj and isinstance(obj["aaData"], list):  # old TPEX php shape: fields are implied
        obj = {"tables": [{"fields": ["代號", "名稱", "前資餘額", "資買", "資賣", "現償", "資今日餘額", "資屬證金", "資使用率", "資限額",
                                      "前券餘額", "券賣", "券買", "券償", "券今日餘額", "券屬證金", "券使用率", "券限額", "資券相抵", "備註"],
                           "data": obj["aaData"]}]}
        obj["tables"][0]["fields"] = [f.replace("前資餘額", "融資前日餘額").replace("前券餘額", "融券前日餘額") for f in obj["tables"][0]["fields"]]
    return obj


def fetch_margin(market: str, want: date) -> tuple[list[dict], dict]:
    """Optional margin source; never raises; status 'fresh' / 'not_published' / 'failed'."""
    errors = []
    for template in MARGIN_URLS[market]:
        url = template.format(ymd=want.strftime("%Y%m%d"), slash=want.strftime("%Y/%m/%d"),
                              roc=f"{want.year - 1911}/{want:%m/%d}")
        if "twse.com.tw" in url:
            time.sleep(3)  # www.twse.com.tw rate-limits bursts
        rows, error = with_retries(f"{market}_MARGIN[{want}]", lambda u=url: (
            (lambda obj: parse_margin_tables(obj, want) if obj is not None else [])(margin_payload(http_get(u, DEFAULT_SIZE_CAP), want))))
        if rows and len(rows) >= 100:
            log(f"{market}_MARGIN: fresh for {want}, {len(rows)} rows")
            return rows, {"status": "fresh", "source_date": want.isoformat(), "rows": len(rows), "error": None, "url": url}
        errors.append(error or f"{len(rows or [])} rows")
    status = "failed" if any(e and not e.endswith(" rows") for e in errors) else "not_published"
    return [], {"status": status, "source_date": None, "rows": 0, "error": "; ".join(e for e in errors if e)[:300] or None}


def prefer_newer_cache(source_id: str, rows, status: dict, limit: date):
    """If the cache holds a newer copy (<= limit) than what we fetched, use the cache."""
    cached = load_cache(source_id)
    current = source_date_of(rows or []) or ""
    if cached and current < (cached.get("source_date") or "") <= limit.isoformat():
        log(f"{source_id}: cached copy {cached['source_date']} is newer than fetched {current or 'none'}; using cache")
        return cached["rows"], {"status": "stale_cache", "source_date": cached["source_date"],
                                "rows": len(cached["rows"]), "fetched_at": cached.get("fetched_at"),
                                "error": status.get("error") or f"fetched copy is older ({current or 'none'})"}
    return rows, status


def refresh_twse(data: dict, sources: dict, target: date, data_date: date) -> None:
    """OpenAPI can lag a day; fill TWSE daily quotes / day-trade list from www.twse.com.tw."""
    if (source_date_of(data.get("TWSE_DAILY") or []) or "") < data_date.isoformat():
        rows, status = fetch_twse_www("TWSE_DAILY", TWSE_WWW_DAILY, parse_twse_www_daily, data_date)
        if rows:
            data["TWSE_DAILY"], sources["TWSE_DAILY"] = rows, status
    data["TWSE_DAILY"], sources["TWSE_DAILY"] = prefer_newer_cache(
        "TWSE_DAILY", data.get("TWSE_DAILY"), sources["TWSE_DAILY"], data_date)
    for want in (target, data_date):
        if (source_date_of(data.get("TWSE_DAYTRADE") or []) or "") >= want.isoformat():
            break
        rows, status = fetch_twse_www("TWSE_DAYTRADE", (TWSE_WWW_DAYTRADE,), parse_twse_www_daytrade, want)
        if rows:
            data["TWSE_DAYTRADE"], sources["TWSE_DAYTRADE"] = rows, status
            break
    data["TWSE_DAYTRADE"], sources["TWSE_DAYTRADE"] = prefer_newer_cache(
        "TWSE_DAYTRADE", data.get("TWSE_DAYTRADE"), sources["TWSE_DAYTRADE"], target)


# --------------------------------------------------------------- cache ------

def cache_path(source_id: str) -> Path:
    return CACHE_DIR / f"{source_id}.json"


def load_cache(source_id: str) -> dict | None:
    try:
        data = json.loads(cache_path(source_id).read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("rows"), list):
            return data
    except (OSError, ValueError):
        pass
    return None


def save_cache(source_id: str, rows: list[dict], source_date: str | None, fetched_at: str) -> None:
    old = load_cache(source_id)
    if old is not None:
        if old.get("rows") == rows:
            return  # unchanged: keep file (and its fetched_at) to avoid churn
        if old.get("source_date") and source_date and source_date < old["source_date"]:
            return  # never replace a newer good copy with an older one
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    lines = ",\n".join("    " + json.dumps(r, ensure_ascii=False, sort_keys=True, separators=(",", ":")) for r in rows)
    text = (
        "{\n"
        f'  "id": {json.dumps(source_id)},\n'
        f'  "source_date": {json.dumps(source_date)},\n'
        f'  "fetched_at": {json.dumps(fetched_at)},\n'
        f'  "rows": [\n{lines}\n  ]\n'
        "}\n"
    )
    tmp = cache_path(source_id).with_suffix(".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(cache_path(source_id))


def now_text() -> str:
    return datetime.now(TAIPEI).replace(microsecond=0).isoformat()


def status_from_cache(source_id: str, error: str | None) -> tuple[list[dict] | None, dict]:
    cached = load_cache(source_id)
    if cached is None:
        log(f"{source_id}: FAILED and no cache available")
        return None, {"status": "failed", "source_date": None, "rows": 0, "fetched_at": None, "error": error}
    log(f"{source_id}: using stale cache from {cached.get('fetched_at')} (source_date {cached.get('source_date')})")
    return cached["rows"], {"status": "stale_cache", "source_date": cached.get("source_date"),
                            "rows": len(cached["rows"]), "fetched_at": cached.get("fetched_at"), "error": error}


def fetch_source(source_id: str) -> tuple[list[dict] | None, dict]:
    """Fetch + normalize one source; fall back to cache on final failure."""
    normalize = SPECS[source_id]
    cap = SIZE_CAP.get(source_id, DEFAULT_SIZE_CAP)

    def attempt():
        rows = normalize(http_get(URLS[source_id], cap))
        if len(rows) < MIN_ROWS.get(source_id, 0):
            raise RuntimeError(f"only {len(rows)} usable rows (< {MIN_ROWS[source_id]})")
        return rows

    rows, error = with_retries(source_id, attempt)
    if rows is None:
        return status_from_cache(source_id, error)
    fetched_at = now_text()
    sdate = source_date_of(rows)
    save_cache(source_id, rows, sdate, fetched_at)
    log(f"{source_id}: fresh, {len(rows)} rows, source_date {sdate}")
    return rows, {"status": "fresh", "source_date": sdate, "rows": len(rows), "fetched_at": fetched_at, "error": None}


def fetch_tpex_daytrade(target: date, holidays: set[date]) -> tuple[list[dict] | None, dict]:
    """Fetch the TPEX day-trade list for target; if not yet published, walk back to older lists."""
    list_date = target
    errors = []
    for _ in range(4):
        url = URLS["TPEX_DAYTRADE"].format(date=list_date.strftime("%Y/%m/%d"))
        result, error = with_retries(f"TPEX_DAYTRADE[{list_date}]", lambda u=url, d=list_date: (
            parse_tpex_daytrade(http_get(u, DEFAULT_SIZE_CAP), d) or []))
        if error:
            errors.append(f"{list_date}: {error}")
            break
        if len(result) >= MIN_ROWS["TPEX_DAYTRADE"]:
            fetched_at = now_text()
            save_cache("TPEX_DAYTRADE", result, list_date.isoformat(), fetched_at)
            cached = load_cache("TPEX_DAYTRADE")
            # A newer list in cache (should not happen) wins over an older fresh one.
            if cached and (cached.get("source_date") or "") > list_date.isoformat():
                return cached["rows"], {"status": "stale_cache", "source_date": cached["source_date"],
                                        "rows": len(cached["rows"]), "fetched_at": cached.get("fetched_at"),
                                        "error": "; ".join(errors) or None}
            log(f"TPEX_DAYTRADE: fresh list for {list_date}, {len(result)} rows")
            return result, {"status": "fresh", "source_date": list_date.isoformat(), "rows": len(result),
                            "fetched_at": fetched_at, "error": "; ".join(errors) or None}
        log(f"TPEX_DAYTRADE: list for {list_date} not published ({len(result)} rows); trying previous trading day")
        errors.append(f"{list_date}: not published")
        list_date = previous_trading_day(list_date, holidays)
    cached = load_cache("TPEX_DAYTRADE")
    if cached and (cached.get("source_date") or "") <= target.isoformat():
        return status_from_cache("TPEX_DAYTRADE", "; ".join(errors) or None)
    return None, {"status": "failed", "source_date": None, "rows": 0, "fetched_at": None,
                  "error": "; ".join(errors) or "no list found"}


# ---------------------------------------------------------------- rules -----

def twse_common(code: str) -> bool:
    return bool(re.fullmatch(r"[1-9]\d{3}", code)) and not code.startswith("91")


def disposition_active(periods: list[dict], code: str, target: date) -> bool:
    for p in periods:
        if p["code"] != code:
            continue
        start, end = parse_date(p.get("start")), parse_date(p.get("end"))
        if start and end and start <= target <= end:
            return True
    return False


def halted_at(events: list[dict], code: str, target: date) -> bool:
    timeline = []
    for e in events:
        if e["code"] != code:
            continue
        halt, resume = parse_date(e.get("halt")), parse_date(e.get("resume"))
        if halt and halt <= target:
            timeline.append((halt, 0, "halt"))
        if resume and resume <= target:
            timeline.append((resume, 1, "resume"))
    return bool(timeline) and max(timeline)[2] == "halt"


def evaluate_market(market: str, ctx: dict) -> tuple[list[dict], list[dict]]:
    """Apply all rules to one market; return (members, excluded)."""
    target, data_date = ctx["target"], ctx["data_date"]
    company = {r["code"]: r for r in ctx["company"]}
    daily = {r["code"]: r for r in ctx["daily"]}
    common = ctx.get("common")  # dict code->row for TPEX (from ISIN), None => code-pattern rule
    changed = {r["code"] for r in ctx.get("changed") or []}
    status = {}
    for r in ctx.get("status") or []:
        status.setdefault(r["code"], set()).update(r.get("flags") or [])
    disposition = ctx.get("disposition") or []
    halts = ctx.get("halt") or []
    daytrade = {r["code"]: r for r in ctx.get("daytrade") or []} if ctx.get("daytrade") is not None else None

    universe = {c for c in set(company) | set(daily) | set(common or ()) if STOCK_SCOPE.fullmatch(c)}
    members, excluded = [], []
    for code in sorted(universe):
        flags = trade_flags(market, code, ctx, daytrade, status, changed)
        reason = None
        if market == "TWSE":
            if not twse_common(code):
                reason = "not_common_stock"
        elif common is not None:
            if code not in common:
                reason = "not_common_stock"
        elif not twse_common(code):
            reason = "not_common_stock"
        comp = company.get(code)
        quote = daily.get(code)
        if reason is None and comp is None:
            reason = "no_company_profile"
        if reason is None and not comp.get("industry"):
            reason = "no_industry"
        if reason is None and quote is None:
            reason = "no_quote"
        if reason is None and quote.get("date") != data_date.isoformat():
            reason = "stale_quote"
        close = quote.get("close") if quote else None
        volume = quote.get("volume") if quote else None
        if reason is None and (close is None or not MIN_CLOSE <= close <= MAX_CLOSE):
            reason = "price_out_of_range"
        if reason is None and (volume is None or volume < MIN_VOLUME_SHARES):
            reason = "low_volume"
        if reason is None and code in changed:
            reason = "changed_trading"
        if reason is None and status.get(code):
            reason = "special_status"
        if reason is None and disposition_active(disposition, code, target):
            reason = "disposition"
        if reason is None and halted_at(halts, code, target):
            reason = "halted"
        if reason is None and (daytrade is None or code not in daytrade):
            reason = "not_daytrade_listed"
        if reason:
            excluded.append({"market": market, "symbol": code, "reason": reason,
                             "industry": (comp or {}).get("industry") or None, "flags": flags})
            continue
        name = comp.get("name") or quote.get("name") or ((common or {}).get(code) or {}).get("name", "")
        members.append({
            "market": market, "symbol": code, "name": name, "industry": comp.get("industry"),
            "close": close, "volume_lots": int(volume // 1000),
            "sell_first_suspended": bool(daytrade[code].get("sell_first_suspended")),
            "flags": flags,
        })
    return members, excluded


def trade_flags(market: str, code: str, ctx: dict, daytrade, status: dict, changed: set) -> dict:
    """Legal day-trade facts for one code, independent of the pool's liquidity / price rules (consumers such as the
    trader trial apply them as hard rules). None = the source was unavailable (unknown, never assumed)."""
    target = ctx["target"]
    disp, halt = ctx.get("disposition"), ctx.get("halt")
    listed = None if daytrade is None else code in daytrade
    return {
        "daytrade": listed,
        "sell_first_suspended": None if not listed else bool(daytrade[code].get("sell_first_suspended")),
        "disposition": None if disp is None else disposition_active(disp, code, target),
        "halted": None if halt is None else halted_at(halt, code, target),
        "changed": (None if ctx.get("changed") is None else code in changed) if market == "TWSE" else False,
        "special": (None if ctx.get("status") is None else bool(status.get(code))) if market == "TPEX" else False,
    }


# --------------------------------------------------------------- output -----

def render(doc: dict) -> str:
    """Pretty-print; member/excluded entries one per line to keep files compact."""
    head = {k: v for k, v in doc.items() if k not in ("members", "excluded", "margin_rows")}
    text = json.dumps(head, ensure_ascii=False, indent=2)
    assert text.endswith("\n}")
    parts = [text[:-2]]
    for key in ("members", "excluded", "margin_rows"):
        if key not in doc:
            continue
        items = ",\n".join("    " + json.dumps(i, ensure_ascii=False, separators=(", ", ": ")) for i in doc[key])
        parts.append(f',\n  "{key}": [\n{items}\n  ]' if items else f',\n  "{key}": []')
    return "".join(parts) + "\n}\n"


def write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def prune_pool_files() -> None:
    dated = sorted(p for p in POOL_DIR.glob("*.json") if re.fullmatch(r"\d{4}-\d{2}-\d{2}\.json", p.name))
    for old in dated[:-KEEP_POOL_FILES]:
        old.unlink()
        log(f"pruned old pool file {old.name}")


# ----------------------------------------------------------------- main -----

def main() -> int:
    started = datetime.now(TAIPEI)
    run_reason = os.environ.get("RUN_REASON", "").strip() or "manual"
    log(f"build_pool start, run_reason={run_reason}")
    sources: dict[str, dict] = {}
    warnings: list[str] = []

    holiday_rows, sources["TWSE_HOLIDAY"] = fetch_source("TWSE_HOLIDAY")
    holidays = holidays_from_rows(holiday_rows or [])
    if holiday_rows is None:
        warnings.append("holiday calendar unavailable: weekday-only calendar used")
    target, data_date = resolve_dates(started, holidays)
    log(f"target={target} data_date={data_date}")

    twse_ids = ["TWSE_COMPANY", "TWSE_DAILY", "TWSE_CHANGED", "TWSE_DISPOSITION", "TWSE_HALT", "TWSE_DAYTRADE"]
    tpex_ids = ["TPEX_COMPANY", "TPEX_DAILY", "TPEX_STATUS", "TPEX_DISPOSITION", "TPEX_HALT"]
    data: dict[str, list[dict] | None] = {}

    def run_serial(ids):
        for sid in ids:
            data[sid], sources[sid] = fetch_source(sid)

    def run_twse():
        run_serial(twse_ids)
        refresh_twse(data, sources, target, data_date)

    def run_tpex():
        run_serial(tpex_ids)
        data["TPEX_DAILY"], sources["TPEX_DAILY"] = prefer_newer_cache(
            "TPEX_DAILY", data.get("TPEX_DAILY"), sources["TPEX_DAILY"], data_date)
        data["TPEX_DAYTRADE"], sources["TPEX_DAYTRADE"] = fetch_tpex_daytrade(target, holidays)

    # One serial lane per host to stay polite; the three lanes run in parallel.
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(run_twse), pool.submit(run_tpex), pool.submit(run_serial, ["TPEX_ISIN"])]
        for f in futures:
            f.result()

    # Day-trade list: target date preferred; otherwise most recent published list (<= target).
    daytrade_date, daytrade_fallback = {}, {}
    for market in ("TWSE", "TPEX"):
        sid = f"{market}_DAYTRADE"
        rows = data.get(sid)
        if rows is not None and (source_date_of(rows) or "") > target.isoformat():
            warnings.append(f"{market} day-trade list dated {source_date_of(rows)} is after target {target}")
        if rows is None:
            daytrade_date[market], daytrade_fallback[market] = None, True
            warnings.append(f"{market} day-trade list unavailable: market has no members")
            continue
        list_date = source_date_of(rows)
        if market == "TWSE":
            # TWTB4U holds a single date; keep only rows of the chosen list.
            rows = [r for r in rows if r.get("date") == list_date]
            data[sid] = rows
        daytrade_date[market] = list_date
        daytrade_fallback[market] = list_date != target.isoformat()
        if daytrade_fallback[market]:
            warnings.append(f"{market} day-trade list for {target} not published yet; using list of {list_date}")

    members, excluded, skipped = [], [], {}
    common_tpex = None
    if data.get("TPEX_ISIN") is not None:
        common_tpex = {r["code"]: r for r in data["TPEX_ISIN"]}
    else:
        warnings.append("TPEX ISIN list unavailable: TPEX common stock = 4-digit code rule")
    for market in ("TWSE", "TPEX"):
        company, daily = data.get(f"{market}_COMPANY"), data.get(f"{market}_DAILY")
        if company is None or daily is None:
            missing = [s for s in (f"{market}_COMPANY", f"{market}_DAILY") if data.get(s) is None]
            skipped[market] = f"no usable copy of {', '.join(missing)}"
            warnings.append(f"{market} skipped: {skipped[market]}")
            continue
        daily_date = source_date_of(daily)
        if daily_date != data_date.isoformat():
            warnings.append(f"{market} daily quotes are dated {daily_date}, expected {data_date}")
        ctx = {
            "target": target, "data_date": data_date, "company": company, "daily": daily,
            "common": common_tpex if market == "TPEX" else None,
            "changed": data.get("TWSE_CHANGED") if market == "TWSE" else None,
            "status": data.get("TPEX_STATUS") if market == "TPEX" else None,
            "disposition": data.get(f"{market}_DISPOSITION"), "halt": data.get(f"{market}_HALT"),
            "daytrade": data.get(f"{market}_DAYTRADE"),
        }
        for sid in (f"{market}_DISPOSITION", f"{market}_HALT") + (("TWSE_CHANGED",) if market == "TWSE" else ("TPEX_STATUS",)):
            if data.get(sid) is None:
                warnings.append(f"{sid} unavailable: its exclusion rule could not be applied")
        bad_periods = [r["code"] for r in data.get(f"{market}_DISPOSITION") or [] if not r.get("start")]
        if bad_periods:
            warnings.append(f"{market}_DISPOSITION: unparseable period for {', '.join(sorted(bad_periods)[:10])}")
        m, e = evaluate_market(market, ctx)
        members += m
        excluded += e

    members.sort(key=lambda r: (r["market"], r["symbol"]))
    excluded.sort(key=lambda r: (r["market"], r["symbol"]))
    margin_rows, margin_status = [], {}
    for market in ("TWSE", "TPEX"):
        rows, margin_status[market] = fetch_margin(market, data_date)
        margin_rows += [{"market": market, "symbol": r["code"], "m": r["m"], "m_prev": r["m_prev"], "s": r["s"], "s_prev": r["s_prev"]} for r in rows]
        if margin_status[market]["status"] != "fresh":
            warnings.append(f"{market} margin balances for {data_date} {margin_status[market]['status']} (optional)")
    margin_rows.sort(key=lambda r: (r["market"], r["symbol"]))
    by_reason: dict[str, int] = {}
    for e in excluded:
        by_reason[e["reason"]] = by_reason.get(e["reason"], 0) + 1
    doc = {
        "schema": SCHEMA,
        "target": target.isoformat(),
        "data_date": data_date.isoformat(),
        "generated_at_taipei": now_text(),
        "run_reason": run_reason,
        "sources": {sid: sources[sid] for sid in sorted(sources)},
        "daytrade_list_date": daytrade_date,
        "daytrade_fallback": daytrade_fallback,
        "skipped_markets": skipped,
        "warnings": warnings,
        "counts": {
            "members": len(members),
            "by_market": {mk: sum(1 for r in members if r["market"] == mk) for mk in ("TWSE", "TPEX")},
            "excluded_by_reason": dict(sorted(by_reason.items())),
        },
        "members": members,
        "excluded": excluded,
        "margin": {"data_date": data_date.isoformat(), "unit": "lots", "status": margin_status},
        "margin_rows": margin_rows,
    }
    for w in warnings:
        log(f"WARNING: {w}")
    log(f"members={len(members)} by_market={doc['counts']['by_market']} excluded={doc['counts']['excluded_by_reason']}")
    if not members:
        log("ERROR: pool is empty; existing pool files are left untouched")
        return 1
    text = render(doc)
    write_atomic(POOL_DIR / f"{target.isoformat()}.json", text)
    write_atomic(POOL_DIR / "latest.json", text)
    prune_pool_files()
    log(f"wrote pool/{target.isoformat()}.json and pool/latest.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
