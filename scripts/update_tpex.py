#!/usr/bin/env python3
"""Fetch official TPEx disposition and company-profile data as validated JSON."""

from __future__ import annotations

import csv
import http.client
import io
import math
import os
import tempfile
import sys
import json
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone, date
from pathlib import Path
from zoneinfo import ZoneInfo

SOURCE_URL = (
    "https://www.tpex.org.tw/web/bulletin/disposal_information/"
    "disposal_information_result.php?l=zh-tw&o=data"
)
COMPANY_PROFILE_SOURCE_URL = (
    "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
)
INSTITUTION_SOURCE_URL = (
    "https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading"
)
MARGIN_SOURCE_URL = (
    "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_margin_balance"
)
TDCC_HOLDER_SOURCE_URL = "https://openapi-t.tdcc.com.tw/v1/opendata/1-5"
OUTPUT_PATH = Path("tpex_disposition.json")
COMPANY_PROFILE_OUTPUT_PATH = Path("tpex_company_profiles.json")
CHIP_OUTPUT_PATH = Path("tpex_chip_data.json")
TDCC_HOLDER_OUTPUT_PATH = Path("tdcc_big_holders.json")
TAIPEI = ZoneInfo("Asia/Taipei")
CODE_PATTERN = re.compile(r"(?<![0-9A-Z])[0-9]{4,6}[A-Z]{0,2}(?![0-9A-Z])", re.I)
FULL_CODE_PATTERN = re.compile(r"[0-9]{4,6}[A-Z]{0,2}", re.I)
CHINESE_PATTERN = re.compile(r"[\u3400-\u9fff]")
CODE_KEY_PATTERN = re.compile(
    r"證券代號|證券代碼|股票代號|股票代碼|有價證券代號|"
    r"security.?code|stock.?code|company.?code",
    re.I,
)


sys.path.insert(0, str(Path(__file__).resolve().parent))
import tpex_root_core as core
import bounded_transport

READ_ONCE = bounded_transport.read_once
ACTIVE_BUDGET = None
JOB_SECONDS = 150
JOB_BODY_BYTES = 64*1024*1024
AUXILIARY_SECONDS = 30
AUXILIARY_BODY_BYTES = 32*1024*1024

def decode_csv(raw: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp950", "big5"):
        try:
            text = raw.decode(encoding)
            if "<html" in text[:500].lower() or "<!doctype" in text[:500].lower():
                raise RuntimeError("TPEx returned HTML instead of CSV")
            return text
        except UnicodeDecodeError:
            continue

    raise RuntimeError("Unable to decode TPEx official CSV")


def parse_json_array(raw: bytes, label: str, minimum_records: int) -> list[dict]:
    try:
        data = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError(f"{label} is not valid UTF-8 JSON") from error

    if not isinstance(data, list):
        raise RuntimeError(f"{label} JSON must be an array")
    records = [row for row in data if isinstance(row, dict)]
    if len(records) < minimum_records:
        raise RuntimeError(
            f"{label} validation found only {len(records)} records"
        )
    return records


def validate_tpex_chip_records(
    institutions: list[dict],
    margins: list[dict],
) -> None:
    institution_codes = {
        str(row.get("SecuritiesCompanyCode", "")).strip()
        for row in institutions
    }
    margin_codes = {
        str(row.get("SecuritiesCompanyCode", "")).strip()
        for row in margins
    }
    if len(institution_codes - {""}) < 100:
        raise RuntimeError("TPEx institutional data has too few security codes")
    if len(margin_codes - {""}) < 100:
        raise RuntimeError("TPEx margin data has too few security codes")


def aggregate_tdcc_big_holders(rows: list[dict]) -> list[dict[str, object]]:
    """Aggregate TDCC holding levels 12-15 (over 400 lots) by stock code."""
    aggregated: dict[str, dict[str, object]] = {}

    for row in rows:
        symbol = str(row.get("證券代號", "")).strip().upper()
        if not re.fullmatch(r"[0-9]{4}", symbol):
            continue
        try:
            level = int(str(row.get("持股分級", "")).strip())
        except ValueError:
            continue
        if level < 12 or level > 15:
            continue
        raw_ratio = row.get("占集保庫存數比例%")
        if raw_ratio is None or not str(raw_ratio).strip():
            raise RuntimeError("TDCC eligible holding level is missing its ratio")
        try:
            ratio = float(str(raw_ratio).replace(",", ""))
        except ValueError as error:
            raise RuntimeError("TDCC eligible holding ratio is not numeric") from error
        if not math.isfinite(ratio):
            raise RuntimeError("TDCC eligible holding ratio is not finite")

        date = str(
            row.get("﻿資料日期", row.get("資料日期", ""))
        ).strip()
        current = aggregated.setdefault(
            symbol,
            {
                "symbol": symbol,
                "date": date,
                "big_holder_400_ratio_pct": 0.0,
            },
        )
        total_ratio = float(current["big_holder_400_ratio_pct"]) + ratio
        if not math.isfinite(total_ratio):
            raise RuntimeError("TDCC holding ratio aggregation overflowed")
        current["big_holder_400_ratio_pct"] = round(total_ratio, 4)
        if date:
            current["date"] = date

    records = sorted(aggregated.values(), key=lambda row: str(row["symbol"]))
    if len(records) < 500:
        raise RuntimeError(
            f"TDCC big-holder validation found only {len(records)} stock codes"
        )
    return records


def parse_records(text: str) -> tuple[list[dict[str, str]], list[str]]:
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise RuntimeError("TPEx CSV has no header row")

    clean_fields = [str(field or "").lstrip("\ufeff").strip() for field in reader.fieldnames]
    if not any(CODE_KEY_PATTERN.search(field) for field in clean_fields):
        raise RuntimeError(f"TPEx CSV code column not recognized: {clean_fields}")

    records: list[dict[str, str]] = []
    codes: set[str] = set()

    for source_row in reader:
        row: dict[str, str] = {}
        for original_key, value in source_row.items():
            key = str(original_key or "").lstrip("\ufeff").strip()
            row[key] = str(value or "").strip()

        if not any(row.values()):
            continue

        records.append(row)
        for key, value in row.items():
            if CODE_KEY_PATTERN.search(key):
                codes.update(match.upper() for match in CODE_PATTERN.findall(value.upper()))

    if not records:
        raise RuntimeError("TPEx CSV contains no data rows")
    if not codes:
        raise RuntimeError("TPEx CSV contains no recognizable security codes")

    return records, sorted(codes)


def parse_company_profiles(raw: bytes) -> list[dict[str, str]]:
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise RuntimeError("Unable to decode TPEx company-profile JSON") from error

    if "<html" in text[:500].lower() or "<!doctype" in text[:500].lower():
        raise RuntimeError("TPEx returned HTML instead of company-profile JSON")

    try:
        data = json.loads(text)
    except json.JSONDecodeError as error:
        raise RuntimeError("TPEx company-profile response is not valid JSON") from error

    if not isinstance(data, list):
        raise RuntimeError("TPEx company-profile JSON must be an array")

    profiles: list[dict[str, str]] = []
    seen_codes: set[str] = set()

    for source_row in data:
        if not isinstance(source_row, dict):
            continue

        code = str(source_row.get("SecuritiesCompanyCode", "")).strip().upper()
        if not FULL_CODE_PATTERN.fullmatch(code) or code in seen_codes:
            continue

        abbreviation = str(source_row.get("CompanyAbbreviation", "")).strip()
        company_name = str(source_row.get("CompanyName", "")).strip()
        chinese_name = (
            abbreviation
            if CHINESE_PATTERN.search(abbreviation)
            else company_name if CHINESE_PATTERN.search(company_name) else ""
        )
        industry_code = str(
            source_row.get("SecuritiesIndustryCode", "")
        ).strip()
        industry_match = re.search(r"\d{1,2}", industry_code)
        normalized_industry = (
            industry_match.group(0).zfill(2) if industry_match else ""
        )

        if not chinese_name or not normalized_industry:
            continue

        profiles.append(
            {
                "SecuritiesCompanyCode": code,
                "CompanyAbbreviation": chinese_name,
                "SecuritiesIndustryCode": normalized_industry,
            }
        )
        seen_codes.add(code)

    if len(profiles) < 100:
        raise RuntimeError(
            f"TPEx company-profile validation found only {len(profiles)} records"
        )

    return profiles


def source_datetime(value):
    if not isinstance(value, str) or not value:
        raise ValueError("missing source timestamp")
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("source timestamp requires a timezone")
    return stamp


def timestamp_fields(now):
    return {
        "fetched_at_utc": now.isoformat().replace("+00:00", "Z"),
        "fetched_at_taipei": now.astimezone(TAIPEI).isoformat(),
    }


def assess_primary_freshness(payload, now, threshold_seconds=None):
    """Pure alert intent only. No Telegram binding, ENV or outbound operation."""
    result = {"state": "PENDING_THRESHOLD", "age_seconds": None,
              "threshold_seconds": threshold_seconds, "should_notify": False,
              "telegram_delivery": "NOT_BOUND"}
    if threshold_seconds is None:
        return result
    if type(threshold_seconds) is not int or threshold_seconds <= 0:
        raise ValueError("freshness threshold requires a positive integer")
    if now.tzinfo is None:
        raise ValueError("now must be timezone aware")
    try:
        original = payload["fetched_at_utc"]
        age = (now - source_datetime(original)).total_seconds()
    except (TypeError, KeyError, ValueError, OverflowError):
        result["state"] = "UNKNOWN"
        return result
    result["original_fetched_at_utc"] = original
    if age < 0:
        result["state"] = "UNKNOWN"
        return result
    result.update(age_seconds=age, state="STALE" if age > threshold_seconds else "FRESH")
    result["should_notify"] = result["state"] == "STALE"
    return result


def reject_nonfinite(value):
    raise ValueError('non-finite cached JSON')


def read_previous(path):
    try:
        value = json.loads(path.read_text(encoding="utf-8"),
                           parse_constant=reject_nonfinite)
        return value if isinstance(value, dict) else None
    except (OSError, UnicodeError, ValueError):
        return None


def validate_profile_cache(rows):
    codes = []
    for row in rows:
        code = row.get("SecuritiesCompanyCode")
        name = row.get("CompanyAbbreviation")
        industry = row.get("SecuritiesIndustryCode")
        if not isinstance(code, str) or not FULL_CODE_PATTERN.fullmatch(code):
            raise ValueError("invalid cached company code")
        if not isinstance(name, str) or not CHINESE_PATTERN.search(name):
            raise ValueError("invalid cached company name")
        if not isinstance(industry, str) or not re.fullmatch(r"[0-9]{2}", industry):
            raise ValueError("invalid cached company industry")
        codes.append(code)
    if len(set(codes)) != len(codes):
        raise ValueError("duplicate cached company code")


def validate_chip_rows(rows):
    codes = {str(row.get("SecuritiesCompanyCode", "")).strip() for row in rows}
    if len(codes - {""}) < 100:
        raise RuntimeError("chip data has too few distinct codes")


def parse_chip_rows(raw, label):
    rows = parse_json_array(raw, label, 100)
    validate_chip_rows(rows)
    return rows


def validate_tdcc_cache(rows):
    symbols = set()
    for row in rows:
        symbol = row.get("symbol")
        ratio = row.get("big_holder_400_ratio_pct")
        if not isinstance(symbol, str) or not re.fullmatch(r"[0-9]{4}", symbol):
            raise ValueError("invalid cached TDCC symbol")
        if type(ratio) not in (int, float) or not math.isfinite(ratio):
            raise ValueError("invalid cached TDCC ratio")
        if symbol in symbols:
            raise ValueError("duplicate cached TDCC symbol")
        symbols.add(symbol)


def validate_previous(payload, *, source_url, minimum_records, now,
                      dataset=None, market=None, rows_validator=None):
    if not isinstance(payload, dict) or payload.get("source_url") != source_url:
        raise ValueError("fallback source identity mismatch")
    if dataset is not None and payload.get("dataset") != dataset:
        raise ValueError("fallback dataset mismatch")
    if market is not None and payload.get("market") != market:
        raise ValueError("fallback market mismatch")
    if dataset is not None and (type(payload.get("schema_version")) is not int
                                or payload["schema_version"] != 1):
        raise ValueError("fallback schema mismatch")
    stamp = source_datetime(payload.get("fetched_at_utc"))
    if source_datetime(payload.get("fetched_at_taipei")) != stamp or stamp > now:
        raise ValueError("fallback timestamps disagree or are in the future")
    rows = payload.get("records")
    if not isinstance(rows, list) or len(rows) < minimum_records:
        raise ValueError("fallback records missing or too few")
    if any(not isinstance(row, dict) for row in rows):
        raise ValueError("fallback contains non-object records")
    if type(payload.get("record_count")) is not int or payload["record_count"] != len(rows):
        raise ValueError("fallback record_count mismatch")
    if rows_validator is not None:
        rows_validator(rows)
    return dict(payload)


def previous_chip_branch(payload, branch):
    if not isinstance(payload, dict) or payload.get("dataset") != "chip_data":
        return None
    if payload.get("schema_version") != 1 or payload.get("market") != "tpex":
        return None
    value = payload.get(branch)
    if not isinstance(value, dict):
        return None
    result = dict(value)
    if not result.get("fetched_at_utc"):
        result["fetched_at_utc"] = payload.get("fetched_at_utc")
        result["fetched_at_taipei"] = payload.get("fetched_at_taipei")
        result["timestamp_provenance"] = "legacy_chip_root"
    return result


def atomic_write_json(path, payload):
    # Staging errors preserve the old target and retain failed stages for diagnosis.
    # This is per-file replacement, not a multi-file/Git transaction.
    body = (json.dumps(payload, ensure_ascii=False, indent=2,
                       allow_nan=False) + "\n").encode("utf-8")
    with tempfile.NamedTemporaryFile(mode="wb", dir=path.parent,
                                     prefix=path.name + ".pending.",
                                     delete=False) as staged:
        staged.write(body)
        staged.flush()
        os.fsync(staged.fileno())
        temporary_path = staged.name
    os.replace(temporary_path, path)


def publish_optional(path, value, statuses):
    if value is None:
        return
    try:
        atomic_write_json(path, value)
    except (OSError, ValueError, TypeError, UnicodeError):
        for status in statuses:
            status["candidate_source_fetched_at_utc"] = status.pop(
                "source_fetched_at_utc", None)
            status.update(status="publication_failed", stale=True, published=False,
                          source_fetched_at_utc=None)
        print("::warning title=Auxiliary publication degraded::" + path.name)
        return
    for status in statuses:
        status["published"] = True


REQUIRED_PRIMARY_FIELDS = (
    '公布日期', '證券代號', '證券名稱', '處置起訖時間', '處置原因', '處置內容',
)


def native_day(value):
    value = str(value or '').strip()
    try:
        if re.fullmatch(r'[0-9]{7}', value):
            return date(int(value[:3])+1911, int(value[3:5]), int(value[5:7])).isoformat()
        if re.fullmatch(r'[0-9]{8}', value):
            return date(int(value[:4]), int(value[4:6]), int(value[6:8])).isoformat()
        return date.fromisoformat(value).isoformat()
    except ValueError as error:
        raise core.SchemaError('invalid native source date') from error


def parse_primary(raw):
    try:
        reader = csv.DictReader(io.StringIO(decode_csv(raw)), strict=True)
        fields = [str(f or '').lstrip('\ufeff').strip() for f in (reader.fieldnames or [])]
        if len(fields) != len(set(fields)) or not set(REQUIRED_PRIMARY_FIELDS).issubset(fields):
            raise core.SchemaError('incomplete/duplicate disposition header')
        records = []; codes = set(); publications = []; periods = []
        for source_row in reader:
            if None in source_row or any(value is None for value in source_row.values()):
                raise core.SchemaError('incomplete or surplus CSV fields')
            row = {str(k).lstrip('\ufeff').strip(): str(v).strip() for k, v in source_row.items()}
            if not any(row.values()):
                continue
            if not all(row.get(k) for k in REQUIRED_PRIMARY_FIELDS):
                raise core.SchemaError('disposition required field absent')
            code = row['證券代號'].upper()
            if not FULL_CODE_PATTERN.fullmatch(code):
                raise core.SchemaError('disposition security code invalid')
            published = native_day(row['公布日期'])
            if published > datetime.now(timezone.utc).astimezone(TAIPEI).date().isoformat():
                raise core.SchemaError('future native publication date')
            match = re.fullmatch(r'([0-9]{7,8})\s*[~～至]\s*([0-9]{7,8})', row['處置起訖時間'])
            if not match:
                raise core.SchemaError('effective period schema unknown')
            start, end = native_day(match[1]), native_day(match[2])
            if start > end:
                raise core.SchemaError('effective period reversed')
            records.append(row); codes.add(code); publications.append(published)
            periods.append({'security_code': code, 'from': start, 'through': end})
        return {'records': records, 'codes': sorted(codes),
                'source_publication_date': max(publications) if publications else 'UNKNOWN',
                'effective_periods': periods, 'covered_through': 'UNKNOWN',
                'complete_response_structure': True,
                'empty_result_basis': 'COMPLETE_SIX_FIELD_HEADER_ZERO_ROWS' if not records else None}
    except (csv.Error, UnicodeError, RuntimeError) as error:
        if isinstance(error, core.SchemaError): raise
        raise core.SchemaError('primary source CSV validation failed') from error


def fetch_raw(url, label, validator, timeout_seconds=20, minimum_bytes=0, max_attempts=3):
    budget = ACTIVE_BUDGET or core.Budget(time.monotonic()+60, 32*1024*1024, clock=time.monotonic)
    captured = {}
    def read_once(source, seconds, cap):
        result = READ_ONCE(source, seconds, cap)
        captured['response'] = result
        return result
    def validate(raw):
        if len(raw) < minimum_bytes:
            raise core.TransientError('short response', partial_bytes=0)
        validator(raw)
    raw = core.fetch(url, read_once, validate, budget,
                     core.FetchPolicy(attempts=max_attempts,attempt_seconds=min(timeout_seconds,20),
                                      endpoint_seconds=60, attempt_bytes=32*1024*1024),
                     decode=lambda value: value,sleep=time.sleep,jitter=core.random.random)
    result = captured['response']; headers = {str(k).lower(): v for k,v in result.headers.items()}
    return raw, {'etag': headers.get('etag',''), 'last_modified': headers.get('last-modified',''),
                 'content_type': headers.get('content-type','')}, result.final_url


def download_csv():
    return fetch_raw(SOURCE_URL, 'TPEx disposition', parse_primary)


def validate_aux_publishability(value):
    """Reject data/serialization failures inside this auxiliary's own boundary."""
    try:
        (json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode("utf-8")
    except (ValueError, TypeError, UnicodeError) as error:
        raise core.SchemaError("auxiliary data cannot be published as finite UTF-8 JSON") from error


def download_json_dataset(source_url, label, minimum_bytes=1000,
                          timeout_seconds=60, max_attempts=3, validator=None):
    # The one retry owner has at most3 attempts; no per-wrapper retry loop.
    if type(max_attempts) is not int or not 1 <= max_attempts <= 3:
        raise ValueError('at most three total attempts')
    def validate(raw):
        value = core.decode_json(raw)
        if not isinstance(value, list):
            raise core.SchemaError('JSON source must be an array')
        # parse_constant does not catch a legal JSON exponent such as 1e999.
        # All JSON download callers are auxiliaries; fail their schema once.
        validate_aux_publishability(value)
        if validator is not None:
            try: validator(raw)
            except (RuntimeError, ValueError, TypeError) as error:
                raise core.SchemaError('JSON source fixed schema/required fields mismatch') from error
    return fetch_raw(source_url,label,validate,timeout_seconds,minimum_bytes,max_attempts)


def download_company_profiles():
    return download_json_dataset(COMPANY_PROFILE_SOURCE_URL,'company_profiles',
                                  validator=parse_company_profiles)


def source_quality(payload, category):
    # No verified repository holiday table was supplied. Do not invent one.
    quality = core.auxiliary_quality(payload,category,[],None,calendar_receipt=None)
    quality['policy_completed_trading_days'] = (5 if category=='display' else
                                               1 if category=='daily_statistics' else None)
    quality['calendar_binding'] = 'UNKNOWN'
    return quality


def finish_optional_source(value, attempt, category):
    value.setdefault('source_publication_date','UNKNOWN');value.setdefault('covered_through','UNKNOWN')
    value['last_attempt_at']=attempt
    value.setdefault('last_successful_fetch_at',value.get('fetched_at_utc','UNKNOWN'))
    value['stale']=True
    quality=source_quality(value,category)
    status={'status':value['refresh_status'],'stale':True,'published':False,
            'source_fetched_at_utc':value.get('fetched_at_utc'),
            'last_attempt_at':attempt,'last_successful_fetch_at':value['last_successful_fetch_at'],
            'source_publication_date':value['source_publication_date'],
            'covered_through':value['covered_through'],'quality':quality,
            'refresh_error':value.get('refresh_error')}
    # Both fresh values and preserved fallback metadata/records must be usable
    # before they can reach main's whole-candidate defence-in-depth check.
    validate_aux_publishability(value)
    validate_aux_publishability(status)
    return value,status


def optional_source(label,url,loader,parser,previous,now,minimum_records,
                    dataset=None,market=None,rows_validator=None,category='daily_statistics'):
    attempt = now.isoformat().replace('+00:00','Z')
    try:
        if ACTIVE_BUDGET.remaining() <= 0:
            raise core.BudgetExceeded('auxiliary wait exhausted')
        raw,meta,resolved = loader()
        try:
            rows = parser(raw)
        except RuntimeError as error:
            raise core.SchemaError('auxiliary parsed data failed its domain checks') from error
        value={'source_name':label,'source_url':url,'resolved_source_url':resolved,
               'record_count':len(rows),'records':rows,'response_metadata':meta,
               **timestamp_fields(datetime.now(timezone.utc)),
               'source_publication_date':'UNKNOWN','covered_through':'UNKNOWN',
               'refresh_status':'fresh_fetch_native_coverage_unknown','refresh_error':None}
        return finish_optional_source(value,attempt,category)
    except (core.TransientError,core.PermanentError,core.BudgetExceeded) as error:
        try:
            value=validate_previous(previous,source_url=url,minimum_records=minimum_records,
                                    now=now,dataset=dataset,market=market,
                                    rows_validator=rows_validator)
            value.update(refresh_status='stale_fallback',refresh_error=type(error).__name__)
            return finish_optional_source(value,attempt,category)
        except (ValueError,RuntimeError,TypeError,OverflowError):
            return None,{'status':'unavailable','stale':True,'published':False,
                         'last_attempt_at':attempt,'last_successful_fetch_at':'UNKNOWN',
                         'source_publication_date':'UNKNOWN','covered_through':'UNKNOWN',
                         'quality':source_quality(None,category),'refresh_error':type(error).__name__}


def main():
    global ACTIVE_BUDGET
    job=core.Budget(time.monotonic()+JOB_SECONDS,JOB_BODY_BYTES,clock=time.monotonic)
    ACTIVE_BUDGET=job;attempt=datetime.now(timezone.utc)
    try:
        raw,meta,resolved=download_csv();primary=parse_primary(raw)
    except (core.PermanentError,core.TransientError,core.BudgetExceeded):
        # A failed attempt is reported through the existing job failure/log.
        # Never rewrite the old primary or its successful source clock.
        print('::error title=Primary refresh failed::previous primary bytes retained; success time unchanged')
        raise
    success=datetime.now(timezone.utc)
    previous_profiles=read_previous(COMPANY_PROFILE_OUTPUT_PATH)
    previous_chip=read_previous(CHIP_OUTPUT_PATH)
    previous_tdcc=read_previous(TDCC_HOLDER_OUTPUT_PATH)
    ACTIVE_BUDGET=core.BudgetView(job,AUXILIARY_SECONDS,AUXILIARY_BODY_BYTES)
    try:
        profiles,ps=optional_source('TPEx company profiles',COMPANY_PROFILE_SOURCE_URL,
            download_company_profiles,parse_company_profiles,previous_profiles,success,100,
            dataset='company_profiles',market='tpex',rows_validator=validate_profile_cache,category='display')
        ip=lambda b:parse_chip_rows(b,'institutions')
        institutions,ins=optional_source('TPEx institutional data',INSTITUTION_SOURCE_URL,
            lambda:download_json_dataset(INSTITUTION_SOURCE_URL,'institutions',validator=ip),ip,
            previous_chip_branch(previous_chip,'institutions'),success,100,rows_validator=validate_chip_rows)
        mp=lambda b:parse_chip_rows(b,'margin')
        margin,ms=optional_source('TPEx margin data',MARGIN_SOURCE_URL,
            lambda:download_json_dataset(MARGIN_SOURCE_URL,'margin',validator=mp),mp,
            previous_chip_branch(previous_chip,'margin'),success,100,rows_validator=validate_chip_rows)
        tp=lambda b:aggregate_tdcc_big_holders(parse_json_array(b,'TDCC',5000))
        tdcc,ts=optional_source('TDCC holders',TDCC_HOLDER_SOURCE_URL,
            lambda:download_json_dataset(TDCC_HOLDER_SOURCE_URL,'TDCC',minimum_bytes=100000,
                                         timeout_seconds=20,validator=tp),tp,
            previous_tdcc,success,500,dataset='tdcc_big_holders',rows_validator=validate_tdcc_cache,
            category='periodic_statistics_unbound')
    finally:
        ACTIVE_BUDGET=job
    if profiles is not None:profiles.update(schema_version=1,dataset='company_profiles',market='tpex')
    if tdcc is not None:
        tdcc.update(schema_version=1,dataset='tdcc_big_holders')
        if tdcc['refresh_status'].startswith('fresh_fetch'):
            tdcc['source_data_date']=max((str(x.get('date',''))for x in tdcc['records']),default='')
        # Original data date is retained, but not relabeled as publication/coverage.
    chip=None
    if institutions is not None and margin is not None:
        oldest=min((institutions,margin),key=lambda v:source_datetime(v['fetched_at_utc']))
        chip={'schema_version':1,'dataset':'chip_data','market':'tpex',
              'fetched_at_utc':oldest['fetched_at_utc'],'fetched_at_taipei':oldest['fetched_at_taipei'],
              'last_attempt_at':success.isoformat(),'source_publication_date':'UNKNOWN',
              'covered_through':'UNKNOWN','stale':True,'institutions':institutions,'margin':margin}
    else:
        for status in (ins,ms):
            status['candidate_status']=status['status']
            status.update(status='chip_file_retained_incomplete_pair',published=False,stale=True)
    payload={'schema_version':1,'market':'tpex','source_name':'TPEx 上櫃處置有價證券資訊',
             'source_url':SOURCE_URL,'resolved_source_url':resolved,**timestamp_fields(success),
             'last_attempt_at':attempt.isoformat().replace('+00:00','Z'),
             'last_successful_fetch_at':success.isoformat().replace('+00:00','Z'),
             'source_publication_date':primary['source_publication_date'],
             'effective_periods':primary['effective_periods'],'covered_through':'UNKNOWN','stale':True,
             'coverage_status':'UNKNOWN_SOURCE_FIELD_ABSENT',
             'eligibility_status':'UNKNOWN_SOURCE_ROLE_BINDING',
             'complete_response_structure':True,'empty_result_basis':primary['empty_result_basis'],
             'record_count':len(primary['records']),'security_codes':primary['codes'],
             'security_code_count':len(primary['codes']),'records':primary['records'],
             'response_metadata':meta,'auxiliary_sources':{'company_profiles':ps,'institutions':ins,'margin':ms,'tdcc':ts}}
    if job.remaining()<=0:raise core.BudgetExceeded('job elapsed before publication candidate')
    # Serialize the complete four-path candidate before any replacement. The
    # existing workflow publishes its Git tree in one commit, never via rebase/force.
    for value in [payload,profiles,chip,tdcc]:
        if value is not None:json.dumps(value,ensure_ascii=False,allow_nan=False)
    publish_optional(COMPANY_PROFILE_OUTPUT_PATH,profiles,[ps])
    publish_optional(CHIP_OUTPUT_PATH,chip,[ins,ms])
    publish_optional(TDCC_HOLDER_OUTPUT_PATH,tdcc,[ts])
    atomic_write_json(OUTPUT_PATH,payload)
    print('Primary candidate ready for existing workflow single commit; coverage UNKNOWN, no ENTRY qualification')


if __name__=='__main__':main()
