#!/usr/bin/env python3
"""Fetch official TPEx disposition and company-profile data as validated JSON."""

from __future__ import annotations

import csv
import io
import json
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

SOURCE_URL = (
    "https://www.tpex.org.tw/web/bulletin/disposal_information/"
    "disposal_information_result.php?l=zh-tw&o=data"
)
COMPANY_PROFILE_SOURCE_URL = (
    "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
)
OUTPUT_PATH = Path("tpex_disposition.json")
COMPANY_PROFILE_OUTPUT_PATH = Path("tpex_company_profiles.json")
TAIPEI = ZoneInfo("Asia/Taipei")
CODE_PATTERN = re.compile(r"(?<![0-9A-Z])[0-9]{4,6}[A-Z]{0,2}(?![0-9A-Z])", re.I)
FULL_CODE_PATTERN = re.compile(r"[0-9]{4,6}[A-Z]{0,2}", re.I)
CHINESE_PATTERN = re.compile(r"[\u3400-\u9fff]")
CODE_KEY_PATTERN = re.compile(
    r"證券代號|證券代碼|股票代號|股票代碼|有價證券代號|"
    r"security.?code|stock.?code|company.?code",
    re.I,
)


def download_csv() -> tuple[bytes, dict[str, str], str]:
    last_error: Exception | None = None

    for attempt in range(1, 4):
        request = urllib.request.Request(
            SOURCE_URL,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "Chrome/126.0 Safari/537.36 "
                    "tpex-disposition-relay/1.0"
                ),
                "Accept": "text/csv,text/plain;q=0.9,*/*;q=0.5",
                "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.5",
                "Accept-Encoding": "identity",
                "Referer": "https://www.tpex.org.tw/zh-tw/announce/market/disposal.html",
            },
        )

        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read()
                final_url = response.geturl()
                headers = {
                    "etag": response.headers.get("ETag", ""),
                    "last_modified": response.headers.get("Last-Modified", ""),
                    "content_type": response.headers.get("Content-Type", ""),
                }

            if "/errors" in final_url:
                raise RuntimeError(f"TPEx redirected to an error page: {final_url}")
            if len(raw) < 30:
                raise RuntimeError("TPEx response is unexpectedly short")
            return raw, headers, final_url
        except (OSError, RuntimeError, urllib.error.URLError) as error:
            last_error = error
            if attempt < 3:
                time.sleep(attempt * 3)

    raise RuntimeError(f"Unable to download TPEx official CSV: {last_error}")


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


def download_company_profiles() -> tuple[bytes, dict[str, str], str]:
    last_error: Exception | None = None

    for attempt in range(1, 4):
        request = urllib.request.Request(
            COMPANY_PROFILE_SOURCE_URL,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "Chrome/126.0 Safari/537.36 "
                    "tpex-disposition-relay/1.1"
                ),
                "Accept": "application/json,text/plain;q=0.9,*/*;q=0.5",
                "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.5",
                "Accept-Encoding": "identity",
                "Referer": "https://www.tpex.org.tw/openapi/",
            },
        )

        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read()
                final_url = response.geturl()
                headers = {
                    "etag": response.headers.get("ETag", ""),
                    "last_modified": response.headers.get("Last-Modified", ""),
                    "content_type": response.headers.get("Content-Type", ""),
                }

            if "/errors" in final_url:
                raise RuntimeError(f"TPEx redirected to an error page: {final_url}")
            if len(raw) < 1000:
                raise RuntimeError("TPEx company-profile response is unexpectedly short")
            return raw, headers, final_url
        except (OSError, RuntimeError, urllib.error.URLError) as error:
            last_error = error
            if attempt < 3:
                time.sleep(attempt * 3)

    raise RuntimeError(
        f"Unable to download TPEx official company profiles: {last_error}"
    )


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


def main() -> None:
    raw, response_headers, final_url = download_csv()
    records, codes = parse_records(decode_csv(raw))
    profile_raw, profile_headers, profile_final_url = download_company_profiles()
    profiles = parse_company_profiles(profile_raw)
    now_utc = datetime.now(timezone.utc)
    now_taipei = now_utc.astimezone(TAIPEI)

    payload = {
        "schema_version": 1,
        "market": "tpex",
        "source_name": "TPEx 上櫃處置有價證券資訊",
        "source_url": SOURCE_URL,
        "resolved_source_url": final_url,
        "fetched_at_utc": now_utc.isoformat().replace("+00:00", "Z"),
        "fetched_at_taipei": now_taipei.isoformat(),
        "record_count": len(records),
        "security_code_count": len(codes),
        "security_codes": codes,
        "response_metadata": response_headers,
        "records": records,
    }

    company_profile_payload = {
        "schema_version": 1,
        "dataset": "company_profiles",
        "market": "tpex",
        "source_name": "TPEx 上櫃股票基本資料",
        "source_url": COMPANY_PROFILE_SOURCE_URL,
        "resolved_source_url": profile_final_url,
        "fetched_at_utc": now_utc.isoformat().replace("+00:00", "Z"),
        "fetched_at_taipei": now_taipei.isoformat(),
        "record_count": len(profiles),
        "response_metadata": profile_headers,
        "records": profiles,
    }

    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    COMPANY_PROFILE_OUTPUT_PATH.write_text(
        json.dumps(company_profile_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        f"Wrote {OUTPUT_PATH}: {len(records)} records, "
        f"{len(codes)} security codes, fetched {payload['fetched_at_taipei']}"
    )
    print(
        f"Wrote {COMPANY_PROFILE_OUTPUT_PATH}: {len(profiles)} company profiles, "
        f"fetched {company_profile_payload['fetched_at_taipei']}"
    )


if __name__ == "__main__":
    main()
