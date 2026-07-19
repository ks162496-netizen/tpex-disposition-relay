# TPEx disposition relay

This public repository relays the official TPEx disposition-security CSV into a
small validated JSON file that Cloudflare Workers can read reliably.

## Data flow

1. GitHub Actions downloads the official TPEx CSV.
2. `scripts/update_tpex.py` rejects HTML/error pages, validates the code column,
   and records the source URL and fetch time.
3. The workflow commits `tpex_disposition.json`.
4. The TradingView Telegram Worker reads the raw JSON and fails closed when the
   file is invalid or more than 96 hours old.

Official source:

- [TPEx 上櫃處置有價證券資訊](https://www.tpex.org.tw/zh-tw/announce/market/disposal.html)
- [Generated JSON](https://raw.githubusercontent.com/ks162496-netizen/tpex-disposition-relay/main/tpex_disposition.json)

## Automatic schedule (Taiwan time)

The workflow refreshes at 07:30, 08:20, 14:30, and 18:00 on weekdays. It can
also be started manually from **Actions → Update TPEx disposition list → Run
workflow**.

This repository contains public market data only. It does not store Telegram
tokens, chat IDs, TradingView payloads, or brokerage credentials.
