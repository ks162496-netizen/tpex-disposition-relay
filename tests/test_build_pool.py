"""Offline tests for scripts/build_pool.py; synthetic rows only, no network."""
import json
import unittest
from datetime import date, datetime

from scripts import build_pool as bp

TARGET = date(2026, 11, 6)
DATA_DATE = date(2026, 11, 5)


class DateParsingTests(unittest.TestCase):
    def test_formats(self):
        self.assertEqual(bp.parse_date("1151105"), date(2026, 11, 5))
        self.assertEqual(bp.parse_date("20261105"), date(2026, 11, 5))
        self.assertEqual(bp.parse_date("115/11/05"), date(2026, 11, 5))
        self.assertEqual(bp.parse_date("2026/11/05"), date(2026, 11, 5))
        self.assertEqual(bp.parse_date("2026-11-05"), date(2026, 11, 5))
        self.assertIsNone(bp.parse_date(""))
        self.assertIsNone(bp.parse_date("--"))
        self.assertIsNone(bp.parse_date("1151345"))

    def test_period(self):
        self.assertEqual(bp.parse_period("115/11/03~115/11/14"), (date(2026, 11, 3), date(2026, 11, 14)))
        self.assertEqual(bp.parse_period("1151103~1151114"), (date(2026, 11, 3), date(2026, 11, 14)))
        self.assertEqual(bp.parse_period("115/11/03-115/11/14"), (date(2026, 11, 3), date(2026, 11, 14)))
        self.assertIsNone(bp.parse_period(""))

    def test_numbers_and_markers(self):
        self.assertEqual(bp.parse_number("1,234,567"), 1234567.0)
        self.assertEqual(bp.parse_number(" 45.60 "), 45.6)
        self.assertIsNone(bp.parse_number("--"))
        self.assertIsNone(bp.parse_number("---"))
        self.assertIsNone(bp.parse_number(""))
        self.assertTrue(bp.is_marked("Y"))
        self.assertTrue(bp.is_marked("Ｙ"))
        self.assertFalse(bp.is_marked(""))
        self.assertFalse(bp.is_marked("N"))


class CalendarTests(unittest.TestCase):
    holidays = bp.holidays_from_rows([
        {"Name": "中華民國開國紀念日", "Date": "1160101", "Description": "依規定放假1日。"},
        {"Name": "國曆新年開始交易日", "Date": "1160104", "Description": "國曆新年開始交易。"},
        {"Name": "測試休市", "Date": "1151109", "Description": "放假"},
    ])

    def test_holiday_rows(self):
        self.assertIn(date(2027, 1, 1), self.holidays)
        self.assertNotIn(date(2027, 1, 4), self.holidays)

    def test_morning_run_targets_today(self):
        now = datetime(2026, 11, 6, 6, 40, tzinfo=bp.TAIPEI)
        self.assertEqual(bp.resolve_dates(now, set()), (date(2026, 11, 6), date(2026, 11, 5)))

    def test_evening_run_targets_next_day_skipping_weekend_and_holiday(self):
        now = datetime(2026, 11, 6, 17, 40, tzinfo=bp.TAIPEI)  # Friday
        self.assertEqual(bp.resolve_dates(now, set()), (date(2026, 11, 9), date(2026, 11, 6)))
        self.assertEqual(bp.resolve_dates(now, self.holidays), (date(2026, 11, 10), date(2026, 11, 6)))

    def test_weekend_run(self):
        now = datetime(2026, 11, 7, 10, 0, tzinfo=bp.TAIPEI)  # Saturday morning
        self.assertEqual(bp.resolve_dates(now, set()), (date(2026, 11, 9), date(2026, 11, 6)))


def company(code, industry="24", name=None):
    return {"code": code, "name": name or f"N{code}", "industry": industry, "date": "2026-11-05"}


def quote(code, close=100.0, volume=1_000_000, day="2026-11-05"):
    return {"code": code, "name": f"Q{code}", "date": day, "close": close, "volume": volume}


class RuleTests(unittest.TestCase):
    def ctx(self, **overrides):
        codes = ["2330", "2303", "2317", "2454", "2603", "1101", "1216", "2882", "3008", "3711", "2412", "1301"]
        base = {
            "target": TARGET, "data_date": DATA_DATE,
            "company": [company(c) for c in codes] + [company("9105"), company("2880", industry="")],
            "daily": [quote(c) for c in codes] + [
                quote("0050"), quote("9105"), quote("2880"), quote("2881A"), quote("9999")],
            "common": None, "changed": [], "status": [], "disposition": [], "halt": [],
            "daytrade": [{"code": c, "date": "2026-11-06", "sell_first_suspended": c == "2330"}
                         for c in codes + ["2880", "9105"]],
        }
        base["daily"] = [q for q in base["daily"] if q["code"] not in overrides.get("drop_quotes", ())]
        overrides.pop("drop_quotes", None)
        base.update(overrides)
        return base

    def reasons(self, excluded):
        return {e["symbol"]: e["reason"] for e in excluded}

    def test_all_rules_twse(self):
        ctx = self.ctx()
        ctx["daily"] = [q for q in ctx["daily"] if q["code"] not in ("2303", "2317", "2454", "1101", "1216")] + [
            quote("2303", close=19.99), quote("2317", close=600.01), quote("2454", volume=499_999),
            quote("1101", day="2026-11-04"), quote("1216", close=None)]
        ctx["company"] = [c for c in ctx["company"] if c["code"] != "3711"]
        ctx["changed"] = [{"code": "2603"}]
        ctx["disposition"] = [{"code": "2882", "start": "2026-11-03", "end": "2026-11-14"},
                              {"code": "3008", "start": "2026-10-01", "end": "2026-11-05"}]
        ctx["halt"] = [{"code": "3008", "halt": "2026-11-06", "resume": None},
                       {"code": "2412", "halt": "2026-10-01", "resume": "2026-10-03"}]
        ctx["daytrade"] = [d for d in ctx["daytrade"] if d["code"] != "1301"]
        members, excluded = bp.evaluate_market("TWSE", ctx)
        r = self.reasons(excluded)
        self.assertEqual(r["0050"], "not_common_stock")
        self.assertEqual(r["9105"], "not_common_stock")
        self.assertEqual(r["2881A"], "not_common_stock")
        self.assertEqual(r["9999"], "no_company_profile")
        self.assertEqual(r["3711"], "no_company_profile")
        self.assertEqual(r["2880"], "no_industry")
        self.assertEqual(r["2303"], "price_out_of_range")
        self.assertEqual(r["2317"], "price_out_of_range")
        self.assertEqual(r["1216"], "price_out_of_range")
        self.assertEqual(r["2454"], "low_volume")
        self.assertEqual(r["1101"], "stale_quote")
        self.assertEqual(r["2603"], "changed_trading")
        self.assertEqual(r["2882"], "disposition")
        self.assertEqual(r["3008"], "halted")
        self.assertEqual(r["1301"], "not_daytrade_listed")
        self.assertEqual([m["symbol"] for m in members], ["2330", "2412"])
        tsmc = members[0]
        self.assertEqual(tsmc["volume_lots"], 1000)
        self.assertTrue(tsmc["sell_first_suspended"])
        self.assertFalse(members[1]["sell_first_suspended"])

    def test_boundaries_inclusive(self):
        ctx = self.ctx()
        ctx["daily"] = [quote("2330", close=20.0, volume=500_000), quote("2303", close=600.0)]
        members, _ = bp.evaluate_market("TWSE", ctx)
        self.assertEqual({m["symbol"] for m in members}, {"2330", "2303"})

    def test_tpex_common_from_isin_and_status(self):
        ctx = self.ctx(common={c: {"code": c, "name": "x"} for c in ["2330", "2303", "2317"]},
                       status=[{"code": "2303", "flags": ["ManagedStock"]}, {"code": "2317", "flags": []}])
        members, excluded = bp.evaluate_market("TPEX", ctx)
        r = self.reasons(excluded)
        self.assertEqual(r["2454"], "not_common_stock")
        self.assertEqual(r["2303"], "special_status")
        self.assertEqual({m["symbol"] for m in members}, {"2330", "2317"})

    def test_missing_daytrade_list_excludes_all(self):
        members, excluded = bp.evaluate_market("TWSE", self.ctx(daytrade=None))
        self.assertEqual(members, [])
        self.assertIn("not_daytrade_listed", self.reasons(excluded).values())

    def test_halt_timeline(self):
        events = [{"code": "1", "halt": "2026-11-01", "resume": "2026-11-04"},
                  {"code": "1", "halt": "2026-11-06", "resume": "2026-11-09"}]
        self.assertTrue(bp.halted_at(events, "1", TARGET))
        self.assertFalse(bp.halted_at(events, "1", date(2026, 11, 5)))
        self.assertFalse(bp.halted_at(events, "1", date(2026, 11, 9)))
        self.assertFalse(bp.halted_at(events, "2", TARGET))


class ParserTests(unittest.TestCase):
    def test_isin_html(self):
        page = (
            "<html><table><tr><td>有價證券代號及名稱 </td><td>國際證券辨識號碼(ISIN Code)</td><td>上市日</td>"
            "<td>市場別</td><td>產業別</td><td>CFICode</td><td>備註</td></tr>"
            "<tr><td colspan=7 ><B> 股票 <B> </td></tr>"
            "<tr><td>6488　環球晶</td><td>TW0006488000</td><td>2015/09/25</td><td>上櫃</td>"
            "<td>半導體業</td><td>ESVUFR</td><td></td></tr>"
            "<tr><td>1240　茂生農經</td><td>TW0001240004</td><td>2017/03/23</td><td>上櫃</td>"
            "<td>農業科技業</td><td>ESVTFR</td><td></td></tr>"
            "<tr><td colspan=7 ><B> 上櫃認購(售)權證 <B> </td></tr>"
            "<tr><td>700001　某權證</td><td>TW00X</td><td>2026/01/01</td><td>上櫃</td>"
            "<td></td><td>ESVUFR</td><td></td></tr></table></html>"
        ).encode("cp950")
        rows = bp.parse_isin_html(page)
        self.assertEqual(rows, [{"code": "6488", "name": "環球晶", "industry": "半導體業"}])

    def test_tpex_daytrade(self):
        payload = {"stat": "ok", "tables": [{"fields": ["證券代號", "證券名稱", "暫停現股賣出後現款買進當沖註記"],
                                             "data": [["6488", "環球晶", ""], ["8069", "元太", "Y"]],
                                             "totalCount": 2}]}
        rows = bp.parse_tpex_daytrade(json.dumps(payload).encode(), TARGET)
        self.assertEqual([(r["code"], r["sell_first_suspended"]) for r in rows], [("6488", False), ("8069", True)])
        empty = {"stat": "ok", "tables": [{"fields": ["證券代號"], "data": [], "totalCount": 0}]}
        self.assertIsNone(bp.parse_tpex_daytrade(json.dumps(empty).encode(), TARGET))
        bad = {"stat": "ok", "tables": [{"fields": ["證券代號"], "data": [["1"]], "totalCount": 5}]}
        with self.assertRaises(RuntimeError):
            bp.parse_tpex_daytrade(json.dumps(bad).encode(), TARGET)

    def test_twse_normalizers(self):
        raw = json.dumps([{"Date": "1151105", "Code": "2330", "Name": "台積電", "ClosingPrice": "1,450.00",
                           "TradeVolume": "25,000,000"}]).encode()
        self.assertEqual(bp.SPECS["TWSE_DAILY"](raw), [{"code": "2330", "name": "台積電", "date": "2026-11-05",
                                                         "close": 1450.0, "volume": 25000000.0}])
        with self.assertRaises(RuntimeError):
            bp.SPECS["TWSE_CHANGED"](json.dumps([{"Unexpected": "2330"}]).encode())
        self.assertEqual(bp.SPECS["TWSE_CHANGED"](b"[]"), [])

    def test_render_is_valid_json(self):
        doc = {"schema": bp.SCHEMA, "counts": {"members": 1},
               "members": [{"market": "TWSE", "symbol": "2330"}], "excluded": []}
        self.assertEqual(json.loads(bp.render(doc)), doc)


if __name__ == "__main__":
    unittest.main()
