"""Download FinMind chip data (margin, foreign shareholding, institutional buy/sell) for the stocks in needed.csv.

No token is used: each GitHub job has its own IP and its own free quota. Stops cleanly when the quota is used up
(HTTP 402) or the time budget runs out; already-written files are skipped on the next run.
Output format is the same as twstock_quant's twsim/stocks.py (chip_frame).
"""
import argparse
import time
from pathlib import Path

import pandas as pd
import requests

URL = "https://api.finmindtrade.com/api/v4/data"
SPEC = {
    "margin": ("TaiwanStockMarginPurchaseShortSale",
               {"MarginPurchaseTodayBalance": "margin_balance", "MarginPurchaseLimit": "margin_limit",
                "ShortSaleTodayBalance": "short_balance", "ShortSaleLimit": "short_limit"}, "2004-01-01"),
    "shareholding": ("TaiwanStockShareholding",
                     {"ForeignInvestmentShares": "foreign_shares", "ForeignInvestmentSharesRatio": "foreign_ratio",
                      "NumberOfSharesIssued": "shares_issued"}, "2004-01-01"),
    "institutional": ("TaiwanStockInstitutionalInvestorsBuySell", None, "2012-05-01"),
    "daytrading": ("TaiwanStockDayTrading", {"Volume": "volume", "BuyAmount": "buy_amount", "SellAmount": "sell_amount"},
                   "2014-01-01"),
}
INST_COLS = ["date", "trust_buy", "trust_sell", "foreign_buy", "foreign_sell"]


class Limit(Exception):
    pass


def frame(kind, raw):
    _, cols, _ = SPEC[kind]
    if kind == "institutional":
        if not len(raw):
            return pd.DataFrame(columns=INST_COLS)
        out = pd.DataFrame({"date": sorted(raw["date"].unique())}).set_index("date")
        for name, tag in (("Investment_Trust", "trust"), ("Foreign_Investor", "foreign")):
            x = raw[raw["name"] == name].drop_duplicates("date").set_index("date")
            out[f"{tag}_buy"], out[f"{tag}_sell"] = x["buy"], x["sell"]
        return out.fillna(0).astype("int64").reset_index()[INST_COLS]
    names = ["date"] + list(cols.values())
    if not len(raw):
        return pd.DataFrame(columns=names)
    df = raw.rename(columns=cols)
    return df[[c for c in names if c in df.columns]].drop_duplicates("date").sort_values("date")


def get(s, dataset, sid, start):
    for _ in range(5):
        try:
            r = s.get(URL, params=dict(dataset=dataset, data_id=sid, start_date=start), timeout=120)
            js = r.json()
        except Exception as e:
            print("  connection error:", str(e)[:80], flush=True)
            time.sleep(20)
            continue
        st = js.get("status", r.status_code)
        if st == 200:
            return pd.DataFrame(js.get("data") or [])
        if st in (402, 429):
            raise Limit()
        raise RuntimeError(f"{dataset} {sid}: {st} {js.get('msg')}")
    raise RuntimeError(f"{dataset} {sid}: retries exhausted")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shard", type=int, default=1)
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--kinds", default="margin,shareholding,institutional")
    ap.add_argument("--max-minutes", type=float, default=18)
    ap.add_argument("--sleep", type=float, default=0.5)
    a = ap.parse_args()
    kinds = [k for k in a.kinds.split(",") if k]
    need = pd.read_csv("needed.csv", dtype=str).fillna("")
    mine = need.iloc[a.shard - 1::a.shards]
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"
    end = time.time() + a.max_minutes * 60
    n = 0
    try:
        for sid, last in zip(mine["stock_id"], mine["last_date"]):
            for k in kinds:
                p = Path("data") / k / f"{sid}.csv.gz"
                if p.exists():
                    continue
                if time.time() > end:
                    raise Limit()
                ds, _, first = SPEC[k]
                raw = pd.DataFrame() if (last and last < first) else get(s, ds, sid, first)
                if not (last and last < first):
                    n += 1
                    time.sleep(a.sleep)
                p.parent.mkdir(parents=True, exist_ok=True)
                frame(k, raw).to_csv(p, index=False)
    except Limit:
        print(f"stopped (quota or time) after {n} requests", flush=True)
    left = sum(1 for sid in mine["stock_id"] for k in kinds if not (Path("data") / k / f"{sid}.csv.gz").exists())
    print(f"shard {a.shard}/{a.shards}: {n} requests this run, {left} files left in this shard", flush=True)


if __name__ == "__main__":
    main()
