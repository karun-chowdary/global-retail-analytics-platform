#!/usr/bin/env python3
"""Generate daily partner-feed files to imitate files arriving in a landing zone.

For every day it writes two files into <out-dir>/<YYYY-MM-DD>/:
  order_events_<YYYYMMDD>.json         newline-delimited JSON, order status events
  delivery_updates_<YYYYMMDD>.parquet  carrier delivery updates

Built-in problems for your pipeline to handle:
  * ~2% duplicate JSON events and ~1% duplicate Parquet rows
  * SCHEMA DRIFT: from --drift-from-day onward the JSON gains a new field, carrier_code
  * one day (--late-day) arrives with timestamps from the previous day (late data)

Example:
  python sources/generate_daily_files.py --days 7 --events-per-day 800
"""
import argparse
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

STATUSES = ["created", "approved", "invoiced", "shipped", "delivered", "canceled"]
CHANNELS = ["marketplace_api", "partner_portal", "mobile_app"]
CARRIERS = ["CARR-A", "CARR-B", "CARR-C", "CARR-D"]


def make_events(order_ids, day, count, rng, with_carrier, late):
    base = datetime.combine(day, datetime.min.time())
    if late:
        base -= timedelta(days=1)
    events = []
    for _ in range(count):
        event = {
            "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "order_id": rng.choice(order_ids),
            "status": rng.choice(STATUSES),
            "event_timestamp": (base + timedelta(seconds=rng.randint(0, 86399))).isoformat(),
            "channel": rng.choice(CHANNELS),
        }
        if with_carrier:
            event["carrier_code"] = rng.choice(CARRIERS)
        events.append(event)
    events += [dict(e) for e in rng.sample(events, k=max(1, int(count * 0.02)))]
    rng.shuffle(events)
    return events


def make_delivery_updates(order_ids, day, count, rng):
    base = datetime.combine(day, datetime.min.time())
    rows = [{
        "order_id": rng.choice(order_ids),
        "carrier": rng.choice(CARRIERS),
        "estimated_days": rng.randint(1, 20),
        "updated_at": base + timedelta(seconds=rng.randint(0, 86399)),
    } for _ in range(count)]
    rows += [dict(r) for r in rng.sample(rows, k=max(1, int(count * 0.01)))]
    rng.shuffle(rows)
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser(description="Generate daily landing-zone files.")
    parser.add_argument("--orders-csv", default="data/raw/olist_orders_dataset.csv")
    parser.add_argument("--out-dir", default="data/landing")
    parser.add_argument("--start-date", default="2018-09-01", help="YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--events-per-day", type=int, default=800)
    parser.add_argument("--drift-from-day", type=int, default=4,
                        help="1-based day number from which carrier_code appears (0 = never)")
    parser.add_argument("--late-day", type=int, default=5,
                        help="1-based day number whose events carry yesterday's timestamps (0 = none)")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    order_ids = pd.read_csv(args.orders_csv, usecols=["order_id"])["order_id"].tolist()
    rng = random.Random(args.seed)
    start = datetime.strptime(args.start_date, "%Y-%m-%d").date()

    for n in range(1, args.days + 1):
        day = start + timedelta(days=n - 1)
        folder = Path(args.out_dir) / day.isoformat()
        folder.mkdir(parents=True, exist_ok=True)
        stamp = day.strftime("%Y%m%d")

        drift = args.drift_from_day > 0 and n >= args.drift_from_day
        late = args.late_day > 0 and n == args.late_day
        events = make_events(order_ids, day, args.events_per_day, rng, drift, late)
        json_path = folder / f"order_events_{stamp}.json"
        with open(json_path, "w", encoding="utf-8") as fh:
            for event in events:
                fh.write(json.dumps(event) + "\n")

        updates = make_delivery_updates(order_ids, day, args.events_per_day // 2, rng)
        parquet_path = folder / f"delivery_updates_{stamp}.parquet"
        updates.to_parquet(parquet_path, index=False)

        notes = []
        if drift:
            notes.append("schema drift: carrier_code")
        if late:
            notes.append("late data")
        print(f"{day}: {len(events)} events, {len(updates)} updates"
              + (f" ({', '.join(notes)})" if notes else ""))


if __name__ == "__main__":
    main()
