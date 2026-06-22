"""Deterministic synthetic SaaS event generator for the Certified Metrics Framework."""
from __future__ import annotations
import csv
import os
import random
from datetime import date, datetime, timedelta

BASE_DATE = date(2026, 1, 1)
DAYS = 90
PLANS = {"free": 0.0, "plus": 12.0, "pro": 24.0, "team": 60.0}
COUNTRIES = ["US", "GB", "DE", "JP", "BR"]
CHANNELS = ["organic", "paid_search", "referral", "social"]


def _write(out_dir: str, name: str, header: list[str], rows: list[list]) -> int:
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, name), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return len(rows)


def generate(out_dir: str, *, seed: int = 42) -> dict[str, int]:
    rng = random.Random(seed)
    n_users = 1000

    users, usage, subs, refunds = [], [], [], []
    event_id = 0
    sub_id = 0
    refund_id = 0

    for uid in range(1, n_users + 1):
        signup_offset = rng.randint(0, DAYS - 1)
        signup = BASE_DATE + timedelta(days=signup_offset)
        users.append([uid, signup.isoformat(), rng.choice(COUNTRIES), rng.choice(CHANNELS)])

        # Engagement: each user active on a random subset of days after signup.
        active_prob = rng.uniform(0.1, 0.7)
        plan = rng.choices(list(PLANS), weights=[55, 20, 15, 10])[0]
        for d in range(signup_offset, DAYS):
            if rng.random() > active_prob:
                continue
            day = BASE_DATE + timedelta(days=d)
            n_events = rng.randint(1, 5)
            for _ in range(n_events):
                event_id += 1
                ts = datetime.combine(day, datetime.min.time()) + timedelta(
                    seconds=rng.randint(0, 86399)
                )
                usage.append([
                    event_id, uid, ts.isoformat(sep=" "),
                    rng.choice(["open", "upload", "share", "preview"]),
                    rng.randint(0, 50_000_000),
                ])

        # Paid users get a subscription row per active month with possible refunds.
        if plan != "free":
            for month_start in (date(2026, 1, 1), date(2026, 2, 1), date(2026, 3, 1)):
                if month_start < signup.replace(day=1):
                    continue
                sub_id += 1
                mrr = PLANS[plan]
                subs.append([sub_id, uid, plan, month_start.isoformat(), f"{mrr:.2f}"])
                if rng.random() < 0.08:  # ~8% of paid months see a partial refund
                    refund_id += 1
                    amt = round(mrr * rng.uniform(0.25, 1.0), 2)
                    refunds.append([refund_id, sub_id, month_start.isoformat(), f"{amt:.2f}"])

    counts = {}
    counts["users.csv"] = _write(out_dir, "users.csv",
        ["user_id", "signup_date", "country", "acquisition_channel"], users)
    counts["usage_events.csv"] = _write(out_dir, "usage_events.csv",
        ["event_id", "user_id", "event_ts", "event_type", "storage_bytes"], usage)
    counts["subscriptions.csv"] = _write(out_dir, "subscriptions.csv",
        ["subscription_id", "user_id", "plan_id", "period_month", "mrr_amount"], subs)
    counts["refunds.csv"] = _write(out_dir, "refunds.csv",
        ["refund_id", "subscription_id", "period_month", "refund_amount"], refunds)
    return counts


if __name__ == "__main__":
    print(generate("seeds"))
