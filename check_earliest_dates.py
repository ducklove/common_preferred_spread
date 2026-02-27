#!/usr/bin/env python3
"""
Check Yahoo Finance for the earliest available historical data date
for each common/preferred stock pair in config.json.
"""

import json
import time
import yfinance as yf
import warnings

warnings.filterwarnings("ignore")

CONFIG_PATH = "/home/cantabile/Works/common_preferred_spread/config.json"

def get_earliest_date(ticker_str):
    """Fetch the earliest available date for a given ticker using period='max'."""
    try:
        ticker = yf.Ticker(ticker_str)
        hist = ticker.history(period="max")
        if hist.empty:
            return None
        return hist.index[0].date()
    except Exception as e:
        print(f"  [ERROR] {ticker_str}: {e}")
        return None

def main():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)

    results = []
    total = len(config)

    print(f"Checking {total} stock pairs against Yahoo Finance...\n")

    for i, entry in enumerate(config, 1):
        name = entry["name"]
        common_ticker = entry["commonTicker"]
        preferred_ticker = entry["preferredTicker"]

        print(f"[{i}/{total}] {name}: {common_ticker} / {preferred_ticker} ...")

        common_earliest = get_earliest_date(common_ticker)
        time.sleep(0.5)

        preferred_earliest = get_earliest_date(preferred_ticker)
        time.sleep(0.5)

        if common_earliest and preferred_earliest:
            effective_start = max(common_earliest, preferred_earliest)
        elif common_earliest:
            effective_start = common_earliest
        elif preferred_earliest:
            effective_start = preferred_earliest
        else:
            effective_start = None

        results.append({
            "name": name,
            "commonTicker": common_ticker,
            "preferredTicker": preferred_ticker,
            "commonEarliest": common_earliest,
            "preferredEarliest": preferred_earliest,
            "effectiveStart": effective_start,
        })

    # Sort by effective start date (None values go to the end)
    results.sort(key=lambda x: str(x["effectiveStart"]) if x["effectiveStart"] else "9999-99-99")

    # Print formatted table
    print("\n" + "=" * 120)
    print(f"{'Name':<16} {'Common':<14} {'Preferred':<14} {'Common Start':<16} {'Preferred Start':<16} {'Effective Start':<16}")
    print("-" * 120)

    for r in results:
        common_str = str(r["commonEarliest"]) if r["commonEarliest"] else "N/A"
        preferred_str = str(r["preferredEarliest"]) if r["preferredEarliest"] else "N/A"
        effective_str = str(r["effectiveStart"]) if r["effectiveStart"] else "N/A"

        print(
            f"{r['name']:<16} "
            f"{r['commonTicker']:<14} "
            f"{r['preferredTicker']:<14} "
            f"{common_str:<16} "
            f"{preferred_str:<16} "
            f"{effective_str:<16}"
        )

    print("=" * 120)

if __name__ == "__main__":
    main()
