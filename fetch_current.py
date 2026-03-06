#!/usr/bin/env python3
"""
현재 주가만 빠르게 가져와서 current.json에 저장하는 스크립트.
장중 10분 간격으로 실행하여 실시간 시세를 제공한다.
"""

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yfinance as yf

KST = timezone(timedelta(hours=9))
CONFIG_PATH = Path(__file__).parent / "config.json"
OUTPUT_PATH = Path(__file__).parent / "current.json"

with open(CONFIG_PATH, encoding="utf-8") as f:
    PAIRS = json.load(f)


def main():
    all_tickers = list(
        dict.fromkeys(
            ticker
            for pair in PAIRS
            for ticker in [pair["commonTicker"], pair["preferredTicker"]]
        )
    )

    print(f"{len(all_tickers)}개 티커 현재가 조회 중...")

    data = yf.download(all_tickers, period="5d", auto_adjust=False, progress=False)
    close = data["Close"]

    prices = {}
    for pair in PAIRS:
        ct = pair["commonTicker"]
        pt = pair["preferredTicker"]
        try:
            c_series = close[ct].dropna()
            p_series = close[pt].dropna()
            common_price = round(float(c_series.iloc[-1]), 0)
            preferred_price = round(float(p_series.iloc[-1]), 0)
            spread = round((common_price - preferred_price) / common_price * 100, 2)

            # 일간 등락률
            if len(c_series) >= 2:
                prev_c = float(c_series.iloc[-2])
                common_change = round((common_price - prev_c) / prev_c * 100, 2)
            else:
                common_change = 0
            if len(p_series) >= 2:
                prev_p = float(p_series.iloc[-2])
                preferred_change = round((preferred_price - prev_p) / prev_p * 100, 2)
            else:
                preferred_change = 0

            prices[pair["id"]] = {
                "commonPrice": common_price,
                "preferredPrice": preferred_price,
                "spread": spread,
                "commonChange": common_change,
                "preferredChange": preferred_change,
            }
        except (KeyError, IndexError) as e:
            print(f"  WARNING: {pair['name']} 현재가 조회 실패: {e}")
            continue

    # 전체 평균 괴리율 (그룹별 대표 종목 기준)
    groups = defaultdict(list)
    for pair in PAIRS:
        if pair["id"] in prices:
            groups[pair["commonName"]].append(
                {"id": pair["id"], "spread": prices[pair["id"]]["spread"]}
            )

    rep_spreads = []
    for items in groups.values():
        best = max(items, key=lambda x: x["spread"])
        rep_spreads.append(best["spread"])

    avg_spread = round(sum(rep_spreads) / len(rep_spreads), 2) if rep_spreads else None

    result = {
        "lastUpdated": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "prices": prices,
        "averageSpread": avg_spread,
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"current.json 갱신 완료 ({len(prices)}개 종목, 평균 괴리율 {avg_spread}%)")


if __name__ == "__main__":
    main()
