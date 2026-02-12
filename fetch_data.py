#!/usr/bin/env python3
"""
한국 우선주 괴리율 데이터 수집 스크립트
Yahoo Finance에서 보통주/우선주 가격 데이터를 가져와 data.js를 생성한다.
"""

import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import yfinance as yf
import pandas as pd

# 종목 페어 설정을 config.json에서 로드
CONFIG_PATH = Path(__file__).parent / "config.json"
with open(CONFIG_PATH, encoding="utf-8") as f:
    PAIRS = json.load(f)


def main():
    # 모든 티커 수집 (중복 제거)
    all_tickers = list(
        dict.fromkeys(
            ticker
            for pair in PAIRS
            for ticker in [pair["commonTicker"], pair["preferredTicker"]]
        )
    )

    end_date = datetime.now()
    start_date = end_date - timedelta(days=3 * 365 + 30)  # 3년 + 여유분

    print(f"Downloading data for {len(all_tickers)} tickers...")
    print(f"Period: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")

    # 일괄 다운로드
    data = yf.download(
        all_tickers,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        auto_adjust=True,
        progress=True,
    )

    close = data["Close"]

    # 각 페어별로 괴리율 계산
    pairs_result = []

    for pair in PAIRS:
        ct = pair["commonTicker"]
        pt = pair["preferredTicker"]

        common_close = close[ct].dropna()
        preferred_close = close[pt].dropna()

        # 두 시리즈의 공통 날짜만 사용
        common_dates = common_close.index.intersection(preferred_close.index)
        if len(common_dates) == 0:
            print(f"  WARNING: No overlapping dates for {pair['name']}, skipping.")
            continue

        c = common_close.loc[common_dates]
        p = preferred_close.loc[common_dates]

        # 괴리율: (보통주 - 우선주) / 보통주 * 100
        spread = (c - p) / c * 100

        # 히스토리 구성
        history = []
        for date in common_dates:
            history.append(
                {
                    "date": date.strftime("%Y-%m-%d"),
                    "commonPrice": round(float(c.loc[date]), 0),
                    "preferredPrice": round(float(p.loc[date]), 0),
                    "spread": round(float(spread.loc[date]), 2),
                }
            )

        # 현재 (마지막 거래일) 정보
        latest = history[-1]
        prev = history[-2] if len(history) >= 2 else latest
        spread_change = round(latest["spread"] - prev["spread"], 2)

        pair_data = {
            "id": pair["id"],
            "name": pair["name"],
            "commonName": pair["commonName"],
            "preferredName": pair["preferredName"],
            "current": {
                "commonPrice": latest["commonPrice"],
                "preferredPrice": latest["preferredPrice"],
                "spread": latest["spread"],
                "spreadChange": spread_change,
            },
            "history": history,
        }
        pairs_result.append(pair_data)

        print(
            f"  {pair['name']}: {len(history)} days, "
            f"current spread {latest['spread']:.2f}% "
            f"({'↑' if spread_change > 0 else '↓'}{abs(spread_change):.2f}%p)"
        )

    # 일별 전체 평균 괴리율 계산
    daily_spreads = defaultdict(list)
    for pair_data in pairs_result:
        for h in pair_data["history"]:
            daily_spreads[h["date"]].append(h["spread"])

    avg_history = []
    for date in sorted(daily_spreads.keys()):
        spreads = daily_spreads[date]
        avg_history.append({
            "date": date,
            "commonPrice": 0,
            "preferredPrice": 0,
            "spread": round(sum(spreads) / len(spreads), 2),
        })

    if avg_history:
        latest_avg = avg_history[-1]
        prev_avg = avg_history[-2] if len(avg_history) >= 2 else latest_avg
        avg_change = round(latest_avg["spread"] - prev_avg["spread"], 2)
        avg_pair = {
            "id": "_average",
            "name": "전체 평균",
            "commonName": "",
            "preferredName": "",
            "isAverage": True,
            "current": {
                "commonPrice": 0,
                "preferredPrice": 0,
                "spread": latest_avg["spread"],
                "spreadChange": avg_change,
            },
            "history": avg_history,
        }
        print(
            f"  전체 평균: {len(avg_history)} days, "
            f"current spread {latest_avg['spread']:.2f}% "
            f"({'↑' if avg_change > 0 else '↓'}{abs(avg_change):.2f}%p)"
        )

    # 전체 평균도 포함하여 괴리율 높은 순 정렬
    if avg_history:
        pairs_result.append(avg_pair)
    pairs_result.sort(key=lambda p: p["current"]["spread"], reverse=True)

    # data.js 출력
    stock_data = {
        "lastUpdated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pairs": pairs_result,
    }

    js_content = "const STOCK_DATA = " + json.dumps(stock_data, ensure_ascii=False, indent=2) + ";\n"

    output_path = "data.js"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(js_content)

    print(f"\nGenerated {output_path} ({len(pairs_result)} pairs, {len(js_content)} bytes)")


if __name__ == "__main__":
    main()
