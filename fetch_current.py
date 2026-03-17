#!/usr/bin/env python3
"""
네이버 실시간 API에서 현재 주가를 가져와 current.json에 저장하는 스크립트.
장중 10분 간격으로 실행하여 실시간 시세를 제공한다.
"""

import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen

KST = timezone(timedelta(hours=9))
CONFIG_PATH = Path(__file__).parent / "config.json"
OUTPUT_PATH = Path(__file__).parent / "current.json"
STOCK_API_URL = "https://polling.finance.naver.com/api/realtime/domestic/stock/{code}"
INDEX_API_URL = "https://polling.finance.naver.com/api/realtime/domestic/index/{code}"
USER_AGENT = "Mozilla/5.0"
REQUEST_TIMEOUT = 10
MAX_WORKERS = 8

with open(CONFIG_PATH, encoding="utf-8") as f:
    PAIRS = json.load(f)


def ticker_to_code(ticker):
    return ticker.split(".")[0]


def parse_int(value):
    if value in (None, ""):
        return None
    return int(str(value).replace(",", ""))


def parse_float(value):
    if value in (None, ""):
        return None
    return float(str(value).replace(",", ""))


def round_or_none(value, digits=2):
    if value is None:
        return None
    return round(value, digits)


def compute_spread(common_price, preferred_price):
    if common_price in (None, 0) or preferred_price is None:
        return None
    return round((common_price - preferred_price) / common_price * 100, 2)


def fetch_json(url):
    request = Request(
        url,
        headers={"User-Agent": USER_AGENT},
    )
    with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
        payload = json.load(response)

    datas = payload.get("datas") or []
    if not datas:
        raise ValueError("빈 응답")
    return datas[0]


def fetch_stock_quote(code):
    return fetch_json(STOCK_API_URL.format(code=code))


def fetch_index_quote(code):
    return fetch_json(INDEX_API_URL.format(code=code))


def fetch_all_quotes(codes):
    if not codes:
        return {}, {}

    quotes = {}
    errors = {}

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(codes))) as executor:
        future_map = {executor.submit(fetch_stock_quote, code): code for code in codes}
        for future in as_completed(future_map):
            code = future_map[future]
            try:
                quotes[code] = future.result()
            except Exception as exc:  # noqa: BLE001 - 개별 종목 실패는 계속 진행
                errors[code] = str(exc)

    return quotes, errors


def build_market_summary(market_quote):
    if not market_quote:
        return None
    return {
        "id": market_quote.get("itemCode") or "KOSPI",
        "name": market_quote.get("stockName") or "KOSPI",
        "price": round_or_none(
            parse_float(market_quote.get("closePriceRaw") or market_quote.get("closePrice"))
        ),
        "change": round_or_none(
            parse_float(
                market_quote.get("compareToPreviousClosePriceRaw")
                or market_quote.get("compareToPreviousClosePrice")
            )
        ),
        "changePct": round_or_none(
            parse_float(
                market_quote.get("fluctuationsRatioRaw")
                or market_quote.get("fluctuationsRatio")
            )
        ),
        "marketStatus": market_quote.get("marketStatus"),
    }


def build_summary(prices, market_summary=None):
    groups = defaultdict(list)
    for pair in PAIRS:
        price = prices.get(pair["id"])
        if price and price["spread"] is not None:
            groups[pair["commonName"]].append({"pair": pair, "price": price})

    representatives = []
    for items in groups.values():
        representatives.append(max(items, key=lambda item: item["price"]["spread"]))

    if not representatives:
        return None

    avg_spread = round(
        sum(item["price"]["spread"] for item in representatives) / len(representatives), 2
    )

    change_values = [
        item["price"]["spreadChange"]
        for item in representatives
        if item["price"]["spreadChange"] is not None
    ]
    avg_spread_change = (
        round(sum(change_values) / len(change_values), 2) if change_values else None
    )
    common_change_values = [
        item["price"]["commonChange"]
        for item in representatives
        if item["price"]["commonChange"] is not None
    ]
    avg_common_change = (
        round(sum(common_change_values) / len(common_change_values), 2)
        if common_change_values
        else None
    )
    preferred_change_values = [
        item["price"]["preferredChange"]
        for item in representatives
        if item["price"]["preferredChange"] is not None
    ]
    avg_preferred_change = (
        round(sum(preferred_change_values) / len(preferred_change_values), 2)
        if preferred_change_values
        else None
    )

    widening_candidates = [
        item
        for item in representatives
        if item["price"]["spreadChange"] is not None and item["price"]["spreadChange"] > 0
    ]
    narrowing_candidates = [
        item
        for item in representatives
        if item["price"]["spreadChange"] is not None and item["price"]["spreadChange"] < 0
    ]

    def serialize_leader(item):
        if not item:
            return None
        pair = item["pair"]
        price = item["price"]
        return {
            "id": pair["id"],
            "name": pair["name"],
            "commonName": pair["commonName"],
            "preferredName": pair["preferredName"],
            "spread": price["spread"],
            "spreadChange": price["spreadChange"],
        }

    return {
        "market": market_summary,
        "averageSpread": avg_spread,
        "averageSpreadChange": avg_spread_change,
        "averageCommonChange": avg_common_change,
        "averagePreferredChange": avg_preferred_change,
        "representativeCount": len(representatives),
        "topWidening": serialize_leader(
            max(widening_candidates, key=lambda item: item["price"]["spreadChange"])
            if widening_candidates
            else None
        ),
        "topNarrowing": serialize_leader(
            min(narrowing_candidates, key=lambda item: item["price"]["spreadChange"])
            if narrowing_candidates
            else None
        ),
    }


def main():
    all_tickers = list(
        dict.fromkeys(
            ticker
            for pair in PAIRS
            for ticker in [pair["commonTicker"], pair["preferredTicker"]]
        )
    )
    all_codes = [ticker_to_code(ticker) for ticker in all_tickers]

    print(f"{len(all_codes)}개 종목 현재가 조회 중...")

    quotes, quote_errors = fetch_all_quotes(all_codes)
    market_quote = None
    try:
        market_quote = fetch_index_quote("KOSPI")
    except Exception as exc:  # noqa: BLE001 - KOSPI 실패 시 나머지 데이터는 유지
        print(f"  WARNING: KOSPI 현재가 조회 실패: {exc}")

    for code, error in quote_errors.items():
        print(f"  WARNING: {code} 현재가 조회 실패: {error}")

    prices = {}
    traded_at_values = []

    for pair in PAIRS:
        common_code = ticker_to_code(pair["commonTicker"])
        preferred_code = ticker_to_code(pair["preferredTicker"])
        common_quote = quotes.get(common_code)
        preferred_quote = quotes.get(preferred_code)

        if not common_quote or not preferred_quote:
            print(f"  WARNING: {pair['name']} 현재가 조회 실패: 네이버 응답 누락")
            continue

        try:
            common_price = parse_int(
                common_quote.get("closePriceRaw") or common_quote.get("closePrice")
            )
            preferred_price = parse_int(
                preferred_quote.get("closePriceRaw")
                or preferred_quote.get("closePrice")
            )
            common_delta = parse_int(
                common_quote.get("compareToPreviousClosePriceRaw")
                or common_quote.get("compareToPreviousClosePrice")
            )
            preferred_delta = parse_int(
                preferred_quote.get("compareToPreviousClosePriceRaw")
                or preferred_quote.get("compareToPreviousClosePrice")
            )

            previous_common_price = (
                common_price - common_delta
                if common_price is not None and common_delta is not None
                else None
            )
            previous_preferred_price = (
                preferred_price - preferred_delta
                if preferred_price is not None and preferred_delta is not None
                else None
            )

            spread = compute_spread(common_price, preferred_price)
            previous_spread = compute_spread(
                previous_common_price, previous_preferred_price
            )
            spread_change = (
                round(spread - previous_spread, 2)
                if spread is not None and previous_spread is not None
                else None
            )

            prices[pair["id"]] = {
                "commonPrice": common_price,
                "preferredPrice": preferred_price,
                "spread": spread,
                "spreadChange": spread_change,
                "commonChange": round_or_none(
                    parse_float(
                        common_quote.get("fluctuationsRatioRaw")
                        or common_quote.get("fluctuationsRatio")
                    ),
                ),
                "preferredChange": round_or_none(
                    parse_float(
                        preferred_quote.get("fluctuationsRatioRaw")
                        or preferred_quote.get("fluctuationsRatio")
                    ),
                ),
            }
            traded_at_values.extend(
                [
                    common_quote.get("localTradedAt"),
                    preferred_quote.get("localTradedAt"),
                ]
            )
        except (TypeError, ValueError) as exc:
            print(f"  WARNING: {pair['name']} 현재가 파싱 실패: {exc}")
            continue

    if market_quote and market_quote.get("localTradedAt"):
        traded_at_values.append(market_quote.get("localTradedAt"))

    valid_times = [datetime.fromisoformat(value) for value in traded_at_values if value]
    last_updated = (
        max(valid_times).astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")
        if valid_times
        else datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    )
    market_summary = build_market_summary(market_quote)
    summary = build_summary(prices, market_summary)
    avg_spread = summary["averageSpread"] if summary else None
    avg_spread_change = summary["averageSpreadChange"] if summary else None

    result = {
        "source": "네이버 증권",
        "lastUpdated": last_updated,
        "prices": prices,
        "market": market_summary,
        "averageSpread": avg_spread,
        "averageSpreadChange": avg_spread_change,
        "summary": summary,
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(
        "current.json 갱신 완료 "
        f"({len(prices)}개 종목, 평균 괴리율 {avg_spread}%, 전일비 {avg_spread_change}%p)"
    )


if __name__ == "__main__":
    main()
