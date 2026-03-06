#!/usr/bin/env python3
"""
한국 우선주 괴리율 데이터 수집 스크립트
Yahoo Finance에서 보통주/우선주 가격 데이터를 가져와 data.js를 생성한다.

기본 실행: 기존 data.js의 마지막 날짜 이후만 가져오는 증분 갱신 모드
--full: 2000년부터 전체 데이터를 다시 다운로드
"""

import argparse
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yfinance as yf
import pandas as pd

KST = timezone(timedelta(hours=9))
CONFIG_PATH = Path(__file__).parent / "config.json"
DATA_PATH = Path(__file__).parent / "data.js"

with open(CONFIG_PATH, encoding="utf-8") as f:
    PAIRS = json.load(f)


def load_existing_data():
    """기존 data.js를 읽어 파싱한다. 파일이 없거나 파싱 실패 시 None 반환."""
    if not DATA_PATH.exists():
        return None
    try:
        content = DATA_PATH.read_text(encoding="utf-8")
        prefix = "const STOCK_DATA = "
        if not content.startswith(prefix):
            return None
        json_str = content[len(prefix):]
        if json_str.endswith(";\n"):
            json_str = json_str[:-2]
        elif json_str.endswith(";"):
            json_str = json_str[:-1]
        return json.loads(json_str)
    except (json.JSONDecodeError, ValueError):
        return None


def get_last_date(existing_data):
    """기존 데이터에서 가장 최근 날짜를 찾는다."""
    last_date = None
    for pair in existing_data.get("pairs", []):
        if pair.get("isAverage"):
            continue
        hist = pair.get("history", [])
        if hist:
            pair_last = hist[-1]["date"]
            if last_date is None or pair_last > last_date:
                last_date = pair_last
    return last_date


# 배당수익률 캐시 (동일 보통주 공유 종목의 중복 요청 방지)
_div_yield_cache = {}


def get_div_yield(ticker):
    if ticker not in _div_yield_cache:
        try:
            _div_yield_cache[ticker] = yf.Ticker(ticker).info.get("dividendYield") or 0
        except Exception:
            _div_yield_cache[ticker] = 0
    return _div_yield_cache[ticker]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="전체 데이터 다시 다운로드")
    args = parser.parse_args()

    existing_data = None if args.full else load_existing_data()

    # 모든 티커 수집 (중복 제거) + KOSPI 지수
    KOSPI_TICKER = "^KS11"
    all_tickers = list(
        dict.fromkeys(
            ticker
            for pair in PAIRS
            for ticker in [pair["commonTicker"], pair["preferredTicker"]]
        )
    )
    all_tickers.append(KOSPI_TICKER)

    end_date = datetime.now()

    if existing_data:
        last_date_str = get_last_date(existing_data)
        if last_date_str:
            # 마지막 날짜에서 5일 전부터 가져와서 안전하게 겹침 처리
            start_date = datetime.strptime(last_date_str, "%Y-%m-%d") - timedelta(days=5)
            print(f"증분 갱신 모드: {start_date.strftime('%Y-%m-%d')}부터 가져옵니다")
        else:
            start_date = datetime(2000, 1, 1)
            print("기존 히스토리 없음, 전체 다운로드")
            existing_data = None
    else:
        start_date = datetime(2000, 1, 1)
        print("전체 다운로드 모드")

    print(f"{len(all_tickers)}개 티커 다운로드 중...")
    print(f"기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")

    # 일괄 다운로드 (비조정 종가 사용)
    data = yf.download(
        all_tickers,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        auto_adjust=False,
        progress=True,
    )

    close = data["Close"]
    volume = data["Volume"]

    # 기존 데이터 맵 (증분 모드용)
    existing_pairs_map = {}
    existing_kospi = {}
    if existing_data:
        for p in existing_data["pairs"]:
            if p.get("isAverage"):
                for h in p.get("history", []):
                    if "kospiPrice" in h:
                        existing_kospi[h["date"]] = h["kospiPrice"]
            else:
                existing_pairs_map[p["id"]] = p

    # 각 페어별로 괴리율 계산
    pairs_result = []

    for pair in PAIRS:
        ct = pair["commonTicker"]
        pt = pair["preferredTicker"]

        # 거래정지일(volume=0) 제외
        common_close = close[ct].dropna()
        preferred_close = close[pt].dropna()
        common_vol = volume[ct].fillna(0)
        preferred_vol = volume[pt].fillna(0)

        # 두 시리즈의 공통 날짜만 사용
        common_dates = common_close.index.intersection(preferred_close.index)
        if len(common_dates) == 0:
            print(f"  WARNING: {pair['name']} 겹치는 날짜 없음, 건너뜀")
            continue

        # 양쪽 모두 거래가 있는 날짜만 사용
        traded = (common_vol.loc[common_dates] > 0) & (preferred_vol.loc[common_dates] > 0)
        common_dates = common_dates[traded]
        if len(common_dates) == 0:
            print(f"  WARNING: {pair['name']} 거래일 없음, 건너뜀")
            continue

        c = common_close.loc[common_dates]
        p = preferred_close.loc[common_dates]

        # 괴리율: (보통주 - 우선주) / 보통주 * 100
        spread = (c - p) / c * 100

        # Yahoo Finance 소급 조정 오류 필터 (괴리율 -100% 미만은 불가능한 값)
        valid = spread > -100
        if not valid.all():
            n_removed = (~valid).sum()
            print(f"  WARNING: {pair['name']}: Yahoo 조정 오류 {n_removed}일 제외")
            common_dates = common_dates[valid]
            c = c.loc[common_dates]
            p = p.loc[common_dates]
            spread = spread.loc[common_dates]

        # 새로 다운로드한 히스토리
        new_history = []
        for date in common_dates:
            new_history.append(
                {
                    "date": date.strftime("%Y-%m-%d"),
                    "commonPrice": round(float(c.loc[date]), 0),
                    "preferredPrice": round(float(p.loc[date]), 0),
                    "spread": round(float(spread.loc[date]), 2),
                }
            )

        # 증분 모드: 기존 히스토리와 병합
        if pair["id"] in existing_pairs_map and new_history:
            existing_hist = existing_pairs_map[pair["id"]]["history"]
            first_new_date = new_history[0]["date"]
            kept = [h for h in existing_hist if h["date"] < first_new_date]
            history = kept + new_history
        else:
            history = new_history

        if not history:
            continue

        # 현재 (마지막 거래일) 정보
        latest = history[-1]
        prev = history[-2] if len(history) >= 2 else latest
        spread_change = round(latest["spread"] - prev["spread"], 2)

        # 일간 등락률
        if len(history) >= 2:
            prev_cp = prev["commonPrice"]
            prev_pp = prev["preferredPrice"]
            common_change = round((latest["commonPrice"] - prev_cp) / prev_cp * 100, 2) if prev_cp else 0
            preferred_change = round((latest["preferredPrice"] - prev_pp) / prev_pp * 100, 2) if prev_pp else 0
        else:
            common_change = 0
            preferred_change = 0

        # 배당수익률 조회
        c_dy = get_div_yield(ct)
        p_dy = get_div_yield(pt)

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
                "commonChange": common_change,
                "preferredChange": preferred_change,
                "commonDivYield": round(c_dy, 2),
                "preferredDivYield": round(p_dy, 2),
            },
            "history": history,
        }
        pairs_result.append(pair_data)

        print(
            f"  {pair['name']}: {len(history)}일, "
            f"현재 괴리율 {latest['spread']:.2f}% "
            f"({'↑' if spread_change > 0 else '↓'}{abs(spread_change):.2f}%p) "
            f"배당: {pair_data['current']['commonDivYield']:.1f}%/{pair_data['current']['preferredDivYield']:.1f}%"
        )

    # KOSPI 지수 데이터 준비
    kospi_close = close[KOSPI_TICKER].dropna()

    # 그룹(commonName)당 최고 괴리율 pair만 선택하여 평균 계산
    rep_pairs = {}
    for pair_data in pairs_result:
        cn = pair_data["commonName"]
        if cn not in rep_pairs or pair_data["current"]["spread"] > rep_pairs[cn]["current"]["spread"]:
            rep_pairs[cn] = pair_data
    rep_pairs_list = list(rep_pairs.values())

    # 일별 전체 평균 괴리율 계산
    daily_spreads = defaultdict(list)
    for pair_data in rep_pairs_list:
        for h in pair_data["history"]:
            daily_spreads[h["date"]].append(h["spread"])

    avg_history = []
    n_pairs = len(rep_pairs_list)
    for date in sorted(daily_spreads.keys()):
        spreads = daily_spreads[date]
        # 종목 수가 절반 미만인 날은 휴장일 오류 데이터이므로 제외
        if len(spreads) < n_pairs / 2:
            continue
        # KOSPI: 새 데이터 우선, 없으면 기존 데이터 사용
        ts = pd.Timestamp(date)
        if ts in kospi_close.index:
            kospi_price = round(float(kospi_close.loc[ts]), 2)
        elif date in existing_kospi:
            kospi_price = existing_kospi[date]
        else:
            kospi_price = None
        entry = {
            "date": date,
            "commonPrice": 0,
            "preferredPrice": 0,
            "spread": round(sum(spreads) / len(spreads), 2),
        }
        if kospi_price is not None:
            entry["kospiPrice"] = kospi_price
        avg_history.append(entry)

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
            f"  전체 평균: {len(avg_history)}일, "
            f"현재 괴리율 {latest_avg['spread']:.2f}% "
            f"({'↑' if avg_change > 0 else '↓'}{abs(avg_change):.2f}%p)"
        )

    # 전체 평균도 포함하여 괴리율 높은 순 정렬
    if avg_history:
        pairs_result.append(avg_pair)
    pairs_result.sort(key=lambda p: p["current"]["spread"], reverse=True)

    # data.js 출력
    stock_data = {
        "lastUpdated": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "pairs": pairs_result,
    }

    js_content = "const STOCK_DATA = " + json.dumps(stock_data, ensure_ascii=False, indent=2) + ";\n"

    with open(DATA_PATH, "w", encoding="utf-8") as f:
        f.write(js_content)

    print(f"\n{DATA_PATH} 생성 완료 ({len(pairs_result)}개 종목, {len(js_content)} bytes)")


if __name__ == "__main__":
    main()
