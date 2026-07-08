#!/usr/bin/env python3
"""
우선주 투자매력도 점수 모듈 (표준 라이브러리만 사용, fetch_data.py에서 호출)

5개 축 × 20점 = 총 100점. 각 축의 기준(anchor)은 아래 상수로 고정한다.
값이 없는 하위 지표는 0점 처리하되 details에 null로 남겨 프런트에서 구분한다.

1. 괴리율(20): 현재 괴리율. 0% → 0점, SPREAD_FULL_SCORE_PCT 이상 → 만점 (선형)
2. 괴리율 이격도(20): 최근 3년 괴리율 분포에서 현재 값의 백분위 × 20
   (분포 상단 = 자기 역사 대비 괴리율이 넓은 상태 = 평균회귀 여지)
3. 유동성(20): 우선주 시총(10) + 최근 1개월(20거래일) 평균 거래액(10), 로그 스케일
4. 배당가치(20): 최근 배당 기준 우선주 배당수익률 / 보통주와의 수익률 차 /
   최근 5개년 평균 배당수익률을 1:1:1 (각 20/3점)
5. 본주 건전성(20): 보통주 시총·외국인소진율·연간 순이익 흑자 흐름·PER·PBR 각 4점
   (순이익은 네이버 기업실적분석의 확정 연간 실적 기준, 통상 3~4개년)
"""

import math

SPREAD_FULL_SCORE_PCT = 60.0            # 괴리율 60% 이상 = 20점
SPREAD_POSITION_WINDOW_YEARS = 3

LIQUIDITY_MCAP_LOG_MIN = 10.0           # 우선주 시총 100억(1e10) 이하 = 0점
LIQUIDITY_MCAP_LOG_MAX = 12.0           # 1조(1e12) 이상 = 10점
LIQUIDITY_TRADED_LOG_MIN = 8.0          # 일평균 거래액 1억(1e8) 이하 = 0점
LIQUIDITY_TRADED_LOG_MAX = 10.0         # 100억(1e10) 이상 = 10점

DIVIDEND_YIELD_FULL_PCT = 8.0           # 배당수익률 8% 이상 = 하위지표 만점
DIVIDEND_GAP_FULL_PCT = 3.0             # 보통주 대비 +3%p 이상 = 하위지표 만점
DIVIDEND_HISTORY_YEARS = 5

HEALTH_MCAP_LOG_MIN = 11.0              # 보통주 시총 1000억(1e11) 이하 = 0점
HEALTH_MCAP_LOG_MAX = 13.0              # 10조(1e13) 이상 = 4점
HEALTH_FOREIGN_FULL_PCT = 40.0          # 외국인소진율 40% 이상 = 4점
HEALTH_PER_BEST = 8.0                   # PER 8배 이하 = 4점
HEALTH_PER_WORST = 30.0                 # PER 30배 이상(또는 적자) = 0점
HEALTH_PBR_BEST = 0.5                   # PBR 0.5배 이하 = 4점
HEALTH_PBR_WORST = 3.0                  # PBR 3배 이상 = 0점

AXIS_KEYS = ["spread", "spreadPosition", "liquidity", "dividend", "health"]


def _to_number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if value != value:  # NaN
        return None
    return float(value)


def _clamp01(ratio):
    return max(0.0, min(1.0, ratio))


def _linear_score(value, zero_at, full_at, max_score):
    """[zero_at, full_at] 구간 선형 점수. value가 None이면 0."""
    value = _to_number(value)
    if value is None:
        return 0.0
    return _clamp01((value - zero_at) / (full_at - zero_at)) * max_score


def _log_score(value, log_min, log_max, max_score):
    """양수 value를 log10 스케일로 [log_min, log_max] 구간 선형 점수화."""
    value = _to_number(value)
    if value is None or value <= 0:
        return 0.0
    return _clamp01((math.log10(value) - log_min) / (log_max - log_min)) * max_score


def score_spread(spread):
    return _linear_score(spread, 0.0, SPREAD_FULL_SCORE_PCT, 20.0)


def spread_percentile_3y(history):
    """최근 3년 괴리율 분포에서 마지막 값의 백분위(0~100)와 표본 수를 반환한다.

    3년 미만 히스토리는 가진 구간 전체를 사용한다. 표본 2개 미만이면 (None, 0).
    """
    records = [h for h in (history or []) if _to_number(h.get("spread")) is not None]
    if len(records) < 2:
        return None, 0
    last_date = records[-1]["date"]
    cutoff_year = int(last_date[:4]) - SPREAD_POSITION_WINDOW_YEARS
    cutoff = f"{cutoff_year}{last_date[4:]}"
    window = [float(h["spread"]) for h in records if h["date"] >= cutoff]
    if len(window) < 2:
        window = [float(h["spread"]) for h in records]
    current = window[-1]
    below = sum(1 for v in window if v < current)
    equal = sum(1 for v in window if v == current)
    percentile = (below + 0.5 * equal) / len(window) * 100
    return percentile, len(window)


def score_spread_position(percentile):
    if percentile is None:
        return 0.0
    return _clamp01(percentile / 100.0) * 20.0


def score_liquidity(preferred_market_cap, avg_traded_value):
    mcap_score = _log_score(preferred_market_cap, LIQUIDITY_MCAP_LOG_MIN, LIQUIDITY_MCAP_LOG_MAX, 10.0)
    traded_score = _log_score(avg_traded_value, LIQUIDITY_TRADED_LOG_MIN, LIQUIDITY_TRADED_LOG_MAX, 10.0)
    return mcap_score + traded_score


def recent_annual_dividend_yields(dividend_entries, history, years=DIVIDEND_HISTORY_YEARS):
    """최근 완결 연도부터 최대 years개 연도의 배당수익률(%) 목록을 반환한다.

    연도별 수익률 = 해당 연도 배당금 합계 / 해당 연도 마지막 우선주 종가 × 100.
    히스토리(가격)가 없는 연도는 제외하고, 가격은 있으나 배당 기록이 없는 연도는
    0%로 집계한다 (배당 조사 범위는 히스토리 범위와 동일하게 수집되므로).
    """
    if not history:
        return []
    last_year = int(history[-1]["date"][:4]) - 1
    first_year = int(history[0]["date"][:4])
    amounts_by_year = {}
    for entry in dividend_entries or []:
        amount = _to_number(entry.get("amount"))
        if amount is None:
            continue
        year = int(entry["date"][:4])
        amounts_by_year[year] = amounts_by_year.get(year, 0.0) + amount

    yields = []
    for year in range(last_year, last_year - years, -1):
        if year < first_year:
            break
        year_prices = [
            _to_number(h.get("preferredPrice"))
            for h in history
            if h["date"][:4] == str(year)
        ]
        year_prices = [p for p in year_prices if p]
        if not year_prices:
            continue
        yields.append(amounts_by_year.get(year, 0.0) / year_prices[-1] * 100)
    return yields


def score_dividend(preferred_yield, common_yield, five_year_yields):
    sub_max = 20.0 / 3
    current_score = _linear_score(preferred_yield, 0.0, DIVIDEND_YIELD_FULL_PCT, sub_max)
    gap = None
    preferred_yield_num = _to_number(preferred_yield)
    common_yield_num = _to_number(common_yield)
    if preferred_yield_num is not None and common_yield_num is not None:
        gap = preferred_yield_num - common_yield_num
    gap_score = _linear_score(gap, 0.0, DIVIDEND_GAP_FULL_PCT, sub_max)
    five_year_avg = sum(five_year_yields) / len(five_year_yields) if five_year_yields else None
    history_score = _linear_score(five_year_avg, 0.0, DIVIDEND_YIELD_FULL_PCT, sub_max)
    return current_score + gap_score + history_score, five_year_avg


def score_health(common_market_cap, foreign_ratio, annual_net_incomes, per, pbr):
    mcap_score = _log_score(common_market_cap, HEALTH_MCAP_LOG_MIN, HEALTH_MCAP_LOG_MAX, 4.0)
    foreign_score = _linear_score(foreign_ratio, 0.0, HEALTH_FOREIGN_FULL_PCT, 4.0)

    incomes = [v for v in (annual_net_incomes or []) if _to_number(v) is not None]
    positive_years = sum(1 for v in incomes if v > 0)
    profit_score = (positive_years / len(incomes)) * 4.0 if incomes else 0.0

    per_num = _to_number(per)
    if per_num is None or per_num <= 0:
        per_score = 0.0
    else:
        per_score = _clamp01((HEALTH_PER_WORST - per_num) / (HEALTH_PER_WORST - HEALTH_PER_BEST)) * 4.0

    pbr_num = _to_number(pbr)
    if pbr_num is None or pbr_num <= 0:
        pbr_score = 0.0
    else:
        pbr_score = _clamp01((HEALTH_PBR_WORST - pbr_num) / (HEALTH_PBR_WORST - HEALTH_PBR_BEST)) * 4.0

    score = mcap_score + foreign_score + profit_score + per_score + pbr_score
    return score, len(incomes), positive_years


def compute_attractiveness(history, current, common_financials, preferred_dividends):
    """pair 1개의 투자매력도를 계산한다. current가 없으면 None."""
    if not current:
        return None
    financials = common_financials or {}

    spread_score = score_spread(current.get("spread"))
    percentile, window_days = spread_percentile_3y(history)
    position_score = score_spread_position(percentile)
    liquidity_score = score_liquidity(
        current.get("preferredMarketCap"),
        current.get("preferredAvgTradedValue20"),
    )
    five_year_yields = recent_annual_dividend_yields(preferred_dividends, history)
    dividend_score, five_year_avg = score_dividend(
        current.get("preferredDivYield"),
        current.get("commonDivYield"),
        five_year_yields,
    )
    health_score, income_years, income_positive_years = score_health(
        current.get("commonMarketCap"),
        financials.get("foreignRatio"),
        financials.get("annualNetIncomes"),
        financials.get("per"),
        financials.get("pbr"),
    )

    scores = {
        "spread": round(spread_score, 1),
        "spreadPosition": round(position_score, 1),
        "liquidity": round(liquidity_score, 1),
        "dividend": round(dividend_score, 1),
        "health": round(health_score, 1),
    }
    return {
        "total": round(sum(scores.values()), 1),
        "scores": scores,
        "details": {
            "spreadPct3y": round(percentile, 1) if percentile is not None else None,
            "spreadWindowDays": window_days,
            "divYield5y": round(five_year_avg, 2) if five_year_avg is not None else None,
            "divYield5yYears": len(five_year_yields),
            "per": financials.get("per"),
            "pbr": financials.get("pbr"),
            "foreignRatio": financials.get("foreignRatio"),
            "netIncomeYears": income_years,
            "netIncomePositiveYears": income_positive_years,
        },
    }
