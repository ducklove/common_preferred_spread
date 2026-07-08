#!/usr/bin/env python3
"""
배당액 소스 선택/검증 규칙 (표준 라이브러리만 사용, fetch_data.py에서 호출)

배경(2026-07 실제 사고 2건):
- 내부 배당 API가 낡은 주당배당금을 반환 — BYC 분할 미반영 3,000원(실제 400원),
  대덕 전년도 400원(실제 1,155원) — 우선순위 1위라 그대로 노출됨
- 구글시트에 열이 추가되며 하드코딩 컬럼 인덱스(14/34)가 어긋나 시트 폴백도 실패

대책:
- detect_sheet_layout: 시트의 배당 블록 시작 컬럼을 앵커로 자동 감지
  (보통주 = "N억" 시총 셀 바로 다음, 우선주 = 헤더의 "YYYY우" 연도 라벨로 역산)
- choose_dividend_per_share: 네이버 종목 페이지의 공식 배당수익률(_dvr)과
  후보 배당액의 함의 수익률을 대조해, 공식값과 안 맞는 낡은 소스를 걸러낸다
"""

import re

SHEET_COMMON_DIVIDEND_FALLBACK_IDX = 15   # 2026-07 시트 기준 보통주 최신 배당 컬럼
SHEET_PREFERRED_DIVIDEND_FALLBACK_IDX = 35  # 〃 우선주 최신 배당 컬럼

# 함의 수익률 vs 공식 수익률 허용 오차: max(절대 0.5%p, 상대 34%)
# — 시세 시점 차·반올림은 통과시키고, 분할 미반영(10x)·전년도 값(수배)은 걸러낸다
OFFICIAL_YIELD_ABS_TOLERANCE_PCT = 0.5
OFFICIAL_YIELD_REL_TOLERANCE = 0.34

_EOK_PATTERN = re.compile(r"억\s*$")
_YEAR_LABEL_PATTERN = re.compile(r"^(\d{4})우$")


def detect_sheet_layout(header_cells, sample_row_cells, latest_fiscal_year):
    """배당 시트에서 (보통주 최신 배당 컬럼, 우선주 최신 배당 컬럼) 인덱스를 감지한다.

    - 보통주: 데이터 행에서 "N억"(시총) 셀 바로 다음 컬럼. 열 삽입에 따라 움직인다.
    - 우선주: 헤더의 "YYYY우" 라벨 위치에서 최신 결산연도까지 역산
      (연도가 1 늘 때마다 컬럼이 1 왼쪽: col(Y) = col(Y0) + Y0 - Y).
    감지 실패 시 현재 알려진 고정 인덱스로 폴백한다.
    """
    common_idx = SHEET_COMMON_DIVIDEND_FALLBACK_IDX
    for i, cell in enumerate(sample_row_cells or []):
        if isinstance(cell, str) and _EOK_PATTERN.search(cell.strip()):
            common_idx = i + 1
            break

    preferred_idx = SHEET_PREFERRED_DIVIDEND_FALLBACK_IDX
    for i, cell in enumerate(header_cells or []):
        if not isinstance(cell, str):
            continue
        match = _YEAR_LABEL_PATTERN.match(cell.strip())
        if not match:
            continue
        candidate = i + int(match.group(1)) - latest_fiscal_year
        if candidate > common_idx:
            preferred_idx = candidate
            break

    return common_idx, preferred_idx


def implied_yield_pct(amount, price):
    if amount is None or not price or price <= 0:
        return None
    return amount / price * 100


def choose_dividend_per_share(price, candidates, official_yield_pct):
    """주당배당금 후보 중 신뢰할 값을 고른다. (amount, source) 반환.

    candidates: [(source_name, amount_or_None), ...] 우선순위순.
    official_yield_pct: 네이버 공식 배당수익률(%) 또는 None.

    공식 수익률이 있으면 함의 수익률이 허용 오차 내인 첫 후보를 채택하고,
    모든 후보가 어긋나면 공식 수익률 역산 금액을 반환한다("naver" 소스).
    공식 수익률이 없으면 첫 non-None 후보를 그대로 쓴다(기존 동작).
    """
    valid = [(source, amount) for source, amount in candidates if amount is not None]

    if official_yield_pct is not None and price and price > 0:
        tolerance = max(
            OFFICIAL_YIELD_ABS_TOLERANCE_PCT,
            official_yield_pct * OFFICIAL_YIELD_REL_TOLERANCE,
        )
        for source, amount in valid:
            implied = implied_yield_pct(amount, price)
            if implied is not None and abs(implied - official_yield_pct) <= tolerance:
                return amount, source
        return round(price * official_yield_pct / 100, 1), "naver"

    if valid:
        source, amount = valid[0]
        return amount, source
    return None, None
