"""증분 수집의 가격 기준 변경과 불완전한 재수집을 검사한다."""

import math

import pandas as pd


def has_price_revision(history, price_series):
    """같은 거래일의 가격을 비교한다. 당일 급등락을 병합비율로 추정하지 않는다."""
    for row in history:
        day = pd.Timestamp(row["date"])
        for field, series in price_series.items():
            old = row.get(field)
            if old is None or day not in series.index:
                continue
            fresh = series.loc[day]
            if pd.isna(fresh) or not math.isfinite(float(fresh)):
                continue
            # 반올림 및 소스 간 미세한 종가 차이로 전체 재수집하지 않는다.
            # 같은 날짜에서 5%를 넘는 가격 기준 변경을 재조회 대상으로 잡는다.
            # 더 작은 정정은 --refresh-pairs로 명시적으로 재조회할 수 있다.
            if abs(float(fresh) - old) > max(1.0, abs(old) * 0.05):
                return True
    return False


def require_complete_refresh(new_history, old_history, pair_id):
    fresh_dates = {row["date"] for row in new_history}
    missing = {row["date"] for row in old_history} - fresh_dates
    if missing:
        raise ValueError(
            f"{pair_id}: 가격 기준 변경 후 과거 {len(missing)}일 재수집 누락 "
            f"({min(missing)}부터). 조정 전후 가격을 섞어 저장할 수 없습니다."
        )
