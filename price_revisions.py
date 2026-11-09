"""증분 수집의 가격 기준 변경과 불완전한 재수집을 검사한다."""

import math

import pandas as pd

MIN_TOLERATED_SOURCE_GAPS = 3
MAX_SOURCE_GAP_RATIO = 0.005


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
    """재수집이 기존 구간을 덮는지 검사하고, 원천에 없어 빠지는 날짜를 돌려준다.

    원천에 아예 없는 날짜 때문에 전 종목 갱신을 세우지 않되, 앞뒤 구간이 잘렸거나
    누락이 많으면 불완전한 응답으로 보고 막는다. 호출자는 돌려받은 날짜를 기존
    이력에서 지워야 한다. 그대로 병합하면 조정 전 가격이 남아 기준이 섞인다.
    """
    fresh_dates = {row["date"] for row in new_history}
    old_dates = sorted({row["date"] for row in old_history})
    missing = [date for date in old_dates if date not in fresh_dates]
    if not missing:
        return []
    tolerated = max(MIN_TOLERATED_SOURCE_GAPS, int(len(old_dates) * MAX_SOURCE_GAP_RATIO))
    truncated = old_dates[0] in missing or old_dates[-1] in missing
    if truncated or len(missing) > tolerated:
        raise ValueError(
            f"{pair_id}: 가격 기준 변경 후 과거 {len(missing)}일 재수집 누락 "
            f"({missing[0]}부터). 조정 전후 가격을 섞어 저장할 수 없습니다."
        )
    return missing


def without_dates(history, dates):
    excluded = set(dates)
    return [row for row in history if row["date"] not in excluded]
