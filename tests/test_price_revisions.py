import pandas as pd
import pytest

from price_revisions import has_price_revision, require_complete_refresh, without_dates


def test_split_revision_uses_same_date_even_across_trading_halt():
    history = [{"date": "2026-08-13", "commonPrice": 1000}]
    series = pd.Series([2000, 1874], index=pd.to_datetime(["2026-08-13", "2026-09-10"]))
    assert has_price_revision(history, {"commonPrice": series})
    # 같은 날 값이 같으면 이후의 실제 주가 변동만으로 수정주가라 추정하지 않는다.
    series.iloc[0] = 1000
    assert not has_price_revision(history, {"commonPrice": series})


def test_rounding_and_missing_quotes_are_not_revisions():
    history = [{"date": "2026-08-13", "commonPrice": 1000}]
    series = pd.Series([1000.49], index=pd.to_datetime(["2026-08-13"]))
    assert not has_price_revision(history, {"commonPrice": series})
    series.iloc[0] = float("nan")
    assert not has_price_revision(history, {"commonPrice": series})


def test_partial_refresh_cannot_preserve_unadjusted_older_prices():
    old = [{"date": "2026-08-12"}, {"date": "2026-08-13"}]
    with pytest.raises(ValueError, match="재수집 누락"):
        require_complete_refresh(old[1:], old, "daekyo")
    require_complete_refresh(old, old, "daekyo")


def _daily_rows(count):
    return [{"date": f"2026-08-{day:02d}"} for day in range(1, count + 1)]


def test_dates_absent_from_the_source_are_returned_and_removed_from_old_history():
    old = _daily_rows(10)
    fresh = [row for row in old if row["date"] not in {"2026-08-04", "2026-08-07"}]
    dropped = require_complete_refresh(fresh, old, "pair")
    assert dropped == ["2026-08-04", "2026-08-07"]
    # 병합 전에 지우면 조정 전 가격 행이 재수집 결과에 섞여 남지 않는다.
    assert without_dates(old, dropped) == fresh
    assert require_complete_refresh(old, old, "pair") == []


def test_truncated_or_heavily_missing_refresh_is_still_blocked():
    old = _daily_rows(10)
    with pytest.raises(ValueError, match="재수집 누락"):
        require_complete_refresh(old[1:], old, "head_cut")
    with pytest.raises(ValueError, match="재수집 누락"):
        require_complete_refresh(old[:-1], old, "tail_cut")
    with pytest.raises(ValueError, match="재수집 누락"):
        require_complete_refresh([old[0], *old[5:]], old, "many_gaps")
