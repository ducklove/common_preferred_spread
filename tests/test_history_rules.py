"""history_rules 순수 함수 테스트 (표준 라이브러리 + pytest만 사용)."""

from history_rules import (
    find_quality_violations,
    merge_history_by_date,
    merge_preserved_history,
)


def make_history(dates, price=1000):
    return [
        {
            "date": date,
            "commonPrice": price,
            "preferredPrice": price - 200,
            "spread": 20.0,
        }
        for date in dates
    ]


# ---------------------------------------------------------------------------
# merge_preserved_history
# ---------------------------------------------------------------------------


class TestMergePreservedHistory:
    def test_previous_earlier_is_prepended(self):
        previous = make_history(["2020-01-02", "2020-01-03", "2020-01-06"], price=900)
        new = make_history(["2020-01-06", "2020-01-07"])

        prepended, merged = merge_preserved_history(new, previous)

        assert prepended == previous[:2]
        assert len(merged) == 4
        assert [h["date"] for h in merged] == [
            "2020-01-02",
            "2020-01-03",
            "2020-01-06",
            "2020-01-07",
        ]
        # 보존 구간이 앞에, 새 히스토리가 그대로 뒤에 온다
        assert merged[:2] == previous[:2]
        assert merged[2:] == new

    def test_boundary_same_date_not_duplicated(self):
        # 이전 히스토리의 첫 날짜 == 새 히스토리의 첫 날짜 → 보존 구간 없음
        previous = make_history(["2020-01-06", "2020-01-07"], price=900)
        new = make_history(["2020-01-06", "2020-01-07", "2020-01-08"])

        prepended, merged = merge_preserved_history(new, previous)

        assert prepended == []
        assert merged == new
        dates = [h["date"] for h in merged]
        assert len(dates) == len(set(dates))

    def test_previous_later_no_change(self):
        previous = make_history(["2020-02-03", "2020-02-04"], price=900)
        new = make_history(["2020-01-06", "2020-01-07"])

        prepended, merged = merge_preserved_history(new, previous)

        assert prepended == []
        assert merged == new

    def test_empty_previous_no_change(self):
        new = make_history(["2020-01-06", "2020-01-07"])

        assert merge_preserved_history(new, []) == ([], new)
        assert merge_preserved_history(new, None) == ([], new)

    def test_empty_new_history_no_change(self):
        previous = make_history(["2020-01-02", "2020-01-03"])

        prepended, merged = merge_preserved_history([], previous)

        assert prepended == []
        assert merged == []

    def test_partial_overlap_keeps_only_earlier_entries(self):
        # 이전 데이터가 새 시작일 이후 구간도 갖고 있어도 이른 구간만 보존된다
        previous = make_history(
            ["2019-12-30", "2020-01-02", "2020-01-06", "2020-01-07"], price=900
        )
        new = make_history(["2020-01-06", "2020-01-08"])

        prepended, merged = merge_preserved_history(new, previous)

        assert [h["date"] for h in prepended] == ["2019-12-30", "2020-01-02"]
        assert [h["date"] for h in merged] == [
            "2019-12-30",
            "2020-01-02",
            "2020-01-06",
            "2020-01-08",
        ]


# ---------------------------------------------------------------------------
# merge_history_by_date
# ---------------------------------------------------------------------------


class TestMergeHistoryByDate:
    def test_same_date_new_wins_missing_dates_preserved(self):
        old = make_history(
            ["2020-01-02", "2020-01-03", "2020-01-06", "2020-01-07"], price=900
        )
        new = make_history(["2020-01-06", "2020-01-08"])

        merged = merge_history_by_date(new, old)

        assert [h["date"] for h in merged] == [
            "2020-01-02",
            "2020-01-03",
            "2020-01-06",
            "2020-01-07",
            "2020-01-08",
        ]
        # 겹치는 1/6은 새 값, 새 데이터에 없는 1/7은 기존 값 보존
        assert merged[2]["commonPrice"] == 1000
        assert merged[3]["commonPrice"] == 900

    def test_empty_inputs(self):
        history = make_history(["2020-01-06", "2020-01-07"])

        assert merge_history_by_date(history, []) == history
        assert merge_history_by_date(history, None) == history
        assert merge_history_by_date([], history) == history
        assert merge_history_by_date(None, history) == history
        assert merge_history_by_date([], []) == []

    def test_no_duplicate_dates_after_merge(self):
        old = make_history(["2020-01-02", "2020-01-03", "2020-01-06"], price=900)
        new = make_history(["2020-01-03", "2020-01-06", "2020-01-07"])

        merged = merge_history_by_date(new, old)

        dates = [h["date"] for h in merged]
        assert len(dates) == len(set(dates))
        assert dates == sorted(dates)

    def test_shrunken_refetch_passes_quality_guard(self):
        # 2026-06 아모레G3우B 사고 재현: Yahoo가 과거 구간을 잃어 재수집(백필)이
        # 기존보다 성긴 시리즈를 반환해도, 병합 결과는 기존 구간을 모두 보존해
        # 품질 가드를 통과해야 한다 (파괴적 병합이었다면 포인트 감소로 exit 1).
        previous = {"pair1": make_history(dates_from(100), price=900)}
        sparse_refetch = make_history(dates_from(100)[::2])

        merged = merge_history_by_date(sparse_refetch, previous["pair1"])

        assert len(merged) == 100
        assert find_quality_violations({"pair1": merged}, previous, {"pair1"}) == []


# ---------------------------------------------------------------------------
# find_quality_violations
# ---------------------------------------------------------------------------


def dates_from(count, prefix="2020"):
    # 검사 로직은 첫 날짜와 길이만 보므로 합성 날짜면 충분하다
    return [f"{prefix}-{(i // 28) + 1:02d}-{(i % 28) + 1:02d}" for i in range(count)]


class TestFindQualityViolations:
    def test_start_date_regression_detected(self):
        previous = {"pair1": make_history(["2020-01-02", "2020-01-03", "2020-01-06"])}
        result = {"pair1": make_history(["2020-01-03", "2020-01-06"])}

        violations = find_quality_violations(result, previous, {"pair1"})

        assert violations == ["pair1: 시작일 후퇴 2020-01-02 -> 2020-01-03"]

    def test_point_drop_exactly_at_threshold_passes(self):
        # 이전 100개 → 허용 감소 max(20, int(100*0.02)) = 20 → 80개까지 허용
        previous = {"pair1": make_history(dates_from(100))}
        result = {"pair1": make_history(dates_from(80))}

        assert find_quality_violations(result, previous, {"pair1"}) == []

    def test_point_drop_one_below_threshold_detected(self):
        previous = {"pair1": make_history(dates_from(100))}
        result = {"pair1": make_history(dates_from(79))}

        violations = find_quality_violations(result, previous, {"pair1"})

        assert violations == ["pair1: 히스토리 포인트 감소 100 -> 79"]

    def test_point_drop_ratio_threshold_for_large_history(self):
        # 이전 2000개 → 허용 감소 max(20, int(2000*0.02)) = 40
        previous = {"pair1": make_history(dates_from(2000, prefix="201X"))}

        ok = {"pair1": make_history(dates_from(1960, prefix="201X"))}
        assert find_quality_violations(ok, previous, {"pair1"}) == []

        bad = {"pair1": make_history(dates_from(1959, prefix="201X"))}
        assert find_quality_violations(bad, previous, {"pair1"}) == [
            "pair1: 히스토리 포인트 감소 2000 -> 1959"
        ]

    def test_missing_pair_detected(self):
        previous = {
            "gone": make_history(["2020-01-02", "2020-01-03"]),
            "empty": make_history(["2020-01-02", "2020-01-03"]),
        }
        result = {"empty": []}

        violations = find_quality_violations(result, previous, {"gone", "empty"})

        assert violations == [
            "gone: 결과에서 사라졌거나 히스토리가 비어 있음",
            "empty: 결과에서 사라졌거나 히스토리가 비어 있음",
        ]

    def test_pair_removed_from_config_is_ignored(self):
        previous = {"removed": make_history(["2020-01-02", "2020-01-03"])}
        result = {}

        assert find_quality_violations(result, previous, {"other"}) == []

    def test_empty_previous_history_is_ignored(self):
        previous = {"pair1": []}
        result = {}

        assert find_quality_violations(result, previous, {"pair1"}) == []

    def test_normal_growth_passes(self):
        previous = {"pair1": make_history(["2020-01-02", "2020-01-03"])}
        result = {
            "pair1": make_history(["2020-01-02", "2020-01-03", "2020-01-06"]),
        }

        assert find_quality_violations(result, previous, {"pair1"}) == []

    def test_start_date_regression_and_point_drop_both_reported(self):
        previous = {"pair1": make_history(dates_from(100))}
        shrunk = make_history(dates_from(50))
        shrunk[0]["date"] = "2021-01-01"  # 시작일도 후퇴
        result = {"pair1": shrunk}

        violations = find_quality_violations(result, previous, {"pair1"})

        assert len(violations) == 2
        assert violations[0].startswith("pair1: 시작일 후퇴 ")
        assert violations[1] == "pair1: 히스토리 포인트 감소 100 -> 50"
