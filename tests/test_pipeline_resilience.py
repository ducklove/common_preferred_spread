"""한 종목의 장애가 전체 파이프라인을 멈추지 않게 하는 회복성 로직 테스트.

2026-08 한화(000880) 거래정지로 일일 갱신이 2주 넘게 전면 중단된 사고의 회귀 방지:
- incremental_start: 뒤처진 종목이 영원히 따라잡지 못하는 자기강화 결함
- carry_forward_missing_pairs: 결과가 빈 종목이 품질 가드를 터뜨려 전체 중단
"""

from datetime import datetime

import pytest

from fetch_data import (
    carry_forward_missing_pairs,
    find_stale_pair_warnings,
    get_pair_last_dates,
    incremental_start,
)
from history_rules import find_quality_violations


def make_history(dates):
    return [
        {"date": date, "commonPrice": 1000, "preferredPrice": 800, "spread": 20.0}
        for date in dates
    ]


def make_pair(pair_id, dates):
    return {
        "id": pair_id,
        "name": pair_id,
        "current": {"spread": 20.0},
        "history": make_history(dates),
    }


class TestIncrementalStart:
    def test_뒤처진_종목_기준으로_창을_잡는다(self):
        # 최신 종목은 08-20까지, 한화만 07-29에 멈춘 상황
        start = incremental_start(["2026-08-20", "2026-08-20", "2026-07-29"])
        assert start == datetime(2026, 7, 24)

    def test_모두_같은_날짜면_중첩일만_뺀다(self):
        start = incremental_start(["2026-08-20", "2026-08-20"])
        assert start == datetime(2026, 8, 15)

    def test_영구정지_종목이_창을_무한정_끌어내리지_않는다(self):
        start = incremental_start(["2026-08-20", "2016-01-04"], max_lookback_days=90)
        assert start == datetime(2026, 5, 22)  # 최신일 - 90일 하한

    def test_뒤처진_종목이_다음_실행에서_따라잡을_수_있다(self):
        # 예전 max 기준 로직은 start(08-15) > 뒤처진 종목 마지막 날짜(07-29)라서
        # 그 종목의 공백이 영구화됐다. min 기준은 그 날짜 이전부터 다시 받는다.
        last_dates = ["2026-08-20", "2026-07-29"]
        start = incremental_start(last_dates)
        assert start < datetime(2026, 7, 29)

    def test_get_pair_last_dates는_평균페어를_제외한다(self):
        existing = {
            "pairs": [
                make_pair("samsung_elec", ["2026-08-19", "2026-08-20"]),
                make_pair("hanwha", ["2026-07-28", "2026-07-29"]),
                {"id": "_average", "isAverage": True, "history": make_history(["2026-08-20"])},
            ]
        }
        assert get_pair_last_dates(existing) == {
            "samsung_elec": "2026-08-20",
            "hanwha": "2026-07-29",
        }


class TestCarryForwardMissingPairs:
    @pytest.fixture
    def previous_data(self):
        return {
            "pairs": [
                make_pair("samsung_elec", ["2026-07-28", "2026-07-29"]),
                make_pair("hanwha", ["2026-07-28", "2026-07-29"]),
                {"id": "_average", "isAverage": True, "history": make_history(["2026-07-29"])},
            ],
            "dividendHistories": {
                "samsung_elec": {"startDate": "2000-01-04"},
                "hanwha": {"startDate": "2016-10-19"},
            },
        }

    def test_결과가_빈_종목은_직전_기록으로_유지된다(self, previous_data):
        pairs_config = [{"id": "samsung_elec", "name": "삼성전자"}, {"id": "hanwha", "name": "한화"}]
        pairs_result = [make_pair("samsung_elec", ["2026-07-29", "2026-08-20"])]
        dividend_histories = {"samsung_elec": {"startDate": "2000-01-04"}}

        carried = carry_forward_missing_pairs(
            pairs_result, pairs_config, previous_data, dividend_histories
        )

        assert carried == {"hanwha"}
        assert {p["id"] for p in pairs_result} == {"samsung_elec", "hanwha"}
        assert dividend_histories["hanwha"] == {"startDate": "2016-10-19"}

    def test_유지된_종목은_품질_가드를_통과한다(self, previous_data):
        """이 조합이 사고의 핵심 — 유지가 없으면 가드가 전체 실행을 중단시켰다."""
        pairs_config = [{"id": "samsung_elec", "name": "삼성전자"}, {"id": "hanwha", "name": "한화"}]
        previous_hist_map = {
            p["id"]: p["history"] for p in previous_data["pairs"] if not p.get("isAverage")
        }
        configured_ids = {p["id"] for p in pairs_config}

        pairs_result = [make_pair("samsung_elec", ["2026-07-28", "2026-07-29", "2026-08-20"])]
        # 유지 전: hanwha가 사라져 가드 위반
        before = find_quality_violations(
            {p["id"]: p["history"] for p in pairs_result}, previous_hist_map, configured_ids
        )
        assert before

        carry_forward_missing_pairs(pairs_result, pairs_config, previous_data, {})

        after = find_quality_violations(
            {p["id"]: p["history"] for p in pairs_result}, previous_hist_map, configured_ids
        )
        assert after == []

    def test_config에서_제거된_종목은_되살리지_않는다(self, previous_data):
        pairs_config = [{"id": "samsung_elec", "name": "삼성전자"}]
        pairs_result = []

        carried = carry_forward_missing_pairs(
            pairs_result, pairs_config, previous_data, {}
        )

        assert "hanwha" not in carried
        assert all(p["id"] != "hanwha" for p in pairs_result)

    def test_직전_데이터가_없으면_유지할_것도_없다(self):
        pairs_config = [{"id": "hanwha", "name": "한화"}]
        pairs_result = []

        assert carry_forward_missing_pairs(pairs_result, pairs_config, None, {}) == set()
        assert pairs_result == []

    def test_평균페어는_유지_대상이_아니다(self, previous_data):
        pairs_config = [{"id": "_average", "name": "괴리율 지수"}]

        assert carry_forward_missing_pairs([], pairs_config, previous_data, {}) == set()


class TestStalePairWarnings:
    def test_전체_최신일보다_많이_뒤처지면_경고한다(self):
        pairs_result = [
            make_pair("samsung_elec", ["2026-08-20"]),
            make_pair("hanwha", ["2026-07-29"]),
        ]

        warnings = find_stale_pair_warnings(pairs_result, warn_days=14)

        assert len(warnings) == 1
        assert "hanwha" in warnings[0]
        assert "22일" in warnings[0]

    def test_경고_임계_이내면_조용하다(self):
        pairs_result = [
            make_pair("samsung_elec", ["2026-08-20"]),
            make_pair("hanwha", ["2026-08-14"]),
        ]

        assert find_stale_pair_warnings(pairs_result, warn_days=14) == []

    def test_결과가_비면_경고도_없다(self):
        assert find_stale_pair_warnings([]) == []
