"""dividend_sources 순수 함수 테스트 (표준 라이브러리 + pytest만 사용)."""

from dividend_sources import choose_dividend_per_share, detect_sheet_layout


class TestDetectSheetLayout:
    # 2026-07 실제 시트 구조: 시총 "N억"이 14열, 보통주 배당 15열~, 우선주 35열~,
    # 헤더의 "2017우" 라벨이 43열 (최신 결산연도 2025 기준 43 + 2017 - 2025 = 35)
    HEADER = [""] * 43 + ["2017우", "2016우"]
    ROW = ["BYC우", "001465", "001460"] + [""] * 10 + ["488", "488억", "400"]

    def test_detects_current_layout(self):
        common_idx, preferred_idx = detect_sheet_layout(self.HEADER, self.ROW, 2025)
        assert (common_idx, preferred_idx) == (15, 35)

    def test_column_insertion_shifts_both(self):
        # 왼쪽에 열이 1개 더 끼면 억 셀과 연도 라벨이 함께 밀린다
        header = [""] * 44 + ["2017우"]
        row = ["BYC우", "001465", "001460"] + [""] * 11 + ["488", "488억", "400"]
        common_idx, preferred_idx = detect_sheet_layout(header, row, 2025)
        assert (common_idx, preferred_idx) == (16, 36)

    def test_new_fiscal_year_shifts_preferred(self):
        # 2026년 결산 열이 두 블록에 하나씩 추가되면 연도 라벨은 2칸 밀린다
        header = [""] * 45 + ["2017우"]
        row = ["BYC우", "001465", "001460"] + [""] * 10 + ["488", "488억", "400"]
        common_idx, preferred_idx = detect_sheet_layout(header, row, 2026)
        assert (common_idx, preferred_idx) == (15, 36)

    def test_fallback_when_anchors_missing(self):
        common_idx, preferred_idx = detect_sheet_layout([], [], 2025)
        assert (common_idx, preferred_idx) == (15, 35)


class TestChooseDividendPerShare:
    def test_stale_split_unadjusted_internal_rejected(self):
        # BYC 사례: 내부 API 3,000원(분할 미반영, 함의 8.7%) vs 공식 1.16% → 시트 400원 채택
        amount, source = choose_dividend_per_share(
            34350, [("internal", 3000.0), ("sheet", 400.0)], 1.16
        )
        assert (amount, source) == (400.0, "sheet")

    def test_stale_previous_year_internal_rejected(self):
        # 대덕 사례: 내부 API 400원(전년도, 함의 2.9%) vs 공식 8.28% → 시트 1,155원 채택
        amount, source = choose_dividend_per_share(
            13950, [("internal", 400.0), ("sheet", 1155.0)], 8.28
        )
        assert (amount, source) == (1155.0, "sheet")

    def test_all_candidates_rejected_derives_from_official(self):
        amount, source = choose_dividend_per_share(
            22650, [("internal", 3050.0), ("sheet", None)], 1.79
        )
        assert source == "naver"
        assert amount == round(22650 * 1.79 / 100, 1)

    def test_valid_internal_kept_first(self):
        # 함의 수익률이 공식값과 맞으면 기존 우선순위(내부 API 우선) 유지
        amount, source = choose_dividend_per_share(
            10000, [("internal", 300.0), ("sheet", 305.0)], 3.05
        )
        assert (amount, source) == (300.0, "internal")

    def test_no_official_keeps_priority_order(self):
        amount, source = choose_dividend_per_share(
            10000, [("internal", None), ("sheet", 500.0)], None
        )
        assert (amount, source) == (500.0, "sheet")

    def test_zero_official_yield_rejects_positive_amounts(self):
        # 공식 0%(무배당)인데 후보가 0.5%p 넘는 배당을 주장하면 0원으로 교정
        amount, source = choose_dividend_per_share(10000, [("internal", 100.0)], 0.0)
        assert (amount, source) == (0.0, "naver")

    def test_zero_amount_accepted_when_official_zero(self):
        amount, source = choose_dividend_per_share(10000, [("sheet", 0.0)], 0.0)
        assert (amount, source) == (0.0, "sheet")

    def test_nothing_available(self):
        assert choose_dividend_per_share(10000, [("internal", None)], None) == (None, None)
