"""attractiveness 점수 모듈 테스트 (표준 라이브러리 + pytest만 사용)."""

from attractiveness import (
    compute_attractiveness,
    recent_annual_dividend_yields,
    score_dividend,
    score_health,
    score_liquidity,
    score_spread,
    score_spread_position,
    spread_percentile_3y,
)


def make_history(entries):
    return [
        {"date": date, "commonPrice": 1000, "preferredPrice": price, "spread": spread}
        for date, price, spread in entries
    ]


class TestScoreSpread:
    def test_anchors(self):
        assert score_spread(0) == 0
        assert score_spread(30) == 10
        assert score_spread(60) == 20
        assert score_spread(90) == 20  # 만점 초과 클램프
        assert score_spread(-10) == 0  # 역전 괴리율
        assert score_spread(None) == 0


class TestSpreadPercentile:
    def test_top_of_distribution(self):
        history = make_history(
            [(f"2026-01-{d:02d}", 800, float(s)) for d, s in zip(range(1, 10), range(10, 90, 10))]
        )
        pct, days = spread_percentile_3y(history)
        assert days == 8
        assert pct > 90  # 마지막 값 80이 최대 → 상위 백분위
        assert score_spread_position(pct) > 18

    def test_window_limited_to_3_years(self):
        old = [(f"2020-01-{d:02d}", 800, 99.0) for d in range(1, 10)]
        recent = [(f"2026-01-{d:02d}", 800, 10.0 + d) for d in range(1, 10)]
        pct, days = spread_percentile_3y(make_history(old + recent))
        assert days == 9  # 2023년 이후 구간만
        assert pct > 90  # 옛 99% 구간이 창 밖이므로 현재 값이 창 내 최대

    def test_insufficient_history(self):
        assert spread_percentile_3y([]) == (None, 0)
        assert spread_percentile_3y(make_history([("2026-01-02", 800, 20.0)])) == (None, 0)
        assert score_spread_position(None) == 0


class TestScoreLiquidity:
    def test_log_anchors(self):
        assert score_liquidity(1e10, 1e8) == 0  # 시총 100억·거래 1억 = 바닥
        assert score_liquidity(1e12, 1e10) == 20  # 시총 1조·거래 100억 = 만점
        assert score_liquidity(1e11, 1e9) == 10  # 로그 중간점
        assert score_liquidity(None, None) == 0
        assert score_liquidity(-5, 0) == 0


class TestRecentAnnualDividendYields:
    def test_yearly_yield_from_entries(self):
        history = make_history(
            [("2024-06-03", 10000, 20.0), ("2024-12-27", 10000, 20.0),
             ("2025-06-02", 20000, 20.0), ("2025-12-29", 20000, 20.0),
             ("2026-07-03", 20000, 20.0)]
        )
        dividends = [
            {"date": "2024-12-27", "amount": 500.0},
            {"date": "2025-12-29", "amount": 500.0},
        ]
        yields = recent_annual_dividend_yields(dividends, history)
        # 최근 완결 연도(2025)부터: 500/20000=2.5%, 500/10000=5.0%
        assert yields == [2.5, 5.0]

    def test_year_without_dividend_counts_as_zero(self):
        history = make_history(
            [("2024-12-27", 10000, 20.0), ("2025-12-29", 10000, 20.0), ("2026-07-03", 10000, 20.0)]
        )
        yields = recent_annual_dividend_yields([{"date": "2024-03-29", "amount": 300.0}], history)
        assert yields == [0.0, 3.0]

    def test_empty_history(self):
        assert recent_annual_dividend_yields([], []) == []


class TestScoreDividend:
    def test_full_marks(self):
        score, avg = score_dividend(8.0, 5.0, [8.0, 9.0])
        assert score == 20.0  # 현재 8%↑ + 차이 3%p↑ + 5년 평균 8%↑
        assert avg == 8.5

    def test_negative_gap_scores_zero_on_gap(self):
        score, _ = score_dividend(4.0, 6.0, [])
        assert 0 < score < 20.0 / 3  # 현재 수익률 몫만

    def test_missing_everything(self):
        score, avg = score_dividend(None, None, [])
        assert score == 0
        assert avg is None


class TestScoreHealth:
    def test_full_marks(self):
        score, years, positive = score_health(1e13, 40.0, [100.0, 200.0, 300.0], 8.0, 0.5)
        assert score == 20.0
        assert (years, positive) == (3, 3)

    def test_loss_years_and_bad_multiples(self):
        score, years, positive = score_health(1e11, 0.0, [-100.0, -50.0, 30.0], -2.0, 5.0)
        # 시총 바닥 0 + 외국인 0 + 흑자 1/3×4 + 적자 PER 0 + PBR 3배↑ 0
        assert round(score, 2) == round(4.0 / 3, 2)
        assert (years, positive) == (3, 1)

    def test_missing_financials(self):
        score, years, positive = score_health(None, None, [], None, None)
        assert score == 0
        assert (years, positive) == (0, 0)


class TestComputeAttractiveness:
    def test_structure_and_total(self):
        history = make_history(
            [(f"2025-{m:02d}-15", 8000, float(20 + m)) for m in range(1, 13)]
            + [("2026-07-03", 8000, 40.0)]
        )
        current = {
            "spread": 40.0,
            "commonDivYield": 2.0,
            "preferredDivYield": 4.0,
            "commonMarketCap": 5e12,
            "preferredMarketCap": 3e11,
            "preferredAvgTradedValue20": 5e8,
        }
        financials = {"per": 10.0, "pbr": 1.0, "foreignRatio": 20.0, "annualNetIncomes": [1e11, 2e11]}
        result = compute_attractiveness(history, current, financials, [])

        assert set(result["scores"]) == {"spread", "spreadPosition", "liquidity", "dividend", "health"}
        assert all(0 <= v <= 20 for v in result["scores"].values())
        assert result["total"] == round(sum(result["scores"].values()), 1)
        assert 0 <= result["total"] <= 100
        assert result["details"]["netIncomeYears"] == 2

    def test_missing_current(self):
        assert compute_attractiveness([], None, {}, []) is None
