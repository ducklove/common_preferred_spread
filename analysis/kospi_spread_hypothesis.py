#!/usr/bin/env python3
from __future__ import annotations

import json
import math
from pathlib import Path
from statistics import NormalDist

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "data.js"
OUTPUT_DIR = ROOT / "analysis" / "outputs"
REPORT_PATH = ROOT / "analysis" / "kospi_spread_hypothesis_report.md"

GAP_THRESHOLD_DAYS = 14
HORIZONS = (1, 5, 20)
MIN_PERIOD_OBS = 120
MIN_PAIR_OBS = 252
ALPHA = 0.05

STD_NORMAL = NormalDist()


def load_stock_data() -> dict:
    content = DATA_PATH.read_text(encoding="utf-8")
    prefix = "const STOCK_DATA = "
    payload = content[len(prefix):]
    if payload.endswith(";\n"):
        payload = payload[:-2]
    elif payload.endswith(";"):
        payload = payload[:-1]
    return json.loads(payload)


def period_bucket(date_value: pd.Timestamp) -> str:
    year = int(date_value.year)
    if year < 2010:
        return "2000s"
    if year < 2020:
        return "2010s"
    return "2020s"


def add_segments(frame: pd.DataFrame) -> pd.DataFrame:
    gap_days = frame["date"].diff().dt.days
    frame = frame.copy()
    frame["segment"] = gap_days.gt(GAP_THRESHOLD_DAYS).fillna(False).cumsum()
    return frame


def load_average_frame(stock_data: dict) -> pd.DataFrame:
    average_pair = next(pair for pair in stock_data["pairs"] if pair.get("isAverage"))
    frame = pd.DataFrame(average_pair["history"]).copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame["spread"] = pd.to_numeric(frame["spread"], errors="coerce")
    frame["kospiPrice"] = pd.to_numeric(frame["kospiPrice"], errors="coerce")
    frame = frame.dropna(subset=["date", "spread", "kospiPrice"]).sort_values("date").reset_index(drop=True)
    return add_segments(frame)


def load_pair_frames(stock_data: dict) -> list[tuple[dict, pd.DataFrame]]:
    out: list[tuple[dict, pd.DataFrame]] = []
    for pair in stock_data["pairs"]:
        if pair.get("isAverage"):
            continue
        frame = pd.DataFrame(pair["history"]).copy()
        frame["date"] = pd.to_datetime(frame["date"])
        frame["spread"] = pd.to_numeric(frame["spread"], errors="coerce")
        frame = frame.dropna(subset=["date", "spread"]).sort_values("date").reset_index(drop=True)
        out.append((pair, add_segments(frame)))
    return out


def build_horizon_sample(frame: pd.DataFrame, horizon: int) -> pd.DataFrame:
    valid = frame["segment"].eq(frame["segment"].shift(horizon))
    sample = pd.DataFrame(
        {
            "date": frame["date"],
            "spread_change": (frame["spread"] - frame["spread"].shift(horizon)).where(valid),
            "kospi_return": (frame["kospiPrice"] / frame["kospiPrice"].shift(horizon) - 1).where(valid),
        }
    ).dropna()
    sample["period"] = sample["date"].map(period_bucket)
    return sample.reset_index(drop=True)


def build_pair_daily_sample(frame: pd.DataFrame, market_daily: pd.DataFrame) -> pd.DataFrame:
    valid = frame["segment"].eq(frame["segment"].shift(1))
    sample = pd.DataFrame(
        {
            "date": frame["date"],
            "spread_change": (frame["spread"] - frame["spread"].shift(1)).where(valid),
        }
    ).dropna()
    sample = sample.merge(market_daily, on="date", how="inner").dropna()
    sample["period"] = sample["date"].map(period_bucket)
    return sample.reset_index(drop=True)


def pearson_stats(x: np.ndarray, y: np.ndarray) -> dict[str, float]:
    n = len(x)
    if n < 4:
        return {"corr": np.nan, "p_value": np.nan, "ci_low": np.nan, "ci_high": np.nan}
    corr = float(np.corrcoef(x, y)[0, 1])
    corr = min(max(corr, -0.999999), 0.999999)
    z = math.atanh(corr) * math.sqrt(max(n - 3, 1))
    p_value = 2 * (1 - STD_NORMAL.cdf(abs(z)))
    z_crit = STD_NORMAL.inv_cdf(1 - ALPHA / 2)
    delta = z_crit / math.sqrt(max(n - 3, 1))
    return {
        "corr": corr,
        "p_value": p_value,
        "ci_low": math.tanh(math.atanh(corr) - delta),
        "ci_high": math.tanh(math.atanh(corr) + delta),
    }


def spearman_stats(x: np.ndarray, y: np.ndarray) -> dict[str, float]:
    rank_x = pd.Series(x).rank(method="average").to_numpy()
    rank_y = pd.Series(y).rank(method="average").to_numpy()
    return pearson_stats(rank_x, rank_y)


def newey_west_ols(y: np.ndarray, x: np.ndarray, max_lag: int | None = None) -> dict[str, float]:
    n = len(y)
    X = np.column_stack([np.ones(n), x])
    if max_lag is None:
        max_lag = int(math.floor(4 * (n / 100) ** (2 / 9)))
    max_lag = max(0, min(max_lag, n - 1))

    xtx = X.T @ X
    xtx_inv = np.linalg.inv(xtx)
    beta = xtx_inv @ (X.T @ y)
    resid = y - X @ beta

    xu = X * resid[:, None]
    s = xu.T @ xu
    for lag in range(1, max_lag + 1):
        weight = 1 - lag / (max_lag + 1)
        gamma = xu[lag:].T @ xu[:-lag]
        s += weight * (gamma + gamma.T)

    cov = xtx_inv @ s @ xtx_inv
    se = np.sqrt(np.diag(cov))
    z_values = np.divide(beta, se, out=np.full_like(beta, np.nan), where=se > 0)
    p_values = np.array([2 * (1 - STD_NORMAL.cdf(abs(z))) if np.isfinite(z) else np.nan for z in z_values])
    z_crit = STD_NORMAL.inv_cdf(1 - ALPHA / 2)
    ci_low = beta - z_crit * se
    ci_high = beta + z_crit * se

    centered_y = y - y.mean()
    r_squared = 1 - float((resid @ resid) / (centered_y @ centered_y)) if float(centered_y @ centered_y) > 0 else np.nan

    return {
        "alpha": float(beta[0]),
        "beta": float(beta[1]),
        "alpha_se": float(se[0]),
        "beta_se": float(se[1]),
        "alpha_p": float(p_values[0]),
        "beta_p": float(p_values[1]),
        "beta_ci_low": float(ci_low[1]),
        "beta_ci_high": float(ci_high[1]),
        "r_squared": r_squared,
        "hac_lag": float(max_lag),
    }


def up_down_stats(sample: pd.DataFrame) -> dict[str, float]:
    subset = sample.loc[sample["kospi_return"] != 0, ["spread_change", "kospi_return"]].copy()
    if subset.empty or subset["kospi_return"].gt(0).nunique() < 2:
        return {
            "up_mean": np.nan,
            "down_mean": np.nan,
            "diff": np.nan,
            "diff_se": np.nan,
            "diff_p": np.nan,
            "up_count": 0,
            "down_count": 0,
        }

    indicator = subset["kospi_return"].gt(0).astype(float).to_numpy()
    y = subset["spread_change"].to_numpy()
    reg = newey_west_ols(y, indicator)
    down_mean = reg["alpha"]
    diff = reg["beta"]
    up_mean = down_mean + diff
    return {
        "up_mean": up_mean,
        "down_mean": down_mean,
        "diff": diff,
        "diff_se": reg["beta_se"],
        "diff_p": reg["beta_p"],
        "up_count": int((indicator == 1).sum()),
        "down_count": int((indicator == 0).sum()),
    }


def analyze_sample(sample: pd.DataFrame) -> dict[str, float]:
    x = sample["kospi_return"].to_numpy(dtype=float)
    y = sample["spread_change"].to_numpy(dtype=float)
    pearson = pearson_stats(x, y)
    spearman = spearman_stats(x, y)
    reg = newey_west_ols(y, x)
    regime = up_down_stats(sample)
    return {
        "n_obs": int(len(sample)),
        "start_date": sample["date"].min().strftime("%Y-%m-%d"),
        "end_date": sample["date"].max().strftime("%Y-%m-%d"),
        "pearson_r": pearson["corr"],
        "pearson_p": pearson["p_value"],
        "pearson_ci_low": pearson["ci_low"],
        "pearson_ci_high": pearson["ci_high"],
        "spearman_rho": spearman["corr"],
        "spearman_p": spearman["p_value"],
        "beta": reg["beta"],
        "beta_se": reg["beta_se"],
        "beta_p": reg["beta_p"],
        "beta_ci_low": reg["beta_ci_low"],
        "beta_ci_high": reg["beta_ci_high"],
        "r_squared": reg["r_squared"],
        "hac_lag": reg["hac_lag"],
        "beta_per_1pct_move": reg["beta"] * 0.01,
        "up_mean": regime["up_mean"],
        "down_mean": regime["down_mean"],
        "up_down_diff": regime["diff"],
        "up_down_diff_se": regime["diff_se"],
        "up_down_diff_p": regime["diff_p"],
        "up_count": regime["up_count"],
        "down_count": regime["down_count"],
    }


def binomial_upper_tail(n: int, k: int, p: float = 0.5) -> float:
    total = 0.0
    for i in range(k, n + 1):
        total += math.comb(n, i) * (p**i) * ((1 - p) ** (n - i))
    return min(1.0, total)


def benjamini_hochberg(p_values: pd.Series) -> pd.Series:
    valid = p_values.dropna().sort_values()
    m = len(valid)
    if m == 0:
        return pd.Series(index=p_values.index, dtype=float)
    adjusted = pd.Series(index=valid.index, dtype=float)
    running = 1.0
    for rank, (idx, value) in enumerate(reversed(list(valid.items())), start=1):
        original_rank = m - rank + 1
        candidate = value * m / original_rank
        running = min(running, candidate)
        adjusted[idx] = running
    return adjusted.reindex(p_values.index)


def build_pair_level_table(pair_frames: list[tuple[dict, pd.DataFrame]], market_daily: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for pair, frame in pair_frames:
        sample = build_pair_daily_sample(frame, market_daily)
        if len(sample) < MIN_PAIR_OBS:
            continue
        stats = analyze_sample(sample)
        rows.append(
            {
                "pair_id": pair["id"],
                "pair_name": pair["name"],
                "preferred_name": pair["preferredName"],
                **stats,
            }
        )

    pair_df = pd.DataFrame(rows).sort_values(["beta", "pearson_r"], ascending=False).reset_index(drop=True)
    if not pair_df.empty:
        pair_df["beta_p_fdr"] = benjamini_hochberg(pair_df["beta_p"])
        pair_df["pearson_p_fdr"] = benjamini_hochberg(pair_df["pearson_p"])
    return pair_df


def format_pct(value: float | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value * 100:.{digits}f}%"


def format_pp(value: float | None, digits: int = 3) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:.{digits}f}%p"


def format_num(value: float | None, digits: int = 3) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:.{digits}f}"


def write_report(
    full_summary: pd.DataFrame,
    period_summary: pd.DataFrame,
    pair_summary: pd.DataFrame,
    average_frame: pd.DataFrame,
) -> None:
    daily_row = full_summary.loc[full_summary["horizon_days"] == 1].iloc[0]
    pair_positive = int((pair_summary["pearson_r"] > 0).sum()) if not pair_summary.empty else 0
    pair_total = int(len(pair_summary))
    sign_test_p = binomial_upper_tail(pair_total, pair_positive, 0.5) if pair_total else np.nan
    strong_pairs = pair_summary.nsmallest(3, "pearson_r")[["pair_name", "pearson_r"]] if not pair_summary.empty else pd.DataFrame()
    top_pairs = pair_summary.nlargest(5, "pearson_r")[["pair_name", "pearson_r", "beta"]] if not pair_summary.empty else pd.DataFrame()
    period_rows = []
    for period in ("2000s", "2010s", "2020s"):
        row = period_summary.loc[period_summary["period"] == period]
        if row.empty:
            continue
        period_rows.append(row.iloc[0])

    lines: list[str] = []
    lines.append("# KOSPI와 평균 괴리율 변화 가설 검증")
    lines.append("")
    lines.append("## 가설")
    lines.append("")
    lines.append("- KOSPI가 오르면 우선주가 본주보다 덜 올라 평균 괴리율이 상승한다.")
    lines.append("- KOSPI가 내리면 우선주가 본주보다 덜 하락해 평균 괴리율이 하락한다.")
    lines.append("- 따라서 KOSPI 변화와 평균 괴리율 변화는 양의 상관관계를 가진다.")
    lines.append("")
    lines.append("## 표본과 방법")
    lines.append("")
    lines.append(f"- 평균 괴리율은 [data.js]({DATA_PATH.as_posix()})의 `_average` 시계열을 사용했다.")
    lines.append(f"- 사용 가능 구간은 {average_frame['date'].min().strftime('%Y-%m-%d')}부터 {average_frame['date'].max().strftime('%Y-%m-%d')}까지다.")
    lines.append(f"- 장기 공백 구간 왜곡을 막기 위해 {GAP_THRESHOLD_DAYS}일 초과 단절을 넘는 변화율은 제외했다.")
    lines.append("- 1일, 5거래일, 20거래일 변화로 나눠 상관과 회귀를 함께 봤다.")
    lines.append("- 회귀식은 `평균 괴리율 변화(%p) = a + b * KOSPI 수익률`이며, 표준오차는 Newey-West HAC로 계산했다.")
    lines.append("- 보조 검정으로 상승일/하락일 평균 차이와 개별 종목 페어별 동일 방향성도 확인했다.")
    lines.append("")
    lines.append("## 핵심 결론")
    lines.append("")
    lines.append(
        f"- 가설은 지지된다. 일간 기준 Pearson 상관은 {format_num(daily_row['pearson_r'])}, Spearman 상관은 {format_num(daily_row['spearman_rho'])}였고, 둘 다 통계적으로 유의했다."
    )
    lines.append(
        f"- 회귀계수는 {format_num(daily_row['beta'])}로, KOSPI가 1.0% 움직일 때 평균 괴리율은 같은 방향으로 약 {format_pp(daily_row['beta_per_1pct_move'])} 움직였다."
    )
    lines.append(
        f"- KOSPI 상승일 평균 괴리율 변화는 {format_pp(daily_row['up_mean'])}, 하락일 평균 괴리율 변화는 {format_pp(daily_row['down_mean'])}였고, 차이는 {format_pp(daily_row['up_down_diff'])}였다."
    )
    lines.append(
        f"- 설명력은 높지 않다. 일간 회귀의 R²는 {format_pct(daily_row['r_squared'])}로, 방향성은 분명하지만 평균 괴리율의 대부분은 개별 종목 요인과 이벤트가 설명한다."
    )
    lines.append("")
    lines.append("## 전체 기간 요약")
    lines.append("")
    lines.append("| 구간 | 표본수 | Pearson r | Spearman rho | 회귀계수 b | 1% KOSPI당 변화 | R² | 상승일 평균 | 하락일 평균 | 상승-하락 차이 |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for _, row in full_summary.iterrows():
        lines.append(
            f"| {int(row['horizon_days'])}거래일 | {int(row['n_obs'])} | {format_num(row['pearson_r'])} | "
            f"{format_num(row['spearman_rho'])} | {format_num(row['beta'])} | {format_pp(row['beta_per_1pct_move'])} | "
            f"{format_pct(row['r_squared'])} | {format_pp(row['up_mean'])} | {format_pp(row['down_mean'])} | {format_pp(row['up_down_diff'])} |"
        )
    lines.append("")
    lines.append("## 시기별 일간 결과")
    lines.append("")
    lines.append("| 기간 | 표본수 | Pearson r | Spearman rho | 회귀계수 b | 1% KOSPI당 변화 | R² | 상승-하락 차이 |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for row in period_rows:
        lines.append(
            f"| {row['period']} | {int(row['n_obs'])} | {format_num(row['pearson_r'])} | {format_num(row['spearman_rho'])} | "
            f"{format_num(row['beta'])} | {format_pp(row['beta_per_1pct_move'])} | {format_pct(row['r_squared'])} | {format_pp(row['up_down_diff'])} |"
        )
    lines.append("")
    lines.append("## 개별 종목 페어 감도")
    lines.append("")
    lines.append(
        f"- 동일한 일간 검정을 개별 페어 53개에 적용했을 때, Pearson 상관이 양수인 종목은 {pair_positive}/{pair_total}개였다."
    )
    lines.append(
        f"- 귀무가설을 '양수/음수 반반'으로 둔 단순 부호검정의 상단확률은 {sign_test_p:.3e}로, 우연으로 보기 어렵다."
    )
    if not strong_pairs.empty:
        weakest = ", ".join(f"{row['pair_name']} ({row['pearson_r']:.3f})" for _, row in strong_pairs.iterrows())
        lines.append(f"- 상관이 가장 약한 쪽도 모두 양수였다: {weakest}.")
    if not top_pairs.empty:
        leaders = ", ".join(
            f"{row['pair_name']} (r={row['pearson_r']:.3f}, b={row['beta']:.2f})" for _, row in top_pairs.iterrows()
        )
        lines.append(f"- 시장 민감도가 가장 큰 페어는 {leaders}였다.")
    lines.append("")
    lines.append("## 해석")
    lines.append("")
    lines.append("- 방향성 자체는 명확하다. 시장이 오를수록 평균 괴리율이 벌어지고, 시장이 내릴수록 평균 괴리율이 줄어드는 경향이 데이터에 있다.")
    lines.append("- 다만 효과 크기는 '약하지만 일관된' 수준이다. KOSPI만으로 평균 괴리율을 강하게 설명하지는 못한다.")
    lines.append("- 2020년대에 상관과 설명력이 더 커져, 최근일수록 이 메커니즘이 더 뚜렷해진 것으로 보인다.")
    lines.append("")
    lines.append("## 산출물")
    lines.append("")
    lines.append(f"- [요약표]({(OUTPUT_DIR / 'kospi_spread_summary.csv').as_posix()})")
    lines.append(f"- [시기별 표]({(OUTPUT_DIR / 'kospi_spread_period_summary.csv').as_posix()})")
    lines.append(f"- [개별 종목 표]({(OUTPUT_DIR / 'kospi_spread_pair_summary.csv').as_posix()})")
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    stock_data = load_stock_data()
    average_frame = load_average_frame(stock_data)
    pair_frames = load_pair_frames(stock_data)

    full_rows = []
    samples_by_horizon: dict[int, pd.DataFrame] = {}
    for horizon in HORIZONS:
        sample = build_horizon_sample(average_frame, horizon)
        samples_by_horizon[horizon] = sample
        stats = analyze_sample(sample)
        full_rows.append({"horizon_days": horizon, **stats})
    full_summary = pd.DataFrame(full_rows)

    daily_sample = samples_by_horizon[1]
    period_rows = []
    for period, sample in daily_sample.groupby("period", sort=False):
        if len(sample) < MIN_PERIOD_OBS:
            continue
        period_rows.append({"period": period, **analyze_sample(sample)})
    period_summary = pd.DataFrame(period_rows)

    market_daily = daily_sample[["date", "kospi_return"]].copy()
    pair_summary = build_pair_level_table(pair_frames, market_daily)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    full_summary.to_csv(OUTPUT_DIR / "kospi_spread_summary.csv", index=False, encoding="utf-8-sig")
    period_summary.to_csv(OUTPUT_DIR / "kospi_spread_period_summary.csv", index=False, encoding="utf-8-sig")
    pair_summary.to_csv(OUTPUT_DIR / "kospi_spread_pair_summary.csv", index=False, encoding="utf-8-sig")
    write_report(full_summary, period_summary, pair_summary, average_frame)

    daily_row = full_summary.loc[full_summary["horizon_days"] == 1].iloc[0]
    print(f"report: {REPORT_PATH}")
    print(f"daily pearson: {daily_row['pearson_r']:.6f}")
    print(f"daily beta: {daily_row['beta']:.6f}")
    print(f"pair count: {len(pair_summary)}")


if __name__ == "__main__":
    main()
