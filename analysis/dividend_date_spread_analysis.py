#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "data.js"
OUTPUT_DIR = ROOT / "analysis" / "outputs"
REPORT_PATH = ROOT / "analysis" / "dividend_date_spread_report.md"

TARGET_MONTH = 12
TARGET_DAY = 27
EVENT_DATE_TOLERANCE_DAYS = 5
MIN_PRE_EVENT_TRADING_DAYS = 60
MAX_REASONABLE_PREF_YIELD = 15.0


def load_stock_data() -> dict:
    content = DATA_PATH.read_text(encoding="utf-8")
    prefix = "const STOCK_DATA = "
    if content.startswith(prefix):
        content = content[len(prefix) :]
    if content.endswith(";\n"):
        content = content[:-2]
    elif content.endswith(";"):
        content = content[:-1]
    return json.loads(content)


def clean_history(pair: dict) -> pd.DataFrame:
    frame = pd.DataFrame(pair["history"]).copy()
    frame["date"] = pd.to_datetime(frame["date"])
    for column in ("commonPrice", "preferredPrice", "spread"):
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    required = ["date", "spread"]
    if not pair.get("isAverage"):
        required.extend(["commonPrice", "preferredPrice"])
    return (
        frame.dropna(subset=required)
        .sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )


def load_average_frame(stock_data: dict) -> pd.DataFrame:
    average_pair = next(pair for pair in stock_data["pairs"] if pair.get("isAverage"))
    return clean_history(average_pair)


def find_nearest_row_position(frame: pd.DataFrame, event_date: pd.Timestamp) -> int | None:
    diffs = (frame["date"] - event_date).abs()
    if diffs.empty:
        return None
    position = int(diffs.idxmin())
    if diffs.iloc[position] > pd.Timedelta(days=EVENT_DATE_TOLERANCE_DAYS):
        return None
    return position


def build_average_window(stock_data: dict) -> pd.DataFrame:
    average = load_average_frame(stock_data)
    rows = []
    offsets = [-60, -40, -20, -10, -5, -1, 0, 1, 5, 10]
    for year in range(int(average["date"].dt.year.min()), int(average["date"].dt.year.max()) + 1):
        target = pd.Timestamp(year=year, month=TARGET_MONTH, day=TARGET_DAY)
        position = find_nearest_row_position(average, target)
        if position is None:
            continue

        row = {
            "year": year,
            "event_date": average.loc[position, "date"],
        }
        for offset in offsets:
            idx = position + offset
            row[f"spread_t{offset}"] = float(average.loc[idx, "spread"]) if 0 <= idx < len(average) else np.nan

        if pd.notna(row.get("spread_t-60")) and pd.notna(row.get("spread_t-1")):
            row["change_t-60_to_t-1"] = row["spread_t-1"] - row["spread_t-60"]
        if pd.notna(row.get("spread_t-20")) and pd.notna(row.get("spread_t-1")):
            row["change_t-20_to_t-1"] = row["spread_t-1"] - row["spread_t-20"]
        rows.append(row)
    return pd.DataFrame(rows)


def dividend_amounts_by_date(rows: list[dict]) -> dict[pd.Timestamp, float]:
    out = {}
    for item in rows:
        date = pd.to_datetime(item.get("date"), errors="coerce")
        amount = pd.to_numeric(item.get("amount"), errors="coerce")
        if pd.isna(date) or pd.isna(amount) or float(amount) <= 0:
            continue
        out[date.normalize()] = float(amount)
    return out


def nearest_common_dividend(
    common_amounts: dict[pd.Timestamp, float],
    event_date: pd.Timestamp,
) -> float:
    if event_date in common_amounts:
        return common_amounts[event_date]
    candidates = sorted(
        (abs((date - event_date).days), amount)
        for date, amount in common_amounts.items()
        if abs((date - event_date).days) <= 7
    )
    return candidates[0][1] if candidates else np.nan


def build_pair_events(stock_data: dict) -> pd.DataFrame:
    dividend_histories = stock_data.get("dividendHistories", {})
    rows = []
    for pair in stock_data["pairs"]:
        if pair.get("isAverage"):
            continue

        frame = clean_history(pair)
        if frame.empty:
            continue

        dividend_item = dividend_histories.get(pair["id"], {})
        common_amounts = dividend_amounts_by_date(dividend_item.get("common", []))

        for event in dividend_item.get("preferred", []):
            event_date = pd.to_datetime(event.get("date"), errors="coerce")
            preferred_dividend = pd.to_numeric(event.get("amount"), errors="coerce")
            if pd.isna(event_date) or pd.isna(preferred_dividend) or float(preferred_dividend) <= 0:
                continue
            event_date = event_date.normalize()
            preferred_dividend = float(preferred_dividend)
            if not (event_date.month == 12 and event_date.day >= 20):
                continue

            position = find_nearest_row_position(frame, event_date)
            if position is None or position < MIN_PRE_EVENT_TRADING_DAYS:
                continue

            r60 = frame.iloc[position - 60]
            r40 = frame.iloc[position - 40]
            r20 = frame.iloc[position - 20]
            r10 = frame.iloc[position - 10]
            r5 = frame.iloc[position - 5]
            r1 = frame.iloc[position - 1]
            r0 = frame.iloc[position]

            common_dividend = nearest_common_dividend(common_amounts, event_date)
            common_60 = float(r60["commonPrice"])
            preferred_60 = float(r60["preferredPrice"])
            common_1 = float(r1["commonPrice"])
            preferred_1 = float(r1["preferredPrice"])
            spread_60 = float(r60["spread"])
            spread_1 = float(r1["spread"])

            endpoint_dividend_removed_spread = np.nan
            preferred_only_removed_spread = np.nan
            if pd.notna(common_dividend) and common_1 > common_dividend and preferred_1 > preferred_dividend:
                endpoint_dividend_removed_spread = (
                    (common_1 - common_dividend) - (preferred_1 - preferred_dividend)
                ) / (common_1 - common_dividend) * 100
            if preferred_1 > preferred_dividend:
                preferred_only_removed_spread = (common_1 - (preferred_1 - preferred_dividend)) / common_1 * 100

            current = pair.get("current", {})
            common_market_cap = current.get("commonMarketCap")
            preferred_market_cap = current.get("preferredMarketCap")
            preferred_market_cap_ratio = (
                preferred_market_cap / common_market_cap * 100
                if common_market_cap and preferred_market_cap
                else np.nan
            )

            rows.append(
                {
                    "pair_id": pair["id"],
                    "pair_name": pair["name"],
                    "common_name": pair["commonName"],
                    "preferred_name": pair["preferredName"],
                    "year": int(event_date.year),
                    "event_date": event_date,
                    "trading_event_date": frame.iloc[position]["date"],
                    "preferred_dividend": preferred_dividend,
                    "common_dividend": common_dividend,
                    "spread_t-60": spread_60,
                    "spread_t-40": float(r40["spread"]),
                    "spread_t-20": float(r20["spread"]),
                    "spread_t-10": float(r10["spread"]),
                    "spread_t-5": float(r5["spread"]),
                    "spread_t-1": spread_1,
                    "spread_t0": float(r0["spread"]),
                    "change_t-60_to_t-1": spread_1 - spread_60,
                    "change_t-20_to_t-1": spread_1 - float(r20["spread"]),
                    "change_t-5_to_t-1": spread_1 - float(r5["spread"]),
                    "ex_date_change_t0_minus_t-1": float(r0["spread"]) - spread_1,
                    "preferred_yield_t-60": preferred_dividend / preferred_60 * 100 if preferred_60 else np.nan,
                    "common_yield_t-60": common_dividend / common_60 * 100
                    if pd.notna(common_dividend) and common_60
                    else np.nan,
                    "yield_gap_t-60": (preferred_dividend / preferred_60 - common_dividend / common_60) * 100
                    if pd.notna(common_dividend) and preferred_60 and common_60
                    else np.nan,
                    "endpoint_dividend_removed_change": endpoint_dividend_removed_spread - spread_60
                    if pd.notna(endpoint_dividend_removed_spread)
                    else np.nan,
                    "preferred_only_removed_change": preferred_only_removed_spread - spread_60
                    if pd.notna(preferred_only_removed_spread)
                    else np.nan,
                    "current_common_market_cap": common_market_cap,
                    "current_preferred_market_cap": preferred_market_cap,
                    "current_preferred_market_cap_ratio": preferred_market_cap_ratio,
                    "current_preferred_avg_traded_value_20": current.get("preferredAvgTradedValue20"),
                }
            )
    return pd.DataFrame(rows).sort_values(["year", "pair_id", "event_date"]).reset_index(drop=True)


def filtered_pair_events(events: pd.DataFrame) -> pd.DataFrame:
    return events[
        events["change_t-60_to_t-1"].notna()
        & events["preferred_yield_t-60"].gt(0)
        & events["preferred_yield_t-60"].le(MAX_REASONABLE_PREF_YIELD)
        & events["spread_t-60"].gt(0)
    ].copy()


def summarize_series(series: pd.Series) -> dict:
    clean = series.dropna()
    return {
        "n": int(len(clean)),
        "mean": float(clean.mean()) if len(clean) else np.nan,
        "median": float(clean.median()) if len(clean) else np.nan,
        "pct_negative": float((clean < 0).mean()) if len(clean) else np.nan,
    }


def global_yield_quartiles(events: pd.DataFrame) -> pd.DataFrame:
    out = events.copy()
    out["preferred_yield_quartile"] = pd.qcut(
        out["preferred_yield_t-60"],
        4,
        labels=["Q1 low", "Q2", "Q3", "Q4 high"],
    )
    return (
        out.groupby("preferred_yield_quartile", observed=True)
        .agg(
            n=("change_t-60_to_t-1", "size"),
            preferred_yield_mean=("preferred_yield_t-60", "mean"),
            yield_gap_mean=("yield_gap_t-60", "mean"),
            raw_change_mean=("change_t-60_to_t-1", "mean"),
            raw_change_median=("change_t-60_to_t-1", "median"),
            raw_contraction_rate=("change_t-60_to_t-1", lambda values: (values < 0).mean()),
            dividend_removed_change_mean=("endpoint_dividend_removed_change", "mean"),
            dividend_removed_change_median=("endpoint_dividend_removed_change", "median"),
            preferred_only_removed_change_mean=("preferred_only_removed_change", "mean"),
        )
        .reset_index()
    )


def period_bucket(year: int) -> str:
    if year < 2010:
        return "2000s"
    if year < 2020:
        return "2010s"
    return "2020s"


def period_summary(events: pd.DataFrame) -> pd.DataFrame:
    out = events.copy()
    out["period"] = out["year"].map(period_bucket)
    return (
        out.groupby("period", observed=True)
        .agg(
            n=("change_t-60_to_t-1", "size"),
            pair_count=("pair_id", "nunique"),
            preferred_yield_mean=("preferred_yield_t-60", "mean"),
            yield_gap_mean=("yield_gap_t-60", "mean"),
            raw_change_mean=("change_t-60_to_t-1", "mean"),
            raw_change_median=("change_t-60_to_t-1", "median"),
            raw_contraction_rate=("change_t-60_to_t-1", lambda values: (values < 0).mean()),
            dividend_removed_change_mean=("endpoint_dividend_removed_change", "mean"),
            dividend_removed_change_median=("endpoint_dividend_removed_change", "median"),
            dividend_removed_contraction_rate=("endpoint_dividend_removed_change", lambda values: (values < 0).mean()),
        )
        .reset_index()
    )


def within_year_high_low(events: pd.DataFrame, metric: str) -> pd.DataFrame:
    rows = []
    for year, group in events.dropna(subset=[metric]).groupby("year"):
        if len(group) < 8:
            continue
        low_cut = group[metric].quantile(0.3)
        high_cut = group[metric].quantile(0.7)
        low = group[group[metric] <= low_cut]
        high = group[group[metric] >= high_cut]
        if len(low) < 2 or len(high) < 2:
            continue
        rows.append(
            {
                "year": int(year),
                "period": period_bucket(int(year)),
                "metric": metric,
                "n": int(len(group)),
                "low_n": int(len(low)),
                "high_n": int(len(high)),
                "low_metric_mean": float(low[metric].mean()),
                "high_metric_mean": float(high[metric].mean()),
                "low_raw_change_mean": float(low["change_t-60_to_t-1"].mean()),
                "high_raw_change_mean": float(high["change_t-60_to_t-1"].mean()),
                "raw_high_minus_low": float(
                    high["change_t-60_to_t-1"].mean() - low["change_t-60_to_t-1"].mean()
                ),
                "low_dividend_removed_change_mean": float(low["endpoint_dividend_removed_change"].mean()),
                "high_dividend_removed_change_mean": float(high["endpoint_dividend_removed_change"].mean()),
                "dividend_removed_high_minus_low": float(
                    high["endpoint_dividend_removed_change"].mean()
                    - low["endpoint_dividend_removed_change"].mean()
                ),
                "preferred_only_removed_high_minus_low": float(
                    high["preferred_only_removed_change"].mean() - low["preferred_only_removed_change"].mean()
                ),
            }
        )
    return pd.DataFrame(rows)


def summarize_high_low(high_low: pd.DataFrame) -> pd.DataFrame:
    return (
        high_low.groupby(["metric", "period"], observed=True)
        .agg(
            years=("year", "count"),
            raw_diff_mean=("raw_high_minus_low", "mean"),
            raw_diff_median=("raw_high_minus_low", "median"),
            raw_negative_year_rate=("raw_high_minus_low", lambda values: (values < 0).mean()),
            dividend_removed_diff_mean=("dividend_removed_high_minus_low", "mean"),
            dividend_removed_diff_median=("dividend_removed_high_minus_low", "median"),
            dividend_removed_negative_year_rate=("dividend_removed_high_minus_low", lambda values: (values < 0).mean()),
        )
        .reset_index()
    )


def ols_with_year_fixed_effects(events: pd.DataFrame, y_col: str, x_cols: list[str]) -> pd.DataFrame:
    data = events.dropna(subset=[y_col, *x_cols, "year"]).copy()
    columns = [np.ones(len(data))]
    names = ["const"]
    for column in x_cols:
        columns.append(data[column].to_numpy(dtype=float))
        names.append(column)
    years = sorted(data["year"].unique())
    for year in years[1:]:
        columns.append((data["year"].to_numpy() == year).astype(float))
        names.append(f"year_{year}")

    x_matrix = np.column_stack(columns)
    y = data[y_col].to_numpy(dtype=float)
    beta = np.linalg.lstsq(x_matrix, y, rcond=None)[0]
    residuals = y - x_matrix @ beta
    n_obs, n_params = x_matrix.shape
    sigma2 = float((residuals @ residuals) / (n_obs - n_params))
    covariance = sigma2 * np.linalg.inv(x_matrix.T @ x_matrix)
    se = np.sqrt(np.diag(covariance))

    rows = []
    for name, value, std_error in zip(names, beta, se):
        if name not in x_cols:
            continue
        rows.append(
            {
                "y": y_col,
                "x": name,
                "n": int(n_obs),
                "beta": float(value),
                "se": float(std_error),
                "t": float(value / std_error) if std_error else np.nan,
            }
        )
    return pd.DataFrame(rows)


def fmt_pp(value: float, digits: int = 2) -> str:
    if pd.isna(value):
        return "-"
    return f"{value:.{digits}f}%p"


def fmt_pct(value: float, digits: int = 1) -> str:
    if pd.isna(value):
        return "-"
    return f"{value * 100:.{digits}f}%"


def write_report(
    average_window: pd.DataFrame,
    events: pd.DataFrame,
    filtered: pd.DataFrame,
    quartiles: pd.DataFrame,
    periods: pd.DataFrame,
    high_low: pd.DataFrame,
    high_low_summary: pd.DataFrame,
    regressions: pd.DataFrame,
) -> None:
    average_valid = average_window.dropna(subset=["change_t-60_to_t-1"])
    average_change = summarize_series(average_valid["change_t-60_to_t-1"])
    average_short_change = summarize_series(average_valid["change_t-20_to_t-1"])
    raw_change = summarize_series(filtered["change_t-60_to_t-1"])
    dividend_removed = summarize_series(filtered["endpoint_dividend_removed_change"])

    pref_yield_high_low = high_low[high_low["metric"] == "preferred_yield_t-60"]
    yield_gap_high_low = high_low[high_low["metric"] == "yield_gap_t-60"]

    lines = [
        "# 배당기준일 접근과 보통주-우선주 괴리율",
        "",
        "## 분석 기준",
        "",
        f"- 원천 데이터: `{DATA_PATH.as_posix()}`",
        "- 괴리율: `(보통주 가격 - 우선주 가격) / 보통주 가격 * 100`.",
        "- 전체 괴리율은 `_average` 시계열을 12월 27일에 가장 가까운 거래일 기준으로 정렬했다.",
        "- 종목 단위는 우선주 배당 히스토리 중 12월 20일 이후 배당 이벤트만 사용했다.",
        "- 이벤트 당일은 배당락일 성격이 섞일 수 있어 주된 비교는 `T-60`부터 `T-1`까지로 잡았다.",
        f"- 종목 단위 주 분석 표본은 `T-60` 괴리율이 양수이고, `T-60` 우선주 배당수익률이 0% 초과 {MAX_REASONABLE_PREF_YIELD:.0f}% 이하인 이벤트로 제한했다.",
        "",
        "## 핵심 답",
        "",
        f"- 전체 평균 괴리율은 낮아진다고 보기 어렵다. `_average` 기준 `T-60 -> T-1` 평균 변화는 {fmt_pp(average_change['mean'])}, 중간값은 {fmt_pp(average_change['median'])}였고, 축소된 해는 {int((average_valid['change_t-60_to_t-1'] < 0).sum())}/{average_change['n']}개였다.",
        f"- 종목-연도 표본도 비슷하다. 필터 후 {len(filtered)}건에서 `T-60 -> T-1` 평균 변화는 {fmt_pp(raw_change['mean'])}, 중간값은 {fmt_pp(raw_change['median'])}, 축소 비율은 {fmt_pct(raw_change['pct_negative'])}였다.",
        f"- 다만 같은 해 안에서 보면 배당수익률이 높은 쪽이 낮은 쪽보다 괴리율이 덜 벌어지거나 더 줄어드는 경향이 있다. 우선주 배당수익률 상위 30%와 하위 30%의 연도별 평균 차이는 {fmt_pp(pref_yield_high_low['raw_high_minus_low'].mean())}이고, 21개 연도 중 {(pref_yield_high_low['raw_high_minus_low'] < 0).sum()}개 연도에서 상위 그룹 변화가 더 낮았다.",
        f"- 이 관계는 단순 우선주 배당수익률보다 `우선주 배당수익률 - 보통주 배당수익률`에서 더 뚜렷했다. 배당수익률 격차 상위 30%와 하위 30%의 연도별 평균 차이는 {fmt_pp(yield_gap_high_low['raw_high_minus_low'].mean())}였고, 21개 연도 중 {(yield_gap_high_low['raw_high_minus_low'] < 0).sum()}개 연도에서 상위 그룹이 더 축소/덜 확대됐다.",
        f"- 배당수령액을 제거하면 효과는 약해진다. 양쪽 주식의 예정 배당을 `T-1` 가격에서 제거하면 전체 변화 평균은 {fmt_pp(dividend_removed['mean'])}로 오히려 확대 쪽이 되고, 우선주 배당수익률 상하위 30% 차이도 {fmt_pp(pref_yield_high_low['dividend_removed_high_minus_low'].mean())}로 작아진다.",
        f"- 최근으로 올수록 전체 계절성이 강해진다고 보기는 어렵다. 필터 후 평균 변화는 2000s {fmt_pp(periods.loc[periods['period'].eq('2000s'), 'raw_change_mean'].iloc[0])}, 2010s {fmt_pp(periods.loc[periods['period'].eq('2010s'), 'raw_change_mean'].iloc[0])}, 2020s {fmt_pp(periods.loc[periods['period'].eq('2020s'), 'raw_change_mean'].iloc[0])}였다. 2010년대에는 축소 쪽이었지만 2020년대에는 다시 확대 쪽이다.",
        "",
        "## 전체 평균 괴리율",
        "",
        "| 비교 | 표본 | 평균 변화 | 중간값 | 축소 비율 |",
        "| --- | ---: | ---: | ---: | ---: |",
        f"| `_average` T-60 -> T-1 | {average_change['n']} | {fmt_pp(average_change['mean'])} | {fmt_pp(average_change['median'])} | {fmt_pct(average_change['pct_negative'])} |",
        f"| `_average` T-20 -> T-1 | {average_short_change['n']} | {fmt_pp(average_short_change['mean'])} | {fmt_pp(average_short_change['median'])} | {fmt_pct(average_short_change['pct_negative'])} |",
        f"| 종목-연도 T-60 -> T-1 | {raw_change['n']} | {fmt_pp(raw_change['mean'])} | {fmt_pp(raw_change['median'])} | {fmt_pct(raw_change['pct_negative'])} |",
        "",
        "## 배당수익률 분위",
        "",
        "| 분위 | 표본 | 우선주 배당수익률 | 배당수익률 격차 | T-60 -> T-1 평균 | 중간값 | 배당 제거 후 평균 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for _, row in quartiles.iterrows():
        lines.append(
            f"| {row['preferred_yield_quartile']} | {int(row['n'])} | "
            f"{row['preferred_yield_mean']:.2f}% | {row['yield_gap_mean']:.2f}%p | "
            f"{fmt_pp(row['raw_change_mean'])} | {fmt_pp(row['raw_change_median'])} | "
            f"{fmt_pp(row['dividend_removed_change_mean'])} |"
        )

    lines.extend(
        [
            "",
            "## 시기별 변화",
            "",
            "| 시기 | 표본 | 종목 수 | 원 변화 평균 | 중간값 | 축소 비율 | 배당 제거 후 평균 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for _, row in periods.iterrows():
        lines.append(
            f"| {row['period']} | {int(row['n'])} | {int(row['pair_count'])} | "
            f"{fmt_pp(row['raw_change_mean'])} | {fmt_pp(row['raw_change_median'])} | "
            f"{fmt_pct(row['raw_contraction_rate'])} | {fmt_pp(row['dividend_removed_change_mean'])} |"
        )

    lines.extend(
        [
            "",
            "## 같은 해 안의 고배당-저배당 차이",
            "",
            "| 기준 | 시기 | 연도 수 | 원 변화 상위-하위 | 배당 제거 후 상위-하위 | 상위 변화가 더 낮은 해 비율 |",
            "| --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    metric_names = {
        "preferred_yield_t-60": "우선주 배당수익률",
        "yield_gap_t-60": "우선주-보통주 배당수익률 격차",
    }
    for _, row in high_low_summary.iterrows():
        lines.append(
            f"| {metric_names[row['metric']]} | {row['period']} | {int(row['years'])} | "
            f"{fmt_pp(row['raw_diff_mean'])} | {fmt_pp(row['dividend_removed_diff_mean'])} | "
            f"{fmt_pct(row['raw_negative_year_rate'])} |"
        )

    lines.extend(
        [
            "",
            "## 회귀 체크",
            "",
            "연도 고정효과를 넣은 단순 OLS다. 계수가 음수이면 해당 값이 높을수록 괴리율 변화가 더 낮다.",
            "",
            "| 종속변수 | 설명변수 | 표본 | 계수 | t |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
    )
    for _, row in regressions.iterrows():
        lines.append(
            f"| {row['y']} | {row['x']} | {int(row['n'])} | {row['beta']:.3f} | {row['t']:.2f} |"
        )

    lines.extend(
        [
            "",
            "## 산출물",
            "",
            f"- 전체 평균 이벤트 창: `{(OUTPUT_DIR / 'dividend_date_average_window.csv').as_posix()}`",
            f"- 종목-연도 이벤트: `{(OUTPUT_DIR / 'dividend_date_pair_events.csv').as_posix()}`",
            f"- 배당수익률 분위: `{(OUTPUT_DIR / 'dividend_date_yield_quartiles.csv').as_posix()}`",
            f"- 시기별 요약: `{(OUTPUT_DIR / 'dividend_date_period_summary.csv').as_posix()}`",
            f"- 같은 해 고배당-저배당 비교: `{(OUTPUT_DIR / 'dividend_date_high_low_by_year.csv').as_posix()}`",
        ]
    )
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    stock_data = load_stock_data()
    average_window = build_average_window(stock_data)
    events = build_pair_events(stock_data)
    filtered = filtered_pair_events(events)

    quartiles = global_yield_quartiles(filtered)
    periods = period_summary(filtered)
    high_low = pd.concat(
        [
            within_year_high_low(filtered, "preferred_yield_t-60"),
            within_year_high_low(filtered, "yield_gap_t-60"),
        ],
        ignore_index=True,
    )
    high_low_summary = summarize_high_low(high_low)

    regressions = pd.concat(
        [
            ols_with_year_fixed_effects(filtered, "change_t-60_to_t-1", ["preferred_yield_t-60"]),
            ols_with_year_fixed_effects(filtered, "change_t-60_to_t-1", ["yield_gap_t-60"]),
            ols_with_year_fixed_effects(filtered, "change_t-60_to_t-1", ["preferred_yield_t-60", "spread_t-60"]),
            ols_with_year_fixed_effects(filtered, "change_t-60_to_t-1", ["yield_gap_t-60", "spread_t-60"]),
            ols_with_year_fixed_effects(filtered, "endpoint_dividend_removed_change", ["preferred_yield_t-60"]),
            ols_with_year_fixed_effects(filtered, "endpoint_dividend_removed_change", ["yield_gap_t-60"]),
        ],
        ignore_index=True,
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    average_window.to_csv(OUTPUT_DIR / "dividend_date_average_window.csv", index=False, encoding="utf-8-sig")
    events.to_csv(OUTPUT_DIR / "dividend_date_pair_events.csv", index=False, encoding="utf-8-sig")
    filtered.to_csv(OUTPUT_DIR / "dividend_date_pair_events_filtered.csv", index=False, encoding="utf-8-sig")
    quartiles.to_csv(OUTPUT_DIR / "dividend_date_yield_quartiles.csv", index=False, encoding="utf-8-sig")
    periods.to_csv(OUTPUT_DIR / "dividend_date_period_summary.csv", index=False, encoding="utf-8-sig")
    high_low.to_csv(OUTPUT_DIR / "dividend_date_high_low_by_year.csv", index=False, encoding="utf-8-sig")
    high_low_summary.to_csv(OUTPUT_DIR / "dividend_date_high_low_summary.csv", index=False, encoding="utf-8-sig")
    regressions.to_csv(OUTPUT_DIR / "dividend_date_regressions.csv", index=False, encoding="utf-8-sig")

    write_report(
        average_window,
        events,
        filtered,
        quartiles,
        periods,
        high_low,
        high_low_summary,
        regressions,
    )

    print(f"report: {REPORT_PATH}")
    print(f"average years: {average_window['change_t-60_to_t-1'].notna().sum()}")
    print(f"pair events: {len(events)}")
    print(f"filtered events: {len(filtered)}")


if __name__ == "__main__":
    main()
