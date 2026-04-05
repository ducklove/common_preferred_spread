#!/usr/bin/env python3
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "data.js"
OUTPUT_DIR = ROOT / "analysis" / "outputs"
REPORT_PATH = ROOT / "analysis" / "hypothesis_event_study_report.md"

LOOKBACK_WINDOWS = (5, 20)
FORWARD_HORIZONS = (5, 20, 60)
PRIMARY_LOOKBACK = 5
PRIMARY_HORIZON = 20
BOOTSTRAP_REPS = 2000
PAIR_BOOTSTRAP_REPS = 3000
MIN_SIGNAL_OBS = 252
MIN_PERIOD_EVENTS = 20
MIN_PERIOD_PAIRS = 5
MIN_PAIR_EVENTS = 8
GAP_THRESHOLD_DAYS = 14
SEED = 20260405


PRIMARY_METRIC = {
    "H1": "fwd_common_return",
    "H2": "fwd_common_return",
    "H3": "fwd_preferred_return",
}

SECONDARY_METRICS = {
    "H1": ["fwd_spread_change", "fwd_relative_return"],
    "H2": ["fwd_spread_change", "fwd_relative_return"],
    "H3": ["fwd_spread_change", "fwd_relative_return"],
}

DIRECTION = {
    ("H1", "fwd_common_return"): "negative",
    ("H1", "fwd_spread_change"): "negative",
    ("H1", "fwd_relative_return"): "negative",
    ("H2", "fwd_common_return"): "positive",
    ("H2", "fwd_spread_change"): "two-sided",
    ("H2", "fwd_relative_return"): "two-sided",
    ("H3", "fwd_preferred_return"): "negative",
    ("H3", "fwd_spread_change"): "positive",
    ("H3", "fwd_relative_return"): "negative",
}


@dataclass
class BootstrapResult:
    mean: float | None
    ci_low: float | None
    ci_high: float | None
    p_value: float | None


def load_stock_data() -> dict:
    content = DATA_PATH.read_text(encoding="utf-8")
    prefix = "const STOCK_DATA = "
    payload = content[len(prefix):]
    if payload.endswith(";\n"):
        payload = payload[:-2]
    elif payload.endswith(";"):
        payload = payload[:-1]
    return json.loads(payload)


def load_pairs() -> list[dict]:
    stock_data = load_stock_data()
    return [pair for pair in stock_data["pairs"] if not pair.get("isAverage")]


def build_pair_frame(pair: dict) -> pd.DataFrame:
    frame = pd.DataFrame(pair["history"]).copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.sort_values("date").reset_index(drop=True)
    frame["commonPrice"] = pd.to_numeric(frame["commonPrice"], errors="coerce")
    frame["preferredPrice"] = pd.to_numeric(frame["preferredPrice"], errors="coerce")
    frame["spread"] = pd.to_numeric(frame["spread"], errors="coerce")

    gap_days = frame["date"].diff().dt.days.fillna(0)
    frame["gap_break"] = gap_days > GAP_THRESHOLD_DAYS
    frame["gap_cumsum"] = frame["gap_break"].cumsum()
    return frame


def add_window_features(frame: pd.DataFrame, lookback: int, horizons: tuple[int, ...]) -> pd.DataFrame:
    df = frame.copy()
    gap_cumsum = df["gap_cumsum"]
    valid_lb = gap_cumsum.eq(gap_cumsum.shift(lookback))

    df[f"lb_common_{lookback}"] = np.where(
        valid_lb,
        df["commonPrice"] / df["commonPrice"].shift(lookback) - 1,
        np.nan,
    )
    df[f"lb_preferred_{lookback}"] = np.where(
        valid_lb,
        df["preferredPrice"] / df["preferredPrice"].shift(lookback) - 1,
        np.nan,
    )
    df[f"lb_spread_change_{lookback}"] = np.where(
        valid_lb,
        df["spread"] - df["spread"].shift(lookback),
        np.nan,
    )
    df[f"lb_return_diff_{lookback}"] = df[f"lb_common_{lookback}"] - df[f"lb_preferred_{lookback}"]
    df[f"lb_return_diff_pref_{lookback}"] = -df[f"lb_return_diff_{lookback}"]
    df[f"lb_abs_spread_change_{lookback}"] = df[f"lb_spread_change_{lookback}"].abs()

    for horizon in horizons:
        valid_fwd = gap_cumsum.shift(-horizon).eq(gap_cumsum)
        df[f"fwd_common_{horizon}"] = np.where(
            valid_fwd,
            df["commonPrice"].shift(-horizon) / df["commonPrice"] - 1,
            np.nan,
        )
        df[f"fwd_preferred_{horizon}"] = np.where(
            valid_fwd,
            df["preferredPrice"].shift(-horizon) / df["preferredPrice"] - 1,
            np.nan,
        )
        df[f"fwd_spread_change_{horizon}"] = np.where(
            valid_fwd,
            df["spread"].shift(-horizon) - df["spread"],
            np.nan,
        )

    return df


def period_bucket(date_value: pd.Timestamp) -> str:
    year = int(date_value.year)
    if year < 2000:
        return "1990s"
    if year < 2010:
        return "2000s"
    if year < 2020:
        return "2010s"
    return "2020s"


def hypothesis_masks(df: pd.DataFrame, lookback: int) -> dict[str, pd.Series]:
    rc = df[f"lb_common_{lookback}"]
    rp = df[f"lb_preferred_{lookback}"]
    ds = df[f"lb_spread_change_{lookback}"]
    diff_cp = df[f"lb_return_diff_{lookback}"]
    diff_pc = df[f"lb_return_diff_pref_{lookback}"]
    abs_ds = df[f"lb_abs_spread_change_{lookback}"]

    signal_sample = pd.DataFrame(
        {
            "rc": rc,
            "rp": rp,
            "ds": ds,
            "diff_cp": diff_cp,
            "diff_pc": diff_pc,
            "abs_ds": abs_ds,
        }
    ).dropna()

    if len(signal_sample) < MIN_SIGNAL_OBS:
        return {}

    rc_hi = signal_sample["rc"].quantile(0.90)
    rp_hi = signal_sample["rp"].quantile(0.90)
    ds_hi = signal_sample["ds"].quantile(0.90)
    ds_lo = signal_sample["ds"].quantile(0.10)
    diff_cp_hi = max(signal_sample["diff_cp"].quantile(0.75), 0.0)
    diff_pc_hi = max(signal_sample["diff_pc"].quantile(0.75), 0.0)
    abs_ds_low = signal_sample["abs_ds"].quantile(0.40)

    return {
        "H1": (rc >= rc_hi) & (ds >= ds_hi) & (diff_cp >= diff_cp_hi),
        "H2": (rc >= rc_hi) & (abs_ds <= abs_ds_low),
        "H3": (rp >= rp_hi) & (ds <= ds_lo) & (diff_pc >= diff_pc_hi),
    }


def select_non_overlapping(indices: list[int], spacing: int) -> list[int]:
    selected: list[int] = []
    last_idx = -10**9
    for idx in indices:
        if idx - last_idx < spacing:
            continue
        selected.append(idx)
        last_idx = idx
    return selected


def generate_events(pair: dict) -> list[dict]:
    base_frame = build_pair_frame(pair)
    events: list[dict] = []

    for lookback in LOOKBACK_WINDOWS:
        df = add_window_features(base_frame, lookback, FORWARD_HORIZONS)
        masks = hypothesis_masks(df, lookback)
        if not masks:
            continue

        for horizon in FORWARD_HORIZONS:
            required_cols = [
                f"fwd_common_{horizon}",
                f"fwd_preferred_{horizon}",
                f"fwd_spread_change_{horizon}",
            ]
            valid_future = df[required_cols].notna().all(axis=1)
            spacing = max(lookback, horizon)

            for hypothesis, mask in masks.items():
                candidate_idx = df.index[mask & valid_future].tolist()
                selected_idx = select_non_overlapping(candidate_idx, spacing)
                for idx in selected_idx:
                    row = df.loc[idx]
                    events.append(
                        {
                            "pairId": pair["id"],
                            "pairName": pair["name"],
                            "commonName": pair["commonName"],
                            "preferredName": pair["preferredName"],
                            "lookback": lookback,
                            "horizon": horizon,
                            "hypothesis": hypothesis,
                            "eventDate": row["date"].strftime("%Y-%m-%d"),
                            "period": period_bucket(row["date"]),
                            "lb_common_return": row[f"lb_common_{lookback}"],
                            "lb_preferred_return": row[f"lb_preferred_{lookback}"],
                            "lb_spread_change": row[f"lb_spread_change_{lookback}"],
                            "fwd_common_return": row[f"fwd_common_{horizon}"],
                            "fwd_preferred_return": row[f"fwd_preferred_{horizon}"],
                            "fwd_spread_change": row[f"fwd_spread_change_{horizon}"],
                            "fwd_relative_return": row[f"fwd_common_{horizon}"] - row[f"fwd_preferred_{horizon}"],
                        }
                    )

    return events


def bootstrap_mean(values_by_group: dict[str, np.ndarray], direction: str, reps: int, seed_offset: int) -> BootstrapResult:
    groups = {group: values for group, values in values_by_group.items() if len(values)}
    if not groups:
        return BootstrapResult(None, None, None, None)

    stacked = np.concatenate(list(groups.values()))
    observed = float(stacked.mean())

    rng = np.random.default_rng(SEED + seed_offset)
    group_names = list(groups)
    boot_means = np.empty(reps)
    for i in range(reps):
        sampled_groups = rng.choice(group_names, size=len(group_names), replace=True)
        sampled_values = np.concatenate([groups[name] for name in sampled_groups])
        boot_means[i] = sampled_values.mean()

    ci_low, ci_high = np.quantile(boot_means, [0.025, 0.975])
    if direction == "negative":
        p_value = float(np.mean(boot_means >= 0))
    elif direction == "positive":
        p_value = float(np.mean(boot_means <= 0))
    else:
        p_value = float(2 * min(np.mean(boot_means >= 0), np.mean(boot_means <= 0)))

    return BootstrapResult(observed, float(ci_low), float(ci_high), min(max(p_value, 0.0), 1.0))


def bootstrap_difference(
    left: pd.DataFrame,
    right: pd.DataFrame,
    metric: str,
    direction: str,
    reps: int,
    seed_offset: int,
) -> BootstrapResult:
    left_groups = {
        pair_id: group[metric].dropna().to_numpy()
        for pair_id, group in left.groupby("pairId")
    }
    right_groups = {
        pair_id: group[metric].dropna().to_numpy()
        for pair_id, group in right.groupby("pairId")
    }
    pair_ids = sorted(set(left_groups) | set(right_groups))
    if not pair_ids:
        return BootstrapResult(None, None, None, None)

    left_all = left[metric].dropna().to_numpy()
    right_all = right[metric].dropna().to_numpy()
    if len(left_all) == 0 or len(right_all) == 0:
        return BootstrapResult(None, None, None, None)

    observed = float(left_all.mean() - right_all.mean())
    rng = np.random.default_rng(SEED + seed_offset)
    boot_diffs = np.empty(reps)
    for i in range(reps):
        sampled_pairs = rng.choice(pair_ids, size=len(pair_ids), replace=True)
        left_vals = [left_groups[pid] for pid in sampled_pairs if pid in left_groups and len(left_groups[pid])]
        right_vals = [right_groups[pid] for pid in sampled_pairs if pid in right_groups and len(right_groups[pid])]
        left_mean = np.concatenate(left_vals).mean() if left_vals else np.nan
        right_mean = np.concatenate(right_vals).mean() if right_vals else np.nan
        boot_diffs[i] = left_mean - right_mean

    boot_diffs = boot_diffs[~np.isnan(boot_diffs)]
    if len(boot_diffs) == 0:
        return BootstrapResult(observed, None, None, None)

    ci_low, ci_high = np.quantile(boot_diffs, [0.025, 0.975])
    if direction == "positive":
        p_value = float(np.mean(boot_diffs <= 0))
    elif direction == "negative":
        p_value = float(np.mean(boot_diffs >= 0))
    else:
        p_value = float(2 * min(np.mean(boot_diffs >= 0), np.mean(boot_diffs <= 0)))

    return BootstrapResult(observed, float(ci_low), float(ci_high), min(max(p_value, 0.0), 1.0))


def support_rate(events: pd.DataFrame, hypothesis: str) -> float | None:
    if events.empty:
        return None
    if hypothesis == "H1":
        mask = (events["fwd_common_return"] < 0) & (events["fwd_spread_change"] < 0)
    elif hypothesis == "H2":
        mask = (events["fwd_common_return"] >= 0) & (events["fwd_spread_change"].abs() <= 1.0)
    else:
        mask = (events["fwd_preferred_return"] < 0) & (events["fwd_spread_change"] > 0)
    return float(mask.mean())


def summarize_pooled(events: pd.DataFrame) -> pd.DataFrame:
    rows = []
    seed_offset = 0
    for lookback in LOOKBACK_WINDOWS:
        for horizon in FORWARD_HORIZONS:
            subset = events[(events["lookback"] == lookback) & (events["horizon"] == horizon)]
            for hypothesis in ("H1", "H2", "H3"):
                group = subset[subset["hypothesis"] == hypothesis]
                if group.empty:
                    continue

                primary_metric = PRIMARY_METRIC[hypothesis]
                metrics = [primary_metric, *SECONDARY_METRICS[hypothesis]]
                record = {
                    "hypothesis": hypothesis,
                    "lookback": lookback,
                    "horizon": horizon,
                    "events": int(len(group)),
                    "pairs": int(group["pairId"].nunique()),
                    "supportRate": support_rate(group, hypothesis),
                }

                for metric in metrics:
                    by_group = {
                        pair_id: pair_group[metric].dropna().to_numpy()
                        for pair_id, pair_group in group.groupby("pairId")
                    }
                    result = bootstrap_mean(by_group, DIRECTION[(hypothesis, metric)], BOOTSTRAP_REPS, seed_offset)
                    seed_offset += 1
                    record[f"{metric}Mean"] = result.mean
                    record[f"{metric}CiLow"] = result.ci_low
                    record[f"{metric}CiHigh"] = result.ci_high
                    record[f"{metric}P"] = result.p_value

                rows.append(record)

    summary = pd.DataFrame(rows)
    if summary.empty:
        return summary

    comparisons = []
    for lookback in LOOKBACK_WINDOWS:
        for horizon in FORWARD_HORIZONS:
            h1 = events[
                (events["lookback"] == lookback)
                & (events["horizon"] == horizon)
                & (events["hypothesis"] == "H1")
            ]
            h2 = events[
                (events["lookback"] == lookback)
                & (events["horizon"] == horizon)
                & (events["hypothesis"] == "H2")
            ]
            if h1.empty or h2.empty:
                continue
            result = bootstrap_difference(
                h2,
                h1,
                "fwd_common_return",
                "positive",
                BOOTSTRAP_REPS,
                10_000 + lookback * 100 + horizon,
            )
            comparisons.append(
                {
                    "lookback": lookback,
                    "horizon": horizon,
                    "H2minusH1CommonMean": result.mean,
                    "H2minusH1CommonCiLow": result.ci_low,
                    "H2minusH1CommonCiHigh": result.ci_high,
                    "H2minusH1CommonP": result.p_value,
                }
            )
            relative_result = bootstrap_difference(
                h2,
                h1,
                "fwd_relative_return",
                "negative",
                BOOTSTRAP_REPS,
                20_000 + lookback * 100 + horizon,
            )
            comparisons[-1].update(
                {
                    "H2minusH1RelativeMean": relative_result.mean,
                    "H2minusH1RelativeCiLow": relative_result.ci_low,
                    "H2minusH1RelativeCiHigh": relative_result.ci_high,
                    "H2minusH1RelativeP": relative_result.p_value,
                }
            )
    comparison_df = pd.DataFrame(comparisons)
    if not comparison_df.empty:
        summary = summary.merge(comparison_df, on=["lookback", "horizon"], how="left")

    return summary.sort_values(["hypothesis", "lookback", "horizon"]).reset_index(drop=True)


def summarize_periods(events: pd.DataFrame) -> pd.DataFrame:
    primary = events[
        (events["lookback"] == PRIMARY_LOOKBACK)
        & (events["horizon"] == PRIMARY_HORIZON)
    ]
    rows = []
    seed_offset = 20_000

    for hypothesis in ("H1", "H2", "H3"):
        subset = primary[primary["hypothesis"] == hypothesis]
        primary_metric = PRIMARY_METRIC[hypothesis]
        for period, group in subset.groupby("period"):
            if len(group) < MIN_PERIOD_EVENTS or group["pairId"].nunique() < MIN_PERIOD_PAIRS:
                continue
            by_group = {
                pair_id: pair_group[primary_metric].dropna().to_numpy()
                for pair_id, pair_group in group.groupby("pairId")
            }
            result = bootstrap_mean(by_group, DIRECTION[(hypothesis, primary_metric)], BOOTSTRAP_REPS, seed_offset)
            seed_offset += 1
            rows.append(
                {
                    "hypothesis": hypothesis,
                    "period": period,
                    "events": int(len(group)),
                    "pairs": int(group["pairId"].nunique()),
                    "supportRate": support_rate(group, hypothesis),
                    "metric": primary_metric,
                    "mean": result.mean,
                    "ciLow": result.ci_low,
                    "ciHigh": result.ci_high,
                    "pValue": result.p_value,
                }
            )

    return pd.DataFrame(rows).sort_values(["hypothesis", "period"]).reset_index(drop=True)


def benjamini_hochberg(p_values: pd.Series) -> pd.Series:
    valid = p_values.notna()
    if not valid.any():
        return pd.Series(np.nan, index=p_values.index)

    ordered = p_values[valid].sort_values()
    ranks = np.arange(1, len(ordered) + 1)
    q_values = ordered * len(ordered) / ranks
    q_values = np.minimum.accumulate(q_values.iloc[::-1])[::-1].clip(upper=1.0)
    output = pd.Series(np.nan, index=p_values.index)
    output.loc[ordered.index] = q_values
    return output


def summarize_pairs(events: pd.DataFrame) -> pd.DataFrame:
    primary = events[
        (events["lookback"] == PRIMARY_LOOKBACK)
        & (events["horizon"] == PRIMARY_HORIZON)
    ]

    rows = []
    seed_offset = 40_000
    for hypothesis in ("H1", "H2", "H3"):
        metric = PRIMARY_METRIC[hypothesis]
        direction = DIRECTION[(hypothesis, metric)]
        subset = primary[primary["hypothesis"] == hypothesis]
        for pair_id, group in subset.groupby("pairId"):
            if len(group) < MIN_PAIR_EVENTS:
                continue
            values = group[metric].dropna().to_numpy()
            if len(values) < MIN_PAIR_EVENTS:
                continue
            observed = float(values.mean())
            rng = np.random.default_rng(SEED + seed_offset)
            seed_offset += 1
            boot_means = np.empty(PAIR_BOOTSTRAP_REPS)
            for i in range(PAIR_BOOTSTRAP_REPS):
                boot_means[i] = rng.choice(values, size=len(values), replace=True).mean()
            ci_low, ci_high = np.quantile(boot_means, [0.025, 0.975])
            if direction == "negative":
                p_value = float(np.mean(boot_means >= 0))
            elif direction == "positive":
                p_value = float(np.mean(boot_means <= 0))
            else:
                p_value = float(2 * min(np.mean(boot_means >= 0), np.mean(boot_means <= 0)))

            rows.append(
                {
                    "hypothesis": hypothesis,
                    "pairId": pair_id,
                    "pairName": group["pairName"].iloc[0],
                    "events": int(len(group)),
                    "supportRate": support_rate(group, hypothesis),
                    "metric": metric,
                    "mean": observed,
                    "ciLow": float(ci_low),
                    "ciHigh": float(ci_high),
                    "pValue": p_value,
                }
            )

    result = pd.DataFrame(rows)
    if result.empty:
        return result

    result["qValue"] = result.groupby("hypothesis")["pValue"].transform(benjamini_hochberg)
    return result.sort_values(["hypothesis", "qValue", "mean"]).reset_index(drop=True)


def format_pct(value: float | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value * 100:.{digits}f}%"


def format_point(value: float | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:.{digits}f}%p"


def format_p(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    if value < 0.001:
        return "<0.001"
    return f"{value:.3f}"


def write_outputs(events: pd.DataFrame, pooled: pd.DataFrame, periods: pd.DataFrame, pairs: pd.DataFrame) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    events.to_csv(OUTPUT_DIR / "hypothesis_events.csv", index=False, encoding="utf-8-sig")
    pooled.to_csv(OUTPUT_DIR / "pooled_summary.csv", index=False, encoding="utf-8-sig")
    periods.to_csv(OUTPUT_DIR / "period_summary.csv", index=False, encoding="utf-8-sig")
    pairs.to_csv(OUTPUT_DIR / "pair_summary_primary.csv", index=False, encoding="utf-8-sig")


def top_pair_table(pairs: pd.DataFrame, hypothesis: str, ascending: bool, limit: int = 8) -> list[str]:
    subset = pairs[pairs["hypothesis"] == hypothesis].copy()
    if subset.empty:
        return ["- 충분한 이벤트가 있는 종목이 없습니다."]

    subset = subset.sort_values(["qValue", "mean"], ascending=[True, ascending]).head(limit)
    lines = []
    for _, row in subset.iterrows():
        lines.append(
            "- "
            + f"{row['pairName']}: 평균 {format_pct(row['mean'])}, "
            + f"95% CI [{format_pct(row['ciLow'])}, {format_pct(row['ciHigh'])}], "
            + f"q={format_p(row['qValue'])}, 이벤트 {int(row['events'])}건"
        )
    return lines


def render_report(events: pd.DataFrame, pooled: pd.DataFrame, periods: pd.DataFrame, pairs: pd.DataFrame) -> None:
    primary = pooled[
        (pooled["lookback"] == PRIMARY_LOOKBACK)
        & (pooled["horizon"] == PRIMARY_HORIZON)
    ].set_index("hypothesis")

    lines: list[str] = []
    lines.append("# 우선주/보통주 괴리율 가설 검증")
    lines.append("")
    lines.append("## 방법")
    lines.append("")
    lines.append("- 데이터: `data.js`의 53개 보통주/우선주 페어 일별 종가 히스토리")
    lines.append("- 이벤트 정의:")
    lines.append(f"  - H1: 최근 {PRIMARY_LOOKBACK}거래일 동안 보통주 급등 + 괴리율 확대 + 보통주가 우선주보다 더 많이 상승")
    lines.append(f"  - H2: 최근 {PRIMARY_LOOKBACK}거래일 동안 보통주 급등, 그러나 괴리율은 거의 확대되지 않음")
    lines.append(f"  - H3: 최근 {PRIMARY_LOOKBACK}거래일 동안 우선주 급등 + 괴리율 축소 + 우선주가 보통주보다 더 많이 상승")
    lines.append("- 임계값은 각 종목 내부 분포의 분위수(주로 90/10/40/75 분위수)로 고정했습니다.")
    lines.append("- 미래 성과는 5, 20, 60거래일 후 수익률과 괴리율 변화로 측정했고, 종목별 이벤트는 `max(lookback, horizon)` 간격으로 비중첩 선택했습니다.")
    lines.append("- 장기 공백 구간(14일 초과)은 이벤트/성과 계산에서 제외했습니다.")
    lines.append("- 전체 추론은 종목 단위 클러스터 부트스트랩(2,000회), 종목별 결과는 이벤트 부트스트랩(3,000회)과 Benjamini-Hochberg FDR 보정으로 계산했습니다.")
    lines.append("- p값은 가설이 예측한 방향의 단측검정입니다. 따라서 p가 1에 가까우면 '지지'가 아니라 '반대 방향으로 움직였다'는 뜻입니다.")
    lines.append("- 따라서 아래 해석은 탐색적 예측력 검정이며, 인과 추정은 아닙니다.")
    lines.append("")
    lines.append("## 1차 결론")
    lines.append("")

    if not primary.empty:
        h1 = primary.loc["H1"]
        h2 = primary.loc["H2"]
        h3 = primary.loc["H3"]
        lines.append(
            "- "
            + "H1 "
            + f"({int(h1['events'])}건): 보통주의 20거래일 후 평균 수익률은 {format_pct(h1['fwd_common_returnMean'])} "
            + f"(95% CI {format_pct(h1['fwd_common_returnCiLow'])} ~ {format_pct(h1['fwd_common_returnCiHigh'])}, "
            + f"p={format_p(h1['fwd_common_returnP'])})였습니다."
        )
        lines.append(
            "- "
            + f"H1의 괴리율 변화는 {format_point(h1['fwd_spread_changeMean'])} "
            + f"(p={format_p(h1['fwd_spread_changeP'])})였고, 지지 이벤트 비율은 {format_pct(h1['supportRate'])}입니다."
        )
        lines.append(
            "- "
            + "H2 "
            + f"({int(h2['events'])}건): 보통주의 20거래일 후 평균 수익률은 {format_pct(h2['fwd_common_returnMean'])} "
            + f"(95% CI {format_pct(h2['fwd_common_returnCiLow'])} ~ {format_pct(h2['fwd_common_returnCiHigh'])}, "
            + f"p={format_p(h2['fwd_common_returnP'])})였습니다."
        )
        lines.append(
            "- "
            + f"H2 - H1 보통주 수익률 차이는 {format_pct(h2['H2minusH1CommonMean'])} "
            + f"(95% CI {format_pct(h2['H2minusH1CommonCiLow'])} ~ {format_pct(h2['H2minusH1CommonCiHigh'])}, "
            + f"p={format_p(h2['H2minusH1CommonP'])})입니다."
        )
        lines.append(
            "- "
            + f"H2 - H1 상대수익률(보통주-우선주) 차이는 {format_pct(h2['H2minusH1RelativeMean'])} "
            + f"(95% CI {format_pct(h2['H2minusH1RelativeCiLow'])} ~ {format_pct(h2['H2minusH1RelativeCiHigh'])}, "
            + f"p={format_p(h2['H2minusH1RelativeP'])})입니다."
        )
        lines.append(
            "- "
            + "H3 "
            + f"({int(h3['events'])}건): 우선주의 20거래일 후 평균 수익률은 {format_pct(h3['fwd_preferred_returnMean'])} "
            + f"(95% CI {format_pct(h3['fwd_preferred_returnCiLow'])} ~ {format_pct(h3['fwd_preferred_returnCiHigh'])}, "
            + f"p={format_p(h3['fwd_preferred_returnP'])})였습니다."
        )
        lines.append(
            "- "
            + f"H3의 괴리율 변화는 {format_point(h3['fwd_spread_changeMean'])} "
            + f"(p={format_p(h3['fwd_spread_changeP'])})였고, 지지 이벤트 비율은 {format_pct(h3['supportRate'])}입니다."
        )
    else:
        lines.append("- 1차 결론을 만들 만큼 충분한 이벤트가 없었습니다.")

    lines.append("")
    lines.append("## 기간별 결과")
    lines.append("")
    if periods.empty:
        lines.append("- 기간별 비교에 쓸 만큼 충분한 이벤트가 없었습니다.")
    else:
        for hypothesis in ("H1", "H2", "H3"):
            subset = periods[periods["hypothesis"] == hypothesis]
            if subset.empty:
                continue
            lines.append(f"### {hypothesis}")
            lines.append("")
            for _, row in subset.iterrows():
                lines.append(
                    "- "
                    + f"{row['period']}: 평균 {format_pct(row['mean'])}, "
                    + f"95% CI [{format_pct(row['ciLow'])}, {format_pct(row['ciHigh'])}], "
                    + f"p={format_p(row['pValue'])}, 이벤트 {int(row['events'])}건, "
                    + f"종목 {int(row['pairs'])}개, 지지율 {format_pct(row['supportRate'])}"
                )
            lines.append("")

    lines.append("## 종목별 결과 (1차 스펙: 5일 신호, 20일 성과)")
    lines.append("")
    lines.append("### H1을 가장 잘 지지한 종목")
    lines.extend(top_pair_table(pairs, "H1", ascending=True))
    lines.append("")
    lines.append("### H2를 가장 잘 지지한 종목")
    lines.extend(top_pair_table(pairs, "H2", ascending=False))
    lines.append("")
    lines.append("### H3을 가장 잘 지지한 종목")
    lines.extend(top_pair_table(pairs, "H3", ascending=True))
    lines.append("")
    lines.append("## 강건성 체크")
    lines.append("")
    for hypothesis in ("H1", "H2", "H3"):
        subset = pooled[pooled["hypothesis"] == hypothesis]
        if subset.empty:
            continue
        lines.append(f"### {hypothesis}")
        lines.append("")
        for _, row in subset.iterrows():
            primary_metric = PRIMARY_METRIC[hypothesis]
            mean_key = f"{primary_metric}Mean"
            ci_low_key = f"{primary_metric}CiLow"
            ci_high_key = f"{primary_metric}CiHigh"
            p_key = f"{primary_metric}P"
            lines.append(
                "- "
                + f"lookback {int(row['lookback'])} / horizon {int(row['horizon'])}: "
                + f"{format_pct(row[mean_key])}, "
                + f"95% CI [{format_pct(row[ci_low_key])}, {format_pct(row[ci_high_key])}], "
                + f"p={format_p(row[p_key])}, 이벤트 {int(row['events'])}건"
            )
        lines.append("")

    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    pairs = load_pairs()
    event_rows: list[dict] = []
    for pair in pairs:
        event_rows.extend(generate_events(pair))

    events = pd.DataFrame(event_rows).sort_values(
        ["hypothesis", "lookback", "horizon", "eventDate", "pairId"]
    ).reset_index(drop=True)
    pooled = summarize_pooled(events)
    periods = summarize_periods(events)
    pair_summary = summarize_pairs(events)

    write_outputs(events, pooled, periods, pair_summary)
    render_report(events, pooled, periods, pair_summary)

    print(f"events: {len(events):,}")
    print(f"pooled rows: {len(pooled):,}")
    print(f"period rows: {len(periods):,}")
    print(f"pair rows: {len(pair_summary):,}")
    print(f"report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
