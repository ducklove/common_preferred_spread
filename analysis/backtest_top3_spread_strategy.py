#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "data.js"
OUTPUT_DIR = ROOT / "analysis" / "outputs"
REPORT_PATH = ROOT / "analysis" / "top3_spread_strategy_report.md"

START_SIGNAL_DATE = pd.Timestamp("1996-06-25")
INITIAL_CAPITAL = 10_000_000.0
BUY_FEE_RATE = 0.01
SELL_FEE_RATE = 0.01
REQUIRED_STREAK = 5
PORTFOLIO_SIZE = 3
KOSPI_CACHE_PATH = ROOT / ".cache" / "naver_index" / "KOSPI.csv"
NAVER_INDEX_WORKERS = 8


@dataclass
class PendingSwap:
    signal_date: pd.Timestamp
    earliest_exec_date: pd.Timestamp
    buy_pair_id: str
    sell_pair_id: str
    signal_buy_spread: float
    signal_sell_spread: float


def load_stock_data() -> dict:
    content = DATA_PATH.read_text(encoding="utf-8")
    prefix = "const STOCK_DATA = "
    payload = content[len(prefix):]
    if payload.endswith(";\n"):
        payload = payload[:-2]
    elif payload.endswith(";"):
        payload = payload[:-1]
    return json.loads(payload)


def load_pair_frames() -> dict[str, dict]:
    stock_data = load_stock_data()
    pairs = [pair for pair in stock_data["pairs"] if not pair.get("isAverage")]
    out = {}
    for pair in pairs:
        frame = pd.DataFrame(pair["history"]).copy()
        frame["date"] = pd.to_datetime(frame["date"])
        frame = frame.sort_values("date").reset_index(drop=True)
        frame["preferredPrice"] = pd.to_numeric(frame["preferredPrice"], errors="coerce")
        frame["spread"] = pd.to_numeric(frame["spread"], errors="coerce")
        frame = frame[["date", "preferredPrice", "spread"]].dropna()
        frame = frame.drop_duplicates(subset=["date"], keep="last")
        frame = frame.set_index("date").sort_index()
        out[pair["id"]] = {
            "id": pair["id"],
            "name": pair["name"],
            "preferredName": pair["preferredName"],
            "commonName": pair["commonName"],
            "frame": frame,
        }
    return out


def load_preferred_dividend_histories() -> dict[str, pd.Series]:
    stock_data = load_stock_data()
    out: dict[str, pd.Series] = {}
    for pair_id, item in stock_data.get("dividendHistories", {}).items():
        rows = []
        for entry in item.get("preferred", []):
            date = pd.to_datetime(entry.get("date"), errors="coerce")
            amount = pd.to_numeric(entry.get("amount"), errors="coerce")
            if pd.isna(date) or pd.isna(amount) or float(amount) <= 0:
                continue
            rows.append((date.normalize(), float(amount)))
        if not rows:
            out[pair_id] = pd.Series(dtype="float64")
            continue
        frame = pd.DataFrame(rows, columns=["date", "amount"])
        out[pair_id] = frame.groupby("date")["amount"].sum().sort_index()
    return out


def get_strategy_calendar(pair_frames: dict[str, dict]) -> list[pd.Timestamp]:
    all_dates = set()
    for item in pair_frames.values():
        all_dates.update(item["frame"].index)
    return sorted(date for date in all_dates if date >= START_SIGNAL_DATE)


def get_daily_quotes(pair_frames: dict[str, dict], calendar: list[pd.Timestamp]) -> dict[pd.Timestamp, dict[str, dict]]:
    quotes_by_date = {date: {} for date in calendar}
    for pair_id, item in pair_frames.items():
        frame = item["frame"]
        eligible = frame[frame.index >= START_SIGNAL_DATE]
        for date, row in eligible.iterrows():
            if date not in quotes_by_date:
                quotes_by_date[date] = {}
            quotes_by_date[date][pair_id] = {
                "preferredPrice": float(row["preferredPrice"]),
                "spread": float(row["spread"]),
            }
    return quotes_by_date


def fetch_naver_index_page(page: int) -> pd.DataFrame:
    request = Request(
        f"https://finance.naver.com/sise/sise_index_day.naver?code=KOSPI&page={page}",
        headers={"User-Agent": "Mozilla/5.0"},
    )
    with urlopen(request, timeout=20) as response:
        html = response.read().decode("euc-kr", errors="replace")

    tables = pd.read_html(StringIO(html))
    if not tables:
        return pd.DataFrame(columns=["date", "close"])

    table = tables[0]
    if table.shape[1] < 2:
        return pd.DataFrame(columns=["date", "close"])

    table = table[["날짜", "체결가"]].dropna()
    if table.empty:
        return pd.DataFrame(columns=["date", "close"])

    df = pd.DataFrame(
        {
            "date": pd.to_datetime(table["날짜"], format="%Y.%m.%d", errors="coerce"),
            "close": pd.to_numeric(table["체결가"], errors="coerce"),
        }
    ).dropna()
    return df


def fetch_kospi_history() -> pd.Series:
    if KOSPI_CACHE_PATH.exists():
        cached = pd.read_csv(KOSPI_CACHE_PATH, parse_dates=["date"])
        if not cached.empty:
            return cached.set_index("date")["close"].sort_index()

    request = Request(
        "https://finance.naver.com/sise/sise_index_day.naver?code=KOSPI&page=1",
        headers={"User-Agent": "Mozilla/5.0"},
    )
    with urlopen(request, timeout=20) as response:
        html = response.read().decode("euc-kr", errors="replace")
    match = re.search(r"pgRR.*?page=(\d+)", html, re.S)
    last_page = int(match.group(1)) if match else 1

    frames = []
    with ThreadPoolExecutor(max_workers=NAVER_INDEX_WORKERS) as executor:
        futures = {executor.submit(fetch_naver_index_page, page): page for page in range(1, last_page + 1)}
        for future in as_completed(futures):
            frames.append(future.result())

    history = pd.concat(frames, ignore_index=True)
    history = history.dropna(subset=["date", "close"]).drop_duplicates(subset=["date"], keep="last")
    history = history.sort_values("date")

    KOSPI_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    history.to_csv(KOSPI_CACHE_PATH, index=False, encoding="utf-8")
    return history.set_index("date")["close"]


def get_next_calendar_date(calendar: list[pd.Timestamp], current_date: pd.Timestamp) -> pd.Timestamp | None:
    idx = calendar.index(current_date)
    if idx + 1 >= len(calendar):
        return None
    return calendar[idx + 1]


def compute_equity(
    cash: float,
    positions: dict[str, int],
    latest_prices: dict[str, float],
) -> tuple[float, dict[str, float]]:
    position_values = {}
    total = cash
    for pair_id, shares in positions.items():
        price = latest_prices.get(pair_id)
        if price is None:
            continue
        value = shares * price
        position_values[pair_id] = value
        total += value
    return total, position_values


def pick_initial_top3(quotes: dict[str, dict]) -> list[str]:
    ranked = sorted(quotes.items(), key=lambda item: item[1]["spread"], reverse=True)
    return [pair_id for pair_id, _ in ranked[:PORTFOLIO_SIZE]]


def choose_signal_candidate(
    top3_ids: list[str],
    quotes: dict[str, dict],
    positions: dict[str, int],
    consecutive_top3: dict[str, int],
    latest_spreads: dict[str, float],
) -> tuple[str, str, float, float] | None:
    outsiders = [
        pair_id
        for pair_id in top3_ids
        if pair_id not in positions and consecutive_top3.get(pair_id, 0) >= REQUIRED_STREAK
    ]
    if not outsiders or len(positions) < PORTFOLIO_SIZE:
        return None

    buy_pair_id = max(outsiders, key=lambda pair_id: quotes[pair_id]["spread"])
    sell_candidates = [(pair_id, latest_spreads.get(pair_id, -math.inf)) for pair_id in positions]
    sell_pair_id, sell_spread = min(sell_candidates, key=lambda item: item[1])
    return buy_pair_id, sell_pair_id, float(quotes[buy_pair_id]["spread"]), float(sell_spread)


def execute_buy(
    date: pd.Timestamp,
    pair_id: str,
    quotes: dict[str, dict],
    cash: float,
    total_equity: float,
) -> tuple[float, int, dict] | tuple[float, int, None]:
    quote = quotes.get(pair_id)
    if not quote:
        return cash, 0, None

    price = float(quote["preferredPrice"])
    max_notional = min(total_equity / PORTFOLIO_SIZE, cash / (1 + BUY_FEE_RATE))
    if max_notional <= 0 or price <= 0:
        return cash, 0, None

    shares = int(max_notional // price)
    if shares <= 0:
        return cash, 0, None

    trade_notional = shares * price
    fee = trade_notional * BUY_FEE_RATE
    total_cash_out = trade_notional + fee
    if total_cash_out > cash + 1e-9:
        shares = int((cash / (1 + BUY_FEE_RATE)) // price)
        if shares <= 0:
            return cash, 0, None
        trade_notional = shares * price
        fee = trade_notional * BUY_FEE_RATE
        total_cash_out = trade_notional + fee

    cash -= total_cash_out
    return cash, shares, {
        "date": date.strftime("%Y-%m-%d"),
        "pairId": pair_id,
        "action": "BUY",
        "price": price,
        "shares": shares,
        "notional": trade_notional,
        "fee": fee,
        "spread": float(quote["spread"]),
    }


def execute_sell(
    date: pd.Timestamp,
    pair_id: str,
    shares: int,
    quotes: dict[str, dict],
) -> tuple[float, dict | None]:
    quote = quotes.get(pair_id)
    if not quote or shares <= 0:
        return 0.0, None

    price = float(quote["preferredPrice"])
    gross_proceeds = shares * price
    fee = gross_proceeds * SELL_FEE_RATE
    net_proceeds = gross_proceeds - fee
    return net_proceeds, {
        "date": date.strftime("%Y-%m-%d"),
        "pairId": pair_id,
        "action": "SELL",
        "price": price,
        "shares": shares,
        "notional": gross_proceeds,
        "fee": fee,
        "spread": float(quote["spread"]),
    }


def max_drawdown(series: pd.Series) -> float:
    if series.empty:
        return float("nan")
    running_max = series.cummax()
    drawdown = series / running_max - 1
    return float(drawdown.min())


def run_backtest() -> dict:
    pair_frames = load_pair_frames()
    dividend_histories = load_preferred_dividend_histories()
    calendar = get_strategy_calendar(pair_frames)
    quotes_by_date = get_daily_quotes(pair_frames, calendar)

    if START_SIGNAL_DATE not in quotes_by_date:
        raise RuntimeError("시작 시점인 1996-06-25의 괴리율 데이터가 없습니다.")

    initial_top3 = pick_initial_top3(quotes_by_date[START_SIGNAL_DATE])
    if len(initial_top3) < PORTFOLIO_SIZE:
        raise RuntimeError("시작 시점에 매수 가능한 top3 종목이 부족합니다.")
    initial_exec_date = get_next_calendar_date(calendar, START_SIGNAL_DATE)
    if initial_exec_date is None:
        raise RuntimeError("초기 매수 실행일을 찾지 못했습니다.")

    cash = INITIAL_CAPITAL
    positions: dict[str, int] = {}
    latest_prices: dict[str, float] = {}
    latest_spreads: dict[str, float] = {}
    consecutive_top3 = {pair_id: 0 for pair_id in pair_frames}
    pending_swap: PendingSwap | None = None
    trade_log: list[dict] = []
    dividend_log: list[dict] = []
    daily_records: list[dict] = []
    cumulative_dividends = 0.0

    for current_date in calendar:
        quotes = quotes_by_date.get(current_date, {})
        for pair_id, quote in quotes.items():
            latest_prices[pair_id] = float(quote["preferredPrice"])
            latest_spreads[pair_id] = float(quote["spread"])

        dividend_cash_today = 0.0
        for pair_id, shares in positions.items():
            dividend_series = dividend_histories.get(pair_id)
            if dividend_series is None or dividend_series.empty or current_date not in dividend_series.index:
                continue
            amount_per_share = float(dividend_series.loc[current_date])
            cash_dividend = shares * amount_per_share
            if cash_dividend <= 0:
                continue
            cash += cash_dividend
            dividend_cash_today += cash_dividend
            cumulative_dividends += cash_dividend
            dividend_log.append(
                {
                    "date": current_date.strftime("%Y-%m-%d"),
                    "pairId": pair_id,
                    "pairName": pair_frames[pair_id]["name"],
                    "shares": shares,
                    "amountPerShare": amount_per_share,
                    "cashDividend": cash_dividend,
                    "cashAfter": cash,
                }
            )

        if current_date == initial_exec_date:
            initial_target_cash = INITIAL_CAPITAL / PORTFOLIO_SIZE
            for pair_id in initial_top3:
                quote = quotes.get(pair_id)
                if not quote:
                    continue
                max_notional = min(initial_target_cash / (1 + BUY_FEE_RATE), cash / (1 + BUY_FEE_RATE))
                if max_notional <= 0:
                    continue
                price = float(quote["preferredPrice"])
                shares = int(max_notional // price)
                if shares <= 0:
                    continue
                trade_notional = shares * price
                fee = trade_notional * BUY_FEE_RATE
                total_cash_out = trade_notional + fee
                cash -= total_cash_out
                positions[pair_id] = positions.get(pair_id, 0) + shares
                trade_log.append(
                    {
                        "date": current_date.strftime("%Y-%m-%d"),
                        "pairId": pair_id,
                        "pairName": pair_frames[pair_id]["name"],
                        "action": "BUY_INIT",
                        "price": price,
                        "shares": shares,
                        "notional": trade_notional,
                        "fee": fee,
                        "spread": float(quote["spread"]),
                        "cashAfter": cash,
                    }
                )

        if pending_swap and current_date >= pending_swap.earliest_exec_date:
            if pending_swap.sell_pair_id in positions and pending_swap.buy_pair_id not in positions:
                sell_shares = positions[pending_swap.sell_pair_id]
                sell_proceeds, sell_trade = execute_sell(current_date, pending_swap.sell_pair_id, sell_shares, quotes)
                can_buy = quotes.get(pending_swap.buy_pair_id) is not None
                if sell_trade and can_buy:
                    cash += sell_proceeds
                    del positions[pending_swap.sell_pair_id]
                    total_equity, _ = compute_equity(cash, positions, latest_prices)
                    cash, buy_shares, buy_trade = execute_buy(current_date, pending_swap.buy_pair_id, quotes, cash, total_equity)
                    if buy_trade and buy_shares > 0:
                        positions[pending_swap.buy_pair_id] = buy_shares
                        sell_trade.update(
                            {
                                "pairName": pair_frames[pending_swap.sell_pair_id]["name"],
                                "reason": "rebalance_out",
                                "signalDate": pending_swap.signal_date.strftime("%Y-%m-%d"),
                                "cashAfter": cash + buy_trade["notional"] + buy_trade["fee"],
                            }
                        )
                        buy_trade.update(
                            {
                                "pairName": pair_frames[pending_swap.buy_pair_id]["name"],
                                "reason": "rebalance_in",
                                "signalDate": pending_swap.signal_date.strftime("%Y-%m-%d"),
                                "signalBuySpread": pending_swap.signal_buy_spread,
                                "signalSellSpread": pending_swap.signal_sell_spread,
                                "cashAfter": cash,
                            }
                        )
                        trade_log.extend([sell_trade, buy_trade])
                        pending_swap = None

        ranked_today = sorted(quotes.items(), key=lambda item: item[1]["spread"], reverse=True)
        top3_ids = [pair_id for pair_id, _ in ranked_today[:PORTFOLIO_SIZE]]
        top3_set = set(top3_ids)
        for pair_id in consecutive_top3:
            if pair_id in top3_set:
                consecutive_top3[pair_id] += 1
            else:
                consecutive_top3[pair_id] = 0

        if pending_swap is None and len(positions) == PORTFOLIO_SIZE and len(top3_ids) == PORTFOLIO_SIZE:
            signal = choose_signal_candidate(top3_ids, quotes, positions, consecutive_top3, latest_spreads)
            next_date = get_next_calendar_date(calendar, current_date)
            if signal and next_date is not None:
                buy_pair_id, sell_pair_id, buy_spread, sell_spread = signal
                pending_swap = PendingSwap(
                    signal_date=current_date,
                    earliest_exec_date=next_date,
                    buy_pair_id=buy_pair_id,
                    sell_pair_id=sell_pair_id,
                    signal_buy_spread=buy_spread,
                    signal_sell_spread=sell_spread,
                )

        equity, position_values = compute_equity(cash, positions, latest_prices)
        daily_records.append(
            {
                "date": current_date.strftime("%Y-%m-%d"),
                "cash": cash,
                "equity": equity,
                "dividendCash": dividend_cash_today,
                "cumulativeDividends": cumulative_dividends,
                "holdings": ",".join(sorted(positions)),
                "top3": ",".join(top3_ids),
                "pendingSwapBuy": pending_swap.buy_pair_id if pending_swap else "",
                "pendingSwapSell": pending_swap.sell_pair_id if pending_swap else "",
                "positionValue": sum(position_values.values()),
            }
        )

    daily_df = pd.DataFrame(daily_records)
    trade_df = pd.DataFrame(trade_log)
    dividend_df = pd.DataFrame(dividend_log)

    kospi_series = fetch_kospi_history()
    kospi_series = kospi_series[kospi_series.index >= initial_exec_date]
    kospi_start_price = float(kospi_series.loc[kospi_series.index.min()])
    kospi_df = kospi_series.rename("kospiClose").to_frame()
    kospi_df["kospiValue"] = INITIAL_CAPITAL * kospi_df["kospiClose"] / kospi_start_price

    daily_df["date"] = pd.to_datetime(daily_df["date"])
    merged = (
        daily_df[
            [
                "date",
                "cash",
                "equity",
                "dividendCash",
                "cumulativeDividends",
                "holdings",
                "top3",
                "pendingSwapBuy",
                "pendingSwapSell",
                "positionValue",
            ]
        ]
        .merge(kospi_df.reset_index().rename(columns={"index": "date"}), on="date", how="left")
        .sort_values("date")
    )
    merged["kospiClose"] = merged["kospiClose"].ffill()
    merged["kospiValue"] = merged["kospiValue"].ffill()

    annual = (
        merged.groupby(merged["date"].dt.year)
        .tail(1)
        .copy()
        .rename(columns={"date": "yearEndDate", "equity": "strategyValue"})
    )
    annual["year"] = annual["yearEndDate"].dt.year
    annual["strategyYoY"] = annual["strategyValue"].pct_change()
    annual["kospiYoY"] = annual["kospiValue"].pct_change()
    annual["excessYoY"] = annual["strategyYoY"] - annual["kospiYoY"]
    annual = annual[
        [
            "year",
            "yearEndDate",
            "strategyValue",
            "strategyYoY",
            "kospiValue",
            "kospiYoY",
            "excessYoY",
            "cumulativeDividends",
        ]
    ]

    strategy_start = merged["date"].min()
    strategy_end = merged["date"].max()
    years_elapsed = (strategy_end - strategy_start).days / 365.25
    final_equity = float(merged["equity"].iloc[-1])
    final_kospi = float(merged["kospiValue"].iloc[-1])
    summary = {
        "strategyStart": strategy_start.strftime("%Y-%m-%d"),
        "strategyEnd": strategy_end.strftime("%Y-%m-%d"),
        "initialTop3": initial_top3,
        "initialExecDate": initial_exec_date.strftime("%Y-%m-%d"),
        "finalStrategyValue": final_equity,
        "finalKospiValue": final_kospi,
        "strategyCAGR": (final_equity / INITIAL_CAPITAL) ** (1 / years_elapsed) - 1 if years_elapsed > 0 else np.nan,
        "kospiCAGR": (final_kospi / INITIAL_CAPITAL) ** (1 / years_elapsed) - 1 if years_elapsed > 0 else np.nan,
        "strategyMaxDrawdown": max_drawdown(merged["equity"]),
        "kospiMaxDrawdown": max_drawdown(merged["kospiValue"]),
        "tradeCount": int(len(trade_df)),
        "rebalanceCount": int((trade_df["action"] == "BUY").sum()) if not trade_df.empty else 0,
        "finalCash": float(daily_df["cash"].iloc[-1]),
        "totalDividendsReceived": float(cumulative_dividends),
        "dividendEventCount": int(len(dividend_df)),
    }

    return {
        "summary": summary,
        "annual": annual,
        "daily": merged,
        "trades": trade_df,
        "dividends": dividend_df,
        "pairFrames": pair_frames,
    }


def fmt_money(value: float) -> str:
    return f"{value:,.0f}"


def fmt_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value * 100:.2f}%"


def write_report(results: dict) -> None:
    summary = results["summary"]
    annual = results["annual"]
    trades = results["trades"]
    dividends = results["dividends"]
    pair_frames = results["pairFrames"]

    initial_names = [pair_frames[pair_id]["name"] for pair_id in summary["initialTop3"]]
    lines = []
    lines.append("# 괴리율 상위 3종 순환 전략 백테스트")
    lines.append("")
    lines.append("## 가정")
    lines.append("")
    lines.append("- 기준 신호일: 1996-06-25")
    lines.append("- 초기자금: 10,000,000원")
    lines.append("- 초기 매수: 1996-06-25 종가 기준 괴리율 상위 3종을 다음 거래일 종가에 균등 매수")
    lines.append("- 일별 리밸런싱: 포트폴리오 외 종목이 5거래일 연속 종가 기준 괴리율 상위 3위에 들어오면 다음 거래일 종가에 교체")
    lines.append("- 교체 규칙: 신호일 기준 포트폴리오 내 괴리율 최저 종목을 매도하고, `min(계좌총액/3, 예수금)` 한도로 신규 종목을 매수")
    lines.append("- 매수/매도 수수료: 각 체결금액의 1.0%")
    lines.append("- 보유 중인 우선주 현금배당은 배당일 기준으로 예수금에 적립하고, 이후 재매수 재원으로 사용")
    lines.append("- 보유수량은 정수 주식 기준이며, 수수료 때문에 예수금이 부족하면 매수금액을 자동 축소")
    lines.append("- 데이터 공백 때문에 다음 거래일 체결가가 없으면, 해당 종목 두 개 모두 종가가 존재하는 가장 이른 다음 날짜로 실행")
    lines.append("- 동시에 여러 신규 후보가 조건을 만족하면, 신호일 기준 괴리율이 가장 높은 outsider 1종만 선택")
    lines.append("- 일별 평가액은 보유 종목의 최신 이용가능 종가를 이용해 mark-to-market")
    lines.append("- KOSPI 비교는 네이버 KOSPI 일별지수를 같은 초기 체결일에 1천만원 일시투자한 price-only buy-and-hold로 계산")
    lines.append("")
    lines.append("## 핵심 결과")
    lines.append("")
    lines.append(f"- 초기 편입 종목: {', '.join(initial_names)}")
    lines.append(f"- 초기 체결일: {summary['initialExecDate']}")
    lines.append(f"- 최종 전략 평가액: {fmt_money(summary['finalStrategyValue'])}원")
    lines.append(f"- 최종 KOSPI 비교 평가액: {fmt_money(summary['finalKospiValue'])}원")
    lines.append(f"- 전략 CAGR: {fmt_pct(summary['strategyCAGR'])}")
    lines.append(f"- KOSPI CAGR: {fmt_pct(summary['kospiCAGR'])}")
    lines.append(f"- 전략 최대낙폭: {fmt_pct(summary['strategyMaxDrawdown'])}")
    lines.append(f"- KOSPI 최대낙폭: {fmt_pct(summary['kospiMaxDrawdown'])}")
    lines.append(f"- 누적 현금배당 수취: {fmt_money(summary['totalDividendsReceived'])}원")
    lines.append(f"- 배당 이벤트 수: {summary['dividendEventCount']}회")
    lines.append(f"- 총 거래수: {summary['tradeCount']}건")
    lines.append(f"- 리밸런싱 매수 횟수: {summary['rebalanceCount']}회")
    lines.append("")
    lines.append("## 연도말 평가액")
    lines.append("")
    lines.append("| 연도 | 기준일 | 전략 평가액 | 전략 YoY | KOSPI 비교값 | KOSPI YoY | 초과수익 | 누적배당 |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for _, row in annual.iterrows():
        lines.append(
            f"| {int(row['year'])} | {row['yearEndDate'].strftime('%Y-%m-%d')} | "
            f"{fmt_money(row['strategyValue'])} | {fmt_pct(row['strategyYoY'])} | "
            f"{fmt_money(row['kospiValue'])} | {fmt_pct(row['kospiYoY'])} | {fmt_pct(row['excessYoY'])} | {fmt_money(row['cumulativeDividends'])} |"
        )
    lines.append("")
    lines.append("## 최근 10건 배당")
    lines.append("")
    if dividends.empty:
        lines.append("- 배당 수취 없음")
    else:
        tail_dividends = dividends.tail(10).copy()
        for _, row in tail_dividends.iterrows():
            lines.append(
                "- "
                + f"{row['date']} {row['pairName']} "
                + f"{int(row['shares'])}주 x {row['amountPerShare']:.2f} = {fmt_money(row['cashDividend'])}원"
            )
    lines.append("")
    lines.append("## 마지막 10건 거래")
    lines.append("")
    if trades.empty:
        lines.append("- 거래 없음")
    else:
        tail = trades.tail(10).copy()
        for _, row in tail.iterrows():
            lines.append(
                "- "
                + f"{row['date']} {row['action']} {row['pairName']} "
                + f"{int(row['shares'])}주 @ {fmt_money(row['price'])} "
                + f"(괴리율 {row['spread']:.2f}%, 수수료 {fmt_money(row['fee'])})"
            )
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    results = run_backtest()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    results["annual"].to_csv(OUTPUT_DIR / "top3_spread_strategy_annual.csv", index=False, encoding="utf-8-sig")
    results["trades"].to_csv(OUTPUT_DIR / "top3_spread_strategy_trades.csv", index=False, encoding="utf-8-sig")
    results["daily"].to_csv(OUTPUT_DIR / "top3_spread_strategy_daily.csv", index=False, encoding="utf-8-sig")
    results["dividends"].to_csv(OUTPUT_DIR / "top3_spread_strategy_dividends.csv", index=False, encoding="utf-8-sig")
    write_report(results)

    print(f"report: {REPORT_PATH}")
    print(f"annual rows: {len(results['annual'])}")
    print(f"trade rows: {len(results['trades'])}")
    print(f"final strategy: {results['summary']['finalStrategyValue']:.0f}")
    print(f"final kospi: {results['summary']['finalKospiValue']:.0f}")


if __name__ == "__main__":
    main()
