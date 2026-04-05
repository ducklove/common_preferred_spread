#!/usr/bin/env python3
"""
한국 우선주 괴리율 데이터 수집 스크립트
Yahoo Finance에서 보통주/우선주 가격 데이터를 가져와 data.js를 생성한다.

기본 실행: 기존 data.js의 마지막 날짜 이후만 가져오는 증분 갱신 모드
--full: 2000년부터 전체 데이터를 다시 다운로드
"""

import argparse
import json
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from urllib.request import Request, urlopen

import yfinance as yf
import pandas as pd

KST = timezone(timedelta(hours=9))
CONFIG_PATH = Path(__file__).parent / "config.json"
DATA_PATH = Path(__file__).parent / "data.js"
PROXY_BACKFILL_PROGRESS_PATH = Path(__file__).parent / "proxy_backfill_progress.json"
DEFAULT_NAVER_BACKFILL_PAIR_IDS = set()
DEFAULT_PROXY_BACKFILL_PAIR_IDS = set()
AUTO_NAVER_BACKFILL_START_DATE = pd.Timestamp("2005-09-29")
AUTO_NAVER_BACKFILL_END_DATE = pd.Timestamp("2010-12-31")
NAVER_HISTORY_CACHE_DIR = Path(__file__).parent / ".cache" / "naver_history"
NAVER_BACKFILL_WORKERS = 6
PROXY_HISTORY_BASE_URL = "http://cantabile.tplinkdns.com:3288"
PROXY_HISTORY_START_DATE = pd.Timestamp("1989-01-01")
PROXY_HISTORY_CACHE_DIR = Path(__file__).parent / ".cache" / "proxy_history"
PROXY_BACKFILL_WORKERS = 2
DIVIDEND_HISTORY_WORKERS = 6
SAFE_ADJUSTMENT_RATIO_MIN = 0.01
SAFE_ADJUSTMENT_RATIO_MAX = 10.0

with open(CONFIG_PATH, encoding="utf-8") as f:
    PAIRS = json.load(f)


def load_existing_data():
    """기존 data.js를 읽어 파싱한다. 파일이 없거나 파싱 실패 시 None 반환."""
    if not DATA_PATH.exists():
        return None
    try:
        content = DATA_PATH.read_text(encoding="utf-8")
        prefix = "const STOCK_DATA = "
        if not content.startswith(prefix):
            return None
        json_str = content[len(prefix):]
        if json_str.endswith(";\n"):
            json_str = json_str[:-2]
        elif json_str.endswith(";"):
            json_str = json_str[:-1]
        return json.loads(json_str)
    except (json.JSONDecodeError, ValueError):
        return None


def get_last_date(existing_data):
    """기존 데이터에서 가장 최근 날짜를 찾는다."""
    last_date = None
    for pair in existing_data.get("pairs", []):
        if pair.get("isAverage"):
            continue
        hist = pair.get("history", [])
        if hist:
            pair_last = hist[-1]["date"]
            if last_date is None or pair_last > last_date:
                last_date = pair_last
    return last_date


def get_pair_start_dates(existing_data):
    start_dates = {}
    if not existing_data:
        return start_dates
    for pair in existing_data.get("pairs", []):
        if pair.get("isAverage"):
            continue
        history = pair.get("history", [])
        if history:
            start_dates[pair["id"]] = history[0]["date"]
    return start_dates


def load_proxy_backfill_progress():
    if PROXY_BACKFILL_PROGRESS_PATH.exists():
        try:
            with open(PROXY_BACKFILL_PROGRESS_PATH, encoding="utf-8") as f:
                progress = json.load(f)
        except (OSError, json.JSONDecodeError):
            progress = {}
    else:
        progress = {}

    completed = set(progress.get("completedPairIds", []))
    completed.update(DEFAULT_PROXY_BACKFILL_PAIR_IDS)
    history = progress.get("history", [])
    return {
        "completedPairIds": sorted(completed),
        "history": history if isinstance(history, list) else [],
    }


def save_proxy_backfill_progress(progress):
    with open(PROXY_BACKFILL_PROGRESS_PATH, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def select_next_proxy_backfill_pairs(existing_data, completed_pair_ids, batch_size):
    if batch_size <= 0:
        return []

    start_dates = get_pair_start_dates(existing_data)
    candidates = [
        pair
        for pair in PAIRS
        if pair["id"] not in completed_pair_ids
    ]
    candidates.sort(
        key=lambda pair: (
            start_dates.get(pair["id"], "9999-12-31"),
            pair["name"],
        ),
        reverse=True,
    )
    return [pair["id"] for pair in candidates[:batch_size]]


def update_proxy_backfill_progress(progress, selected_pair_ids, before_starts, after_starts):
    completed = set(progress.get("completedPairIds", []))
    history = list(progress.get("history", []))
    attempted_at = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

    for pair_id in selected_pair_ids:
        after_start = after_starts.get(pair_id)
        if not after_start:
            continue
        completed.add(pair_id)
        history.append(
            {
                "pairId": pair_id,
                "attemptedAt": attempted_at,
                "beforeStart": before_starts.get(pair_id),
                "afterStart": after_start,
                "extended": before_starts.get(pair_id) != after_start,
            }
        )

    progress["completedPairIds"] = sorted(completed)
    progress["history"] = history[-200:]
    return progress


# 배당수익률 캐시 (동일 보통주 공유 종목의 중복 요청 방지)
_div_yield_cache = {}
_ticker_meta_cache = {}
_naver_meta_cache = {}
_naver_daily_history_cache = {}
_proxy_daily_history_cache = {}
_pair_yahoo_history_cache = {}
_dividend_series_cache = {}


def get_div_yield(ticker):
    if ticker not in _div_yield_cache:
        _div_yield_cache[ticker] = get_ticker_meta(ticker)["dividendYield"] or 0
    return _div_yield_cache[ticker]


def get_dividend_series(ticker):
    if ticker in _dividend_series_cache:
        return _dividend_series_cache[ticker].copy()

    empty = pd.Series(dtype="float64")

    try:
        dividends = yf.Ticker(ticker).dividends
    except Exception:
        _dividend_series_cache[ticker] = empty
        return empty.copy()

    if dividends is None or len(dividends) == 0:
        _dividend_series_cache[ticker] = empty
        return empty.copy()

    series = pd.to_numeric(dividends, errors="coerce").dropna()
    if series.empty:
        _dividend_series_cache[ticker] = empty
        return empty.copy()

    index = pd.to_datetime(series.index, errors="coerce")
    if getattr(index, "tz", None) is not None:
        index = index.tz_localize(None)
    series.index = index.normalize()
    series = series[series.index.notna()]
    series = series[series > 0]
    if series.empty:
        _dividend_series_cache[ticker] = empty
        return empty.copy()

    series = series.groupby(series.index).sum().sort_index()
    _dividend_series_cache[ticker] = series
    return series.copy()


def build_dividend_history(series, start_date_text, end_date_text):
    if series.empty:
        return []

    start_ts = pd.Timestamp(start_date_text)
    end_ts = pd.Timestamp(end_date_text)
    filtered = series[(series.index >= start_ts) & (series.index <= end_ts)]

    return [
        {
            "date": date.strftime("%Y-%m-%d"),
            "amount": round(float(amount), 4),
        }
        for date, amount in filtered.items()
    ]


def _read_fast_info_value(fast_info, *keys):
    if fast_info is None:
        return None
    for key in keys:
        try:
            if hasattr(fast_info, "get"):
                value = fast_info.get(key)
            else:
                value = fast_info[key]
        except Exception:
            value = None
        if value is not None:
            return value
    return None


def parse_number_text(value):
    digits = "".join(ch for ch in str(value or "") if ch.isdigit() or ch == ",")
    if not digits:
        return None
    return int(digits.replace(",", ""))


def extract_naver_row_value(html, label):
    marker = f'<th scope="row">{label}</th>'
    marker_idx = html.find(marker)
    if marker_idx == -1:
        return None
    row_end = html.find("</tr>", marker_idx)
    if row_end == -1:
        row_end = marker_idx + 400
    row_html = html[marker_idx:row_end]
    em_start = row_html.find("<em")
    if em_start == -1:
        return None
    text_start = row_html.find(">", em_start)
    text_end = row_html.find("</em>", text_start)
    if text_start == -1 or text_end == -1:
        return None
    return parse_number_text(row_html[text_start + 1:text_end])


def get_naver_ticker_meta(ticker):
    if ticker in _naver_meta_cache:
        return _naver_meta_cache[ticker]

    meta = {
        "marketCap": None,
        "sharesOutstanding": None,
    }

    code = ticker.split(".")[0]
    if not code:
        _naver_meta_cache[ticker] = meta
        return meta

    try:
        request = Request(
            f"https://finance.naver.com/item/main.naver?code={code}",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urlopen(request, timeout=10) as response:
            html = response.read().decode("utf-8", errors="replace")
    except Exception:
        _naver_meta_cache[ticker] = meta
        return meta

    market_cap_eok = extract_naver_row_value(html, "시가총액")
    shares_outstanding = extract_naver_row_value(html, "상장주식수")
    if market_cap_eok is not None:
        meta["marketCap"] = market_cap_eok * 100_000_000
    if shares_outstanding is not None:
        meta["sharesOutstanding"] = shares_outstanding

    _naver_meta_cache[ticker] = meta
    return meta


def get_ticker_meta(ticker):
    if ticker in _ticker_meta_cache:
        return _ticker_meta_cache[ticker]

    meta = {
        "dividendYield": 0,
        "marketCap": None,
        "sharesOutstanding": None,
    }

    try:
        yf_ticker = yf.Ticker(ticker)
        info = yf_ticker.info or {}
    except Exception:
        yf_ticker = None
        info = {}

    fast_info = None
    if yf_ticker is not None:
        try:
            fast_info = yf_ticker.fast_info
        except Exception:
            fast_info = None

    naver_meta = get_naver_ticker_meta(ticker)

    meta["dividendYield"] = info.get("dividendYield") or 0
    meta["marketCap"] = (
        naver_meta["marketCap"]
        or info.get("marketCap")
        or _read_fast_info_value(fast_info, "marketCap", "market_cap")
    )
    meta["sharesOutstanding"] = (
        naver_meta["sharesOutstanding"]
        or info.get("sharesOutstanding")
        or _read_fast_info_value(fast_info, "sharesOutstanding", "shares", "shares_outstanding")
    )

    _ticker_meta_cache[ticker] = meta
    return meta


def fetch_naver_daily_history(ticker):
    if ticker in _naver_daily_history_cache:
        return _naver_daily_history_cache[ticker].copy()

    code = ticker.split(".")[0]
    if not code:
        empty = pd.DataFrame(columns=["close", "volume"])
        _naver_daily_history_cache[ticker] = empty
        return empty.copy()

    cache_path = NAVER_HISTORY_CACHE_DIR / f"{code}.csv"
    if cache_path.exists():
        try:
            cached = pd.read_csv(cache_path, index_col=0, parse_dates=True)
            if {"close", "volume"}.issubset(cached.columns):
                _naver_daily_history_cache[ticker] = cached
                return cached.copy()
        except Exception:
            pass

    def fetch_html(page):
        request = Request(
            f"https://finance.naver.com/item/sise_day.naver?code={code}&page={page}",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urlopen(request, timeout=10) as response:
            return response.read().decode("euc-kr", errors="replace")

    try:
        first_html = fetch_html(1)
    except Exception:
        empty = pd.DataFrame(columns=["close", "volume"])
        _naver_daily_history_cache[ticker] = empty
        return empty.copy()

    match = re.search(r'pgRR.*?page=(\d+)', first_html, re.S)
    last_page = int(match.group(1)) if match else 1

    frames = []
    for page in range(1, last_page + 1):
        html = first_html if page == 1 else fetch_html(page)
        try:
            table = pd.read_html(StringIO(html))[0]
        except ValueError:
            continue

        if table.shape[1] < 7:
            continue

        table = table.iloc[:, [0, 1, 6]].copy()
        table.columns = ["date", "close", "volume"]
        table = table.dropna(subset=["date", "close", "volume"])
        if table.empty:
            continue

        page_df = pd.DataFrame(
            {
                "close": pd.to_numeric(table["close"], errors="coerce").to_numpy(),
                "volume": pd.to_numeric(table["volume"], errors="coerce").to_numpy(),
            },
            index=pd.to_datetime(table["date"], format="%Y.%m.%d", errors="coerce").to_numpy(),
        ).dropna(subset=["close", "volume"])

        if page_df.empty:
            continue
        frames.append(page_df)

    if frames:
        history = pd.concat(frames).sort_index()
        history = history[~history.index.duplicated(keep="first")]
        NAVER_HISTORY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        history.to_csv(cache_path, encoding="utf-8")
    else:
        history = pd.DataFrame(columns=["close", "volume"])

    _naver_daily_history_cache[ticker] = history
    return history.copy()


def fetch_proxy_daily_history(ticker):
    if ticker in _proxy_daily_history_cache:
        return _proxy_daily_history_cache[ticker].copy()

    code = ticker.split(".")[0]
    if not code:
        empty = pd.DataFrame(columns=["close", "volume"])
        _proxy_daily_history_cache[ticker] = empty
        return empty.copy()

    cache_path = PROXY_HISTORY_CACHE_DIR / f"{code}.csv"
    if cache_path.exists():
        try:
            cached = pd.read_csv(cache_path, index_col=0, parse_dates=True)
            if {"close", "volume"}.issubset(cached.columns):
                _proxy_daily_history_cache[ticker] = cached
                return cached.copy()
        except Exception:
            pass

    history_rows = []
    seen_dates = set()
    cursor_end = datetime.now(KST).date()
    start_date = PROXY_HISTORY_START_DATE.date()

    try:
        while cursor_end >= start_date:
            url = (
                f"{PROXY_HISTORY_BASE_URL}/v1/stocks/{code}/history"
                f"?start_date={start_date.isoformat()}"
                f"&end_date={cursor_end.isoformat()}"
                f"&period=D&adjusted=true"
            )
            request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(request, timeout=20) as response:
                payload = json.loads(response.read().decode("utf-8", errors="replace"))

            items = payload.get("items", [])
            if not items:
                break

            oldest_date = None
            batch_count = 0
            for item in items:
                date_text = item.get("stck_bsop_date")
                if not date_text or date_text in seen_dates:
                    continue
                seen_dates.add(date_text)
                history_rows.append(
                    {
                        "date": pd.to_datetime(date_text, format="%Y%m%d", errors="coerce"),
                        "close": pd.to_numeric(item.get("stck_clpr"), errors="coerce"),
                        "volume": pd.to_numeric(item.get("acml_vol"), errors="coerce"),
                    }
                )
                oldest_date = date_text
                batch_count += 1

            if batch_count == 0 or oldest_date is None:
                break

            oldest_dt = datetime.strptime(oldest_date, "%Y%m%d").date()
            next_end = oldest_dt - timedelta(days=1)
            if next_end >= cursor_end:
                break
            cursor_end = next_end
    except Exception:
        if cache_path.exists():
            try:
                cached = pd.read_csv(cache_path, index_col=0, parse_dates=True)
                if {"close", "volume"}.issubset(cached.columns):
                    _proxy_daily_history_cache[ticker] = cached
                    return cached.copy()
            except Exception:
                pass
        empty = pd.DataFrame(columns=["close", "volume"])
        _proxy_daily_history_cache[ticker] = empty
        return empty.copy()

    if history_rows:
        history = pd.DataFrame(history_rows).dropna(subset=["date", "close", "volume"])
        history = history.set_index("date").sort_index()
        history = history[~history.index.duplicated(keep="first")]
        PROXY_HISTORY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        history.to_csv(cache_path, encoding="utf-8")
    else:
        history = pd.DataFrame(columns=["close", "volume"])

    _proxy_daily_history_cache[ticker] = history
    return history.copy()


def merge_external_backfill(yahoo_close, yahoo_vol, external_history, ticker, source_name, enabled=False):
    if not enabled or yahoo_close.empty:
        return yahoo_close, yahoo_vol, None

    if external_history.empty:
        return yahoo_close, yahoo_vol, None

    overlap_dates = yahoo_close.index.intersection(external_history.index)
    if overlap_dates.empty:
        return yahoo_close, yahoo_vol, None

    overlap_dates = overlap_dates.sort_values()[:20]
    overlap_ratios = (
        yahoo_close.loc[overlap_dates] / external_history.loc[overlap_dates, "close"]
    ).replace([float("inf"), float("-inf")], pd.NA).dropna()

    adjustment_ratio = float(overlap_ratios.median()) if not overlap_ratios.empty else 1.0
    if adjustment_ratio <= 0:
        adjustment_ratio = 1.0

    earliest_yahoo = yahoo_close.index.min()
    earlier_history = external_history[external_history.index < earliest_yahoo].copy()
    if earlier_history.empty:
        return yahoo_close, yahoo_vol, None

    if (
        adjustment_ratio < SAFE_ADJUSTMENT_RATIO_MIN
        or adjustment_ratio > SAFE_ADJUSTMENT_RATIO_MAX
    ):
        info = {
            "source": source_name,
            "ticker": ticker,
            "earliestYahoo": earliest_yahoo.strftime("%Y-%m-%d"),
            "earliestNaver": earlier_history.index.min().strftime("%Y-%m-%d"),
            "adjustmentRatio": adjustment_ratio,
            "skipped": True,
        }
        return yahoo_close, yahoo_vol, info

    earlier_history["close"] = earlier_history["close"] * adjustment_ratio
    if adjustment_ratio != 0:
        earlier_history["volume"] = earlier_history["volume"] / adjustment_ratio

    merged_close = pd.concat([earlier_history["close"], yahoo_close]).sort_index()
    merged_close = merged_close[~merged_close.index.duplicated(keep="last")]

    merged_vol = pd.concat([earlier_history["volume"], yahoo_vol]).sort_index()
    merged_vol = merged_vol[~merged_vol.index.duplicated(keep="last")]

    info = {
        "source": source_name,
        "ticker": ticker,
        "earliestYahoo": earliest_yahoo.strftime("%Y-%m-%d"),
        "earliestNaver": earlier_history.index.min().strftime("%Y-%m-%d"),
        "adjustmentRatio": adjustment_ratio,
    }
    return merged_close, merged_vol, info


def merge_naver_backfill(yahoo_close, yahoo_vol, ticker, enabled=False):
    return merge_external_backfill(
        yahoo_close,
        yahoo_vol,
        fetch_naver_daily_history(ticker),
        ticker,
        "naver",
        enabled=enabled,
    )


def merge_proxy_backfill(yahoo_close, yahoo_vol, ticker, enabled=False):
    return merge_external_backfill(
        yahoo_close,
        yahoo_vol,
        fetch_proxy_daily_history(ticker),
        ticker,
        "proxy",
        enabled=enabled,
    )


def fetch_full_yahoo_pair_history(pair, end_date):
    cache_key = (pair["id"], end_date.strftime("%Y-%m-%d"))
    if cache_key in _pair_yahoo_history_cache:
        return _pair_yahoo_history_cache[cache_key]

    pair_data = yf.download(
        [pair["commonTicker"], pair["preferredTicker"]],
        start="2000-01-01",
        end=end_date.strftime("%Y-%m-%d"),
        auto_adjust=False,
        progress=False,
    )
    close = pair_data["Close"]
    volume = pair_data["Volume"]
    _pair_yahoo_history_cache[cache_key] = (close, volume)
    return close, volume


def determine_naver_backfill_targets(close, explicit_pair_ids):
    target_pair_ids = set(explicit_pair_ids)
    target_tickers = set()
    reasons = {}

    for pair in PAIRS:
        ct = pair["commonTicker"]
        pt = pair["preferredTicker"]
        common_close = close[ct].dropna()
        preferred_close = close[pt].dropna()
        if common_close.empty or preferred_close.empty:
            continue

        pair_first_yahoo = max(common_close.index.min(), preferred_close.index.min())
        auto_target = (
            AUTO_NAVER_BACKFILL_START_DATE
            <= pair_first_yahoo
            <= AUTO_NAVER_BACKFILL_END_DATE
        )
        if pair["id"] in explicit_pair_ids or auto_target:
            target_pair_ids.add(pair["id"])
            target_tickers.add(ct)
            target_tickers.add(pt)
            reasons[pair["id"]] = pair_first_yahoo.strftime("%Y-%m-%d")

    return target_pair_ids, sorted(target_tickers), reasons


def prefetch_naver_histories(tickers):
    tickers = sorted(set(tickers))
    if not tickers:
        return

    workers = min(NAVER_BACKFILL_WORKERS, len(tickers))
    print(f"네이버 백필 병렬 수집: {len(tickers)}개 티커, {workers}개 워커")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(fetch_naver_daily_history, ticker): ticker
            for ticker in tickers
        }
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                history = future.result()
                print(f"  NAVER {ticker}: {len(history)}일")
            except Exception as exc:
                print(f"  WARNING: NAVER {ticker} 수집 실패 ({exc})")


def prefetch_proxy_histories(tickers):
    tickers = sorted(set(tickers))
    if not tickers:
        return

    workers = min(PROXY_BACKFILL_WORKERS, len(tickers))
    print(f"프록시 백필 병렬 수집: {len(tickers)}개 티커, {workers}개 워커")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(fetch_proxy_daily_history, ticker): ticker
            for ticker in tickers
        }
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                history = future.result()
                print(f"  PROXY {ticker}: {len(history)}일")
            except Exception as exc:
                print(f"  WARNING: PROXY {ticker} 수집 실패 ({exc})")


def prefetch_dividend_histories(tickers):
    tickers = sorted(set(tickers))
    if not tickers:
        return

    workers = min(DIVIDEND_HISTORY_WORKERS, len(tickers))
    print(f"배당 히스토리 병렬 수집: {len(tickers)}개 티커, {workers}개 워커")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(get_dividend_series, ticker): ticker
            for ticker in tickers
        }
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                series = future.result()
                print(f"  DIV {ticker}: {len(series)}건")
            except Exception as exc:
                print(f"  WARNING: DIV {ticker} 수집 실패 ({exc})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="전체 데이터 다시 다운로드")
    parser.add_argument(
        "--naver-backfill",
        nargs="*",
        default=None,
        help="네이버 일별 시세로 과거 구간을 백필할 pair id 목록",
    )
    parser.add_argument(
        "--proxy-backfill",
        nargs="*",
        default=None,
        help="프록시 API 일별 시세로 과거 구간을 백필할 pair id 목록",
    )
    parser.add_argument(
        "--auto-proxy-backfill-batch-size",
        type=int,
        default=0,
        help="완료되지 않은 다음 pair를 자동 선택해 프록시 백필할 개수",
    )
    args = parser.parse_args()

    explicit_naver_backfill_pair_ids = set(DEFAULT_NAVER_BACKFILL_PAIR_IDS)
    if args.naver_backfill is not None:
        explicit_naver_backfill_pair_ids.update(args.naver_backfill)
    explicit_proxy_backfill_pair_ids = set(DEFAULT_PROXY_BACKFILL_PAIR_IDS)
    if args.proxy_backfill is not None:
        explicit_proxy_backfill_pair_ids.update(args.proxy_backfill)
    existing_data = None if args.full else load_existing_data()
    proxy_backfill_progress = load_proxy_backfill_progress()
    if args.full:
        explicit_proxy_backfill_pair_ids.update(proxy_backfill_progress["completedPairIds"])
    auto_proxy_backfill_pair_ids = select_next_proxy_backfill_pairs(
        existing_data,
        set(proxy_backfill_progress["completedPairIds"]),
        args.auto_proxy_backfill_batch_size,
    )
    explicit_proxy_backfill_pair_ids.update(auto_proxy_backfill_pair_ids)
    existing_pair_ids = set()
    if existing_data:
        existing_pair_ids = {
            pair["id"]
            for pair in existing_data.get("pairs", [])
            if not pair.get("isAverage")
        }

    # 모든 티커 수집 (중복 제거) + KOSPI 지수
    KOSPI_TICKER = "^KS11"
    all_tickers = list(
        dict.fromkeys(
            ticker
            for pair in PAIRS
            for ticker in [pair["commonTicker"], pair["preferredTicker"]]
        )
    )
    all_tickers.append(KOSPI_TICKER)

    end_date = datetime.now()
    configured_pair_ids = {pair["id"] for pair in PAIRS if not pair.get("isAverage")}
    missing_pair_ids = sorted(configured_pair_ids - existing_pair_ids)

    if existing_data:
        if missing_pair_ids:
            start_date = datetime(2000, 1, 1)
            print(
                "신규 종목 감지, 전체 백필 모드: "
                + ", ".join(missing_pair_ids)
            )
            existing_data = None
        else:
            last_date_str = get_last_date(existing_data)
            if last_date_str:
                # 마지막 날짜에서 5일 전부터 가져와서 안전하게 겹침 처리
                start_date = datetime.strptime(last_date_str, "%Y-%m-%d") - timedelta(days=5)
                print(f"증분 갱신 모드: {start_date.strftime('%Y-%m-%d')}부터 가져옵니다")
            else:
                start_date = datetime(2000, 1, 1)
                print("기존 히스토리 없음, 전체 다운로드")
                existing_data = None
    else:
        start_date = datetime(2000, 1, 1)
        print("전체 다운로드 모드")

    print(f"{len(all_tickers)}개 티커 다운로드 중...")
    print(f"기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")

    # 일괄 다운로드 (비조정 종가 사용)
    data = yf.download(
        all_tickers,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        auto_adjust=False,
        progress=True,
    )

    close = data["Close"]
    volume = data["Volume"]

    naver_backfill_pair_ids, naver_backfill_tickers, naver_backfill_reasons = (
        determine_naver_backfill_targets(close, explicit_naver_backfill_pair_ids)
    )
    proxy_backfill_pair_ids = set(explicit_proxy_backfill_pair_ids)
    proxy_backfill_tickers = sorted(
        {
            ticker
            for pair in PAIRS
            if pair["id"] in proxy_backfill_pair_ids
            for ticker in [pair["commonTicker"], pair["preferredTicker"]]
        }
    )
    before_pair_start_dates = get_pair_start_dates(existing_data)
    if proxy_backfill_pair_ids:
        if auto_proxy_backfill_pair_ids:
            print("자동 프록시 백필 선택: " + ", ".join(auto_proxy_backfill_pair_ids))
        naver_backfill_pair_ids = {
            pair_id for pair_id in naver_backfill_pair_ids if pair_id not in proxy_backfill_pair_ids
        }
        naver_backfill_reasons = {
            pair_id: reason
            for pair_id, reason in naver_backfill_reasons.items()
            if pair_id not in proxy_backfill_pair_ids
        }
        naver_backfill_tickers = [
            ticker for ticker in naver_backfill_tickers if ticker not in proxy_backfill_tickers
        ]
        print("프록시 백필 대상 pair: " + ", ".join(sorted(proxy_backfill_pair_ids)))
        prefetch_proxy_histories(proxy_backfill_tickers)
    if naver_backfill_pair_ids:
        print(
            "네이버 백필 대상 pair: "
            + ", ".join(
                f"{pair_id}({naver_backfill_reasons.get(pair_id, 'explicit')})"
                for pair_id in sorted(naver_backfill_pair_ids)
            )
        )
        prefetch_naver_histories(naver_backfill_tickers)

    # 기존 데이터 맵 (증분 모드용)
    prefetch_dividend_histories(
        [
            ticker
            for pair in PAIRS
            for ticker in [pair["commonTicker"], pair["preferredTicker"]]
        ]
    )

    existing_pairs_map = {}
    existing_kospi = {}
    if existing_data:
        for p in existing_data["pairs"]:
            if p.get("isAverage"):
                for h in p.get("history", []):
                    if "kospiPrice" in h:
                        existing_kospi[h["date"]] = h["kospiPrice"]
            else:
                existing_pairs_map[p["id"]] = p

    # 각 페어별로 괴리율 계산
    pairs_result = []
    dividend_histories = {}

    for pair in PAIRS:
        ct = pair["commonTicker"]
        pt = pair["preferredTicker"]
        apply_naver_backfill = pair["id"] in naver_backfill_pair_ids
        apply_proxy_backfill = pair["id"] in proxy_backfill_pair_ids

        # 거래정지일(volume=0) 제외
        if apply_proxy_backfill and not args.full:
            pair_close, pair_volume = fetch_full_yahoo_pair_history(pair, end_date)
            common_close = pair_close[ct].dropna()
            preferred_close = pair_close[pt].dropna()
            common_vol = pair_volume[ct].fillna(0)
            preferred_vol = pair_volume[pt].fillna(0)
        else:
            common_close = close[ct].dropna()
            preferred_close = close[pt].dropna()
            common_vol = volume[ct].fillna(0)
            preferred_vol = volume[pt].fillna(0)

        common_close, common_vol, common_backfill = merge_proxy_backfill(
            common_close,
            common_vol,
            ct,
            enabled=apply_proxy_backfill,
        )
        preferred_close, preferred_vol, preferred_backfill = merge_proxy_backfill(
            preferred_close,
            preferred_vol,
            pt,
            enabled=apply_proxy_backfill,
        )
        if common_backfill is None and (apply_naver_backfill or apply_proxy_backfill):
            common_close, common_vol, common_backfill = merge_naver_backfill(
                common_close,
                common_vol,
                ct,
                enabled=True,
            )
        if preferred_backfill is None and (apply_naver_backfill or apply_proxy_backfill):
            preferred_close, preferred_vol, preferred_backfill = merge_naver_backfill(
                preferred_close,
                preferred_vol,
                pt,
                enabled=True,
            )

        if common_backfill or preferred_backfill:
            common_msg = (
                (
                    f"{common_backfill['source']} 보통주 스킵 {common_backfill['earliestYahoo']} -> {common_backfill['earliestNaver']} "
                    f"(x{common_backfill['adjustmentRatio']:.6f})"
                    if common_backfill.get("skipped")
                    else f"{common_backfill['source']} 보통주 {common_backfill['earliestYahoo']} -> {common_backfill['earliestNaver']} "
                    f"(x{common_backfill['adjustmentRatio']:.6f})"
                )
                if common_backfill
                else "보통주 변화 없음"
            )
            preferred_msg = (
                (
                    f"{preferred_backfill['source']} 우선주 스킵 {preferred_backfill['earliestYahoo']} -> {preferred_backfill['earliestNaver']} "
                    f"(x{preferred_backfill['adjustmentRatio']:.6f})"
                    if preferred_backfill.get("skipped")
                    else f"{preferred_backfill['source']} 우선주 {preferred_backfill['earliestYahoo']} -> {preferred_backfill['earliestNaver']} "
                    f"(x{preferred_backfill['adjustmentRatio']:.6f})"
                )
                if preferred_backfill
                else "우선주 변화 없음"
            )
            print(f"  INFO: {pair['name']} 네이버 백필 {common_msg}, {preferred_msg}")

        # 두 시리즈의 공통 날짜만 사용
        common_dates = common_close.index.intersection(preferred_close.index)
        if len(common_dates) == 0:
            print(f"  WARNING: {pair['name']} 겹치는 날짜 없음, 건너뜀")
            continue

        # 양쪽 모두 거래가 있는 날짜만 사용
        traded = (common_vol.loc[common_dates] > 0) & (preferred_vol.loc[common_dates] > 0)
        common_dates = common_dates[traded]
        if len(common_dates) == 0:
            print(f"  WARNING: {pair['name']} 거래일 없음, 건너뜀")
            continue

        c = common_close.loc[common_dates]
        p = preferred_close.loc[common_dates]

        # 괴리율: (보통주 - 우선주) / 보통주 * 100
        spread = (c - p) / c * 100

        # Yahoo Finance 소급 조정 오류 필터 (괴리율 -100% 미만은 불가능한 값)
        valid = spread > -100
        if not valid.all():
            n_removed = (~valid).sum()
            print(f"  WARNING: {pair['name']}: Yahoo 조정 오류 {n_removed}일 제외")
            common_dates = common_dates[valid]
            c = c.loc[common_dates]
            p = p.loc[common_dates]
            spread = spread.loc[common_dates]

        # 새로 다운로드한 히스토리
        new_history = []
        for date in common_dates:
            new_history.append(
                {
                    "date": date.strftime("%Y-%m-%d"),
                    "commonPrice": round(float(c.loc[date]), 0),
                    "preferredPrice": round(float(p.loc[date]), 0),
                    "spread": round(float(spread.loc[date]), 2),
                }
            )

        # 증분 모드: 기존 히스토리와 병합
        if pair["id"] in existing_pairs_map and new_history:
            existing_hist = existing_pairs_map[pair["id"]]["history"]
            first_new_date = new_history[0]["date"]
            kept = [h for h in existing_hist if h["date"] < first_new_date]
            history = kept + new_history
        else:
            history = new_history

        if not history:
            continue

        # 현재 (마지막 거래일) 정보
        latest = history[-1]
        prev = history[-2] if len(history) >= 2 else latest
        spread_change = round(latest["spread"] - prev["spread"], 2)

        # 일간 등락률
        if len(history) >= 2:
            prev_cp = prev["commonPrice"]
            prev_pp = prev["preferredPrice"]
            common_change = round((latest["commonPrice"] - prev_cp) / prev_cp * 100, 2) if prev_cp else 0
            preferred_change = round((latest["preferredPrice"] - prev_pp) / prev_pp * 100, 2) if prev_pp else 0
        else:
            common_change = 0
            preferred_change = 0

        # 배당수익률 조회
        common_meta = get_ticker_meta(ct)
        preferred_meta = get_ticker_meta(pt)
        c_dy = get_div_yield(ct)
        p_dy = get_div_yield(pt)

        history_start_date = history[0]["date"]
        history_end_date = history[-1]["date"]
        dividend_histories[pair["id"]] = {
            "startDate": history_start_date,
            "endDate": history_end_date,
            "commonTicker": ct,
            "preferredTicker": pt,
            "commonName": pair["commonName"],
            "preferredName": pair["preferredName"],
            "common": build_dividend_history(
                get_dividend_series(ct),
                history_start_date,
                history_end_date,
            ),
            "preferred": build_dividend_history(
                get_dividend_series(pt),
                history_start_date,
                history_end_date,
            ),
        }

        pair_data = {
            "id": pair["id"],
            "name": pair["name"],
            "commonName": pair["commonName"],
            "preferredName": pair["preferredName"],
            "current": {
                "commonPrice": latest["commonPrice"],
                "preferredPrice": latest["preferredPrice"],
                "spread": latest["spread"],
                "spreadChange": spread_change,
                "commonChange": common_change,
                "preferredChange": preferred_change,
                "commonDivYield": round(c_dy, 2),
                "preferredDivYield": round(p_dy, 2),
                "commonMarketCap": common_meta["marketCap"],
                "preferredMarketCap": preferred_meta["marketCap"],
                "commonSharesOutstanding": common_meta["sharesOutstanding"],
                "preferredSharesOutstanding": preferred_meta["sharesOutstanding"],
            },
            "history": history,
        }
        pairs_result.append(pair_data)

        print(
            f"  {pair['name']}: {len(history)}일, "
            f"현재 괴리율 {latest['spread']:.2f}% "
            f"({'↑' if spread_change > 0 else '↓'}{abs(spread_change):.2f}%p) "
            f"배당: {pair_data['current']['commonDivYield']:.1f}%/{pair_data['current']['preferredDivYield']:.1f}%"
        )

    # KOSPI 지수 데이터 준비
    kospi_close = close[KOSPI_TICKER].dropna()

    # 그룹(commonName)당 최고 괴리율 pair만 선택하여 평균 계산
    rep_pairs = {}
    for pair_data in pairs_result:
        cn = pair_data["commonName"]
        if cn not in rep_pairs or pair_data["current"]["spread"] > rep_pairs[cn]["current"]["spread"]:
            rep_pairs[cn] = pair_data
    rep_pairs_list = list(rep_pairs.values())

    # 일별 전체 평균 괴리율 계산
    daily_spreads = defaultdict(list)
    for pair_data in rep_pairs_list:
        for h in pair_data["history"]:
            daily_spreads[h["date"]].append(h["spread"])

    avg_history = []
    n_pairs = len(rep_pairs_list)
    for date in sorted(daily_spreads.keys()):
        spreads = daily_spreads[date]
        # 종목 수가 절반 미만인 날은 휴장일 오류 데이터이므로 제외
        if len(spreads) < n_pairs / 2:
            continue
        # KOSPI: 새 데이터 우선, 없으면 기존 데이터 사용
        ts = pd.Timestamp(date)
        if ts in kospi_close.index:
            kospi_price = round(float(kospi_close.loc[ts]), 2)
        elif date in existing_kospi:
            kospi_price = existing_kospi[date]
        else:
            kospi_price = None
        entry = {
            "date": date,
            "commonPrice": 0,
            "preferredPrice": 0,
            "spread": round(sum(spreads) / len(spreads), 2),
        }
        if kospi_price is not None:
            entry["kospiPrice"] = kospi_price
        avg_history.append(entry)

    if avg_history:
        latest_avg = avg_history[-1]
        prev_avg = avg_history[-2] if len(avg_history) >= 2 else latest_avg
        avg_change = round(latest_avg["spread"] - prev_avg["spread"], 2)
        avg_pair = {
            "id": "_average",
            "name": "전체 평균",
            "commonName": "",
            "preferredName": "",
            "isAverage": True,
            "current": {
                "commonPrice": 0,
                "preferredPrice": 0,
                "spread": latest_avg["spread"],
                "spreadChange": avg_change,
            },
            "history": avg_history,
        }
        print(
            f"  전체 평균: {len(avg_history)}일, "
            f"현재 괴리율 {latest_avg['spread']:.2f}% "
            f"({'↑' if avg_change > 0 else '↓'}{abs(avg_change):.2f}%p)"
        )

    # 전체 평균도 포함하여 괴리율 높은 순 정렬
    if avg_history:
        pairs_result.append(avg_pair)
    pairs_result.sort(key=lambda p: p["current"]["spread"], reverse=True)

    if auto_proxy_backfill_pair_ids:
        after_pair_start_dates = {
            pair["id"]: pair["history"][0]["date"]
            for pair in pairs_result
            if not pair.get("isAverage") and pair.get("history")
        }
        proxy_backfill_progress = update_proxy_backfill_progress(
            proxy_backfill_progress,
            auto_proxy_backfill_pair_ids,
            before_pair_start_dates,
            after_pair_start_dates,
        )
        save_proxy_backfill_progress(proxy_backfill_progress)

    # data.js 출력
    stock_data = {
        "lastUpdated": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "dividendHistories": dividend_histories,
        "pairs": pairs_result,
    }

    js_content = "const STOCK_DATA = " + json.dumps(stock_data, ensure_ascii=False, indent=2) + ";\n"

    with open(DATA_PATH, "w", encoding="utf-8") as f:
        f.write(js_content)

    print(f"\n{DATA_PATH} 생성 완료 ({len(pairs_result)}개 종목, {len(js_content)} bytes)")


if __name__ == "__main__":
    main()
