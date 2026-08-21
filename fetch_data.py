#!/usr/bin/env python3
"""
한국 우선주 괴리율 데이터 수집 스크립트
Yahoo Finance에서 보통주/우선주 가격 데이터를 가져와 data.js를 생성한다.

기본 실행: 기존 data.js의 마지막 날짜 이후만 가져오는 증분 갱신 모드
--full: 2000년부터 전체 데이터를 다시 다운로드
"""

import argparse
import json
import math
import os
import re
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import yfinance as yf
import pandas as pd

import attractiveness
import data_writer
import dividend_sources
import history_rules

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
PROXY_HISTORY_BASE_URL = os.environ.get("PROXY_HISTORY_BASE_URL", "").strip()
PROXY_HISTORY_START_DATE = pd.Timestamp("1989-01-01")
PROXY_HISTORY_CACHE_DIR = Path(__file__).parent / ".cache" / "proxy_history"
PROXY_BACKFILL_WORKERS = 2
PROXY_HISTORY_WINDOW_DAYS = 730
PROXY_HISTORY_TIMEOUT_SECONDS = 60
PROXY_HISTORY_RETRIES = 3
def getenv_nonempty(name, default=""):
    value = os.environ.get(name, "").strip()
    return value if value else default


INTERNAL_PRICE_API_ENABLED = getenv_nonempty("INTERNAL_PRICE_API", "1").lower() not in {
    "0",
    "false",
    "no",
}
INTERNAL_PRICE_API_BASE_URL = getenv_nonempty(
    "INTERNAL_PRICE_API_BASE_URL",
    "http://192.168.68.84:8400",
).rstrip("/")


def default_internal_api_url(env_name, path):
    if not INTERNAL_PRICE_API_ENABLED:
        return ""
    return getenv_nonempty(env_name, f"{INTERNAL_PRICE_API_BASE_URL}{path}")


INTERNAL_CLOSE_API_URL = default_internal_api_url("INTERNAL_CLOSE_API_URL", "/api/prices/close")
INTERNAL_DAILY_API_URL = default_internal_api_url("INTERNAL_DAILY_API_URL", "/api/prices/daily")
INTERNAL_INDICES_API_URL = default_internal_api_url("INTERNAL_INDICES_API_URL", "/api/macro/indices")
INTERNAL_DIVIDENDS_API_URL = default_internal_api_url("INTERNAL_DIVIDENDS_API_URL", "/api/fundamentals/dividends")
INTERNAL_CLOSE_TIMEOUT_SECONDS = 10
INTERNAL_PRICE_TIMEOUT_SECONDS = 30
INTERNAL_PRICE_HEALTH_TIMEOUT_SECONDS = 8
INTERNAL_PRICE_MAX_DAYS = 3650
DIVIDEND_HISTORY_WORKERS = 6
# 증분 창을 가장 뒤처진 종목 기준으로 잡되, 영구 거래정지 종목이 창을 무한정
# 끌어내리지 않도록 최신 종목 기준 하한을 둔다.
INCREMENTAL_OVERLAP_DAYS = 5
INCREMENTAL_MAX_LOOKBACK_DAYS = 90
# 전체 최신일보다 이만큼 뒤처진 종목은 경고로 알린다 (거래정지/상장폐지 후보).
STALE_PAIR_WARN_DAYS = 14
SAFE_ADJUSTMENT_RATIO_MIN = 0.01
SAFE_ADJUSTMENT_RATIO_MAX = 10.0
AVG_TRADED_VALUE_WINDOW = 20
GOOGLE_SHEET_DIVIDEND_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1RKLAARnfVNsLKBxyXfdjHhw7AUE1Wv91OrEm868Q3Z8/gviz/tq?tqx=out:csv&sheet=Data"
)
GOOGLE_SHEET_DIVIDEND_CACHE_PATH = Path(__file__).parent / ".cache" / "google_sheet" / "dividend_data.csv"
DIVIDEND_AMOUNT_OVERRIDES = {
    ("019680.KS", "019685.KS"): {
        "preferredDividendPerShare": 60.0,
    },
}

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


def get_pair_last_dates(existing_data):
    """pair id -> 마지막 히스토리 날짜 (평균 페어 제외)."""
    last_dates = {}
    for pair in (existing_data or {}).get("pairs", []):
        if pair.get("isAverage"):
            continue
        hist = pair.get("history", [])
        if hist:
            last_dates[pair["id"]] = hist[-1]["date"]
    return last_dates


def incremental_start(
    last_dates,
    overlap_days=INCREMENTAL_OVERLAP_DAYS,
    max_lookback_days=INCREMENTAL_MAX_LOOKBACK_DAYS,
):
    """증분 수집 시작일을 계산한다.

    기준은 '가장 뒤처진 종목'의 마지막 날짜(-overlap_days)다. 예전에는 가장 앞선
    종목을 기준으로 삼았는데, 그러면 한 종목이 거래정지 등으로 하루라도 뒤처지는
    순간 다음 실행의 수집 창이 그 종목의 마지막 날짜보다 뒤로 밀려서 영원히
    따라잡지 못하는 자기강화 결함이 있었다 (2026-08 한화 거래정지 사고).

    영구 정지 종목이 창을 무한정 끌어내리지 않도록 최신 종목 기준 하한을 둔다.
    """
    newest = datetime.strptime(max(last_dates), "%Y-%m-%d")
    oldest = datetime.strptime(min(last_dates), "%Y-%m-%d")
    return max(oldest - timedelta(days=overlap_days), newest - timedelta(days=max_lookback_days))


def carry_forward_missing_pairs(pairs_result, pairs_config, previous_data, dividend_histories):
    """이번 회차에 결과가 없는 종목을 직전 기록으로 이어붙이고 그 id 집합을 반환한다.

    거래정지·상장폐지·소스 장애로 한 종목의 신규 데이터가 비어도, 그 종목만
    직전 기록으로 넘어가고 나머지 종목의 갱신은 계속되게 한다. 예전에는 이런
    종목이 결과에서 통째로 빠져 아래 품질 가드가 전체 실행을 중단시켰고,
    그 결과 60개 전 종목의 히스토리가 함께 멈췄다 (2026-08 한화 거래정지 사고).

    config에서 제거된 종목은 의도적 삭제이므로 되살리지 않는다.
    """
    previous_pairs_map = {
        p["id"]: p
        for p in (previous_data or {}).get("pairs", [])
        if p.get("id") and not p.get("isAverage")
    }
    previous_dividend_histories = (previous_data or {}).get("dividendHistories") or {}
    processed_pair_ids = {p["id"] for p in pairs_result}

    carried_pair_ids = set()
    for pair in pairs_config:
        pair_id = pair["id"]
        if pair_id in processed_pair_ids:
            continue
        previous_pair = previous_pairs_map.get(pair_id)
        if not previous_pair or not previous_pair.get("history"):
            continue
        pairs_result.append(previous_pair)
        carried_pair_ids.add(pair_id)
        if pair_id in previous_dividend_histories:
            dividend_histories[pair_id] = previous_dividend_histories[pair_id]
        print(
            f"  WARNING: {pair['name']} 새 데이터 없음 — 기존 기록 유지 "
            f"({len(previous_pair['history'])}일, ~{previous_pair['history'][-1]['date']})"
        )
    return carried_pair_ids


def find_stale_pair_warnings(pairs_result, warn_days=STALE_PAIR_WARN_DAYS):
    """전체 최신일보다 warn_days 넘게 뒤처진 종목 경고 메시지 목록 (중단은 하지 않음)."""
    last_dates = [
        p["history"][-1]["date"]
        for p in pairs_result
        if p.get("history") and not p.get("isAverage")
    ]
    if not last_dates:
        return []

    global_latest = datetime.strptime(max(last_dates), "%Y-%m-%d")
    warnings = []
    for pair_data in pairs_result:
        history = pair_data.get("history")
        if pair_data.get("isAverage") or not history:
            continue
        pair_last = history[-1]["date"]
        gap_days = (global_latest - datetime.strptime(pair_last, "%Y-%m-%d")).days
        if gap_days > warn_days:
            warnings.append(
                f"{pair_data['id']}: 마지막 데이터가 전체 최신일보다 {gap_days}일 "
                f"뒤처짐 ({pair_last}) — 거래정지/상장폐지 확인 필요"
            )
    return warnings


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
_internal_close_history_cache = {}
_internal_ticker_meta_cache = {}
_internal_index_history_cache = {}
_internal_dividend_rows_cache = {}
_pair_yahoo_history_cache = {}
_dividend_series_cache = {}
_sheet_dividend_amount_cache = None
_internal_price_api_available = None


def get_div_yield(ticker):
    if ticker not in _div_yield_cache:
        _div_yield_cache[ticker] = get_ticker_meta(ticker)["dividendYield"] or 0
    return _div_yield_cache[ticker]


def normalize_ticker_code(ticker):
    if not ticker:
        return None
    code = str(ticker).split(".")[0]
    digits = re.sub(r"\D", "", code)
    if not digits:
        return None
    return digits[-6:].zfill(6)


def is_internal_price_api_available():
    global _internal_price_api_available

    if _internal_price_api_available is not None:
        return _internal_price_api_available
    if not INTERNAL_PRICE_API_ENABLED or not INTERNAL_PRICE_API_BASE_URL:
        _internal_price_api_available = False
        return _internal_price_api_available

    try:
        request = Request(
            f"{INTERNAL_PRICE_API_BASE_URL}/api/health",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urlopen(request, timeout=INTERNAL_PRICE_HEALTH_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode("utf-8", errors="replace"))
        _internal_price_api_available = payload.get("status") == "ok"
    except Exception as exc:
        print(f"  WARNING: 내부 가격 API 사용 불가, 기존 fallback 사용 ({exc})")
        _internal_price_api_available = False

    return _internal_price_api_available


def fetch_internal_dividend_rows(tickers):
    ticker_codes = {
        ticker: normalize_ticker_code(ticker)
        for ticker in tickers
    }
    missing_codes = sorted(
        {
            code for code in ticker_codes.values()
            if code and code not in _internal_dividend_rows_cache
        }
    )

    if INTERNAL_DIVIDENDS_API_URL and missing_codes and is_internal_price_api_available():
        params = {"tickers": ",".join(missing_codes)}
        request = Request(
            f"{INTERNAL_DIVIDENDS_API_URL}?{urlencode(params)}",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        try:
            with urlopen(request, timeout=INTERNAL_PRICE_TIMEOUT_SECONDS) as response:
                payload = json.loads(response.read().decode("utf-8", errors="replace"))
            dividends = payload.get("dividends") or {}
            for code in missing_codes:
                _internal_dividend_rows_cache[code] = dividends.get(code) or []
        except Exception as exc:
            print(f"  WARNING: 내부 배당 API 수집 실패 ({exc})")
            for code in missing_codes:
                _internal_dividend_rows_cache.setdefault(code, [])
    else:
        for code in missing_codes:
            _internal_dividend_rows_cache.setdefault(code, [])

    return {
        ticker: _internal_dividend_rows_cache.get(code, [])
        for ticker, code in ticker_codes.items()
        if code
    }


def latest_internal_dividend_per_share(ticker):
    rows = fetch_internal_dividend_rows([ticker]).get(ticker, [])
    normalized = []
    for row in rows:
        amount = pd.to_numeric(row.get("cash_dividend_per_share"), errors="coerce")
        if pd.isna(amount):
            continue
        fiscal_year = pd.to_numeric(row.get("fiscal_year"), errors="coerce")
        normalized.append(
            {
                "fiscalYear": int(fiscal_year) if not pd.isna(fiscal_year) else 0,
                "date": str(row.get("fiscal_period_end") or row.get("available_date") or ""),
                "amount": float(amount),
            }
        )
    if not normalized:
        return None

    normalized.sort(key=lambda item: (item["fiscalYear"], item["date"]))
    return round(normalized[-1]["amount"], 4)


def get_internal_dividend_amounts(common_ticker, preferred_ticker):
    return {
        "commonDividendPerShare": latest_internal_dividend_per_share(common_ticker),
        "preferredDividendPerShare": latest_internal_dividend_per_share(preferred_ticker),
    }


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


def normalize_sheet_code(value):
    if value is None or pd.isna(value):
        return None
    digits = re.sub(r"\D", "", str(value))
    if not digits:
        return None
    return digits[-6:].zfill(6)


def parse_sheet_dividend_amount(value):
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    text = text.replace(",", "")
    try:
        return round(float(text), 4)
    except ValueError:
        return None


def fetch_sheet_dividend_amounts():
    global _sheet_dividend_amount_cache

    if _sheet_dividend_amount_cache is not None:
        return _sheet_dividend_amount_cache

    csv_text = None
    try:
        request = Request(
            GOOGLE_SHEET_DIVIDEND_URL,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urlopen(request, timeout=15) as response:
            csv_text = response.read().decode("utf-8")
        GOOGLE_SHEET_DIVIDEND_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        GOOGLE_SHEET_DIVIDEND_CACHE_PATH.write_text(csv_text, encoding="utf-8")
    except Exception:
        if GOOGLE_SHEET_DIVIDEND_CACHE_PATH.exists():
            csv_text = GOOGLE_SHEET_DIVIDEND_CACHE_PATH.read_text(encoding="utf-8")

    if not csv_text:
        _sheet_dividend_amount_cache = {
            "byPair": {},
            "byPreferred": {},
            "byCommon": {},
        }
        return _sheet_dividend_amount_cache

    try:
        sheet_df = pd.read_csv(StringIO(csv_text), dtype=str, header=None)
    except Exception:
        _sheet_dividend_amount_cache = {
            "byPair": {},
            "byPreferred": {},
            "byCommon": {},
        }
        return _sheet_dividend_amount_cache

    # 배당 블록 시작 컬럼은 시트에 열이 추가될 때마다 밀리므로 앵커로 감지한다
    # (헤더 행은 종목코드 정규화가 None이 되어 아래 루프에서 자연히 건너뛴다)
    header_cells = sheet_df.iloc[0].tolist() if len(sheet_df) else []
    sample_row_cells = []
    for _, row in sheet_df.iloc[1:].iterrows():
        cells = row.tolist()
        if any(isinstance(c, str) and c.strip().endswith("억") for c in cells):
            sample_row_cells = cells
            break
    latest_fiscal_year = datetime.now(KST).year - 1
    common_dividend_idx, preferred_dividend_idx = dividend_sources.detect_sheet_layout(
        header_cells, sample_row_cells, latest_fiscal_year
    )

    by_pair = {}
    by_preferred = {}
    by_common = {}

    for _, row in sheet_df.iterrows():
        preferred_code = normalize_sheet_code(row.iloc[1] if len(row) > 1 else None)
        common_code = normalize_sheet_code(row.iloc[2] if len(row) > 2 else None)
        common_dividend = parse_sheet_dividend_amount(
            row.iloc[common_dividend_idx] if len(row) > common_dividend_idx else None
        )
        preferred_dividend = parse_sheet_dividend_amount(
            row.iloc[preferred_dividend_idx] if len(row) > preferred_dividend_idx else None
        )

        if not preferred_code and not common_code:
            continue

        dividend_data = {
            "commonDividendPerShare": common_dividend,
            "preferredDividendPerShare": preferred_dividend,
        }

        if common_code and preferred_code:
            by_pair[(common_code, preferred_code)] = dividend_data
        if preferred_code:
            by_preferred[preferred_code] = dividend_data
        if common_code:
            by_common[common_code] = dividend_data

    _sheet_dividend_amount_cache = {
        "byPair": by_pair,
        "byPreferred": by_preferred,
        "byCommon": by_common,
    }
    return _sheet_dividend_amount_cache


def get_sheet_dividend_amounts(common_ticker, preferred_ticker):
    dividend_data = fetch_sheet_dividend_amounts()
    common_code = normalize_sheet_code(common_ticker.split(".")[0] if common_ticker else None)
    preferred_code = normalize_sheet_code(preferred_ticker.split(".")[0] if preferred_ticker else None)

    if common_code and preferred_code:
        pair_match = dividend_data["byPair"].get((common_code, preferred_code))
        if pair_match:
            return pair_match

    preferred_match = dividend_data["byPreferred"].get(preferred_code) if preferred_code else None
    common_match = dividend_data["byCommon"].get(common_code) if common_code else None

    if preferred_match and common_match:
        return {
            "commonDividendPerShare": (
                preferred_match.get("commonDividendPerShare")
                if preferred_match.get("commonDividendPerShare") is not None
                else common_match.get("commonDividendPerShare")
            ),
            "preferredDividendPerShare": (
                preferred_match.get("preferredDividendPerShare")
                if preferred_match.get("preferredDividendPerShare") is not None
                else common_match.get("preferredDividendPerShare")
            ),
        }

    return preferred_match or common_match


def calculate_average_traded_value(price_series, volume_series, window=AVG_TRADED_VALUE_WINDOW):
    if price_series is None or volume_series is None:
        return None

    traded_value = (
        pd.to_numeric(price_series, errors="coerce")
        * pd.to_numeric(volume_series, errors="coerce")
    ).dropna()
    if traded_value.empty:
        return None

    return round(float(traded_value.tail(window).mean()), 0)


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


def extract_naver_row_text(html, label):
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
    return row_html[text_start + 1:text_end]


def extract_naver_row_value(html, label):
    return parse_number_text(extract_naver_row_text(html, label))


def parse_float_text(value):
    match = re.search(r"-?\d[\d,]*(?:\.\d+)?", str(value or ""))
    if not match:
        return None
    try:
        return float(match.group().replace(",", ""))
    except ValueError:
        return None


def extract_naver_indicator(html, indicator_id):
    """네이버 종목 메인 페이지의 <em id="_per">8.66</em> 류 투자지표 값을 파싱한다."""
    match = re.search(
        rf'id="{indicator_id}"[^>]*>\s*([-\d,.]+)\s*<', html
    )
    return parse_float_text(match.group(1)) if match else None


def extract_naver_foreign_ratio(html):
    """외국인소진율(B/A) 행의 값을 파싱한다.

    라벨이 <strong>으로 감싸이고 도움말 툴팁 div가 끼어 있어 행 파서로는 못 잡는다.
    라벨 위치에서 가장 가까운 다음 <em> 값을 읽는다 (툴팁에는 <em>이 없음).
    """
    idx = html.find("외국인소진율")
    if idx == -1:
        return None
    match = re.search(r"<em[^>]*>\s*([\d,.]+)\s*%?\s*</em>", html[idx:idx + 3000])
    return parse_float_text(match.group(1)) if match else None


def parse_naver_annual_net_incomes(html):
    """기업실적분석 표에서 확정 연간 당기순이익 목록(원 단위, 과거→최근)을 반환한다.

    추정치 컬럼('(E)' 표기)은 제외한다. 표가 없거나 파싱 실패 시 빈 목록.
    """
    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        return []
    for table in tables:
        try:
            first_col = table.iloc[:, 0].astype(str).str.strip()
        except Exception:
            continue
        row_matches = first_col[first_col == "당기순이익"]
        if row_matches.empty or not isinstance(table.columns, pd.MultiIndex):
            continue
        row = table.loc[row_matches.index[0]]
        incomes = []
        for column in table.columns[1:]:
            labels = [str(level) for level in column]
            if not any("최근 연간 실적" in level for level in labels):
                continue
            if any("(E)" in level for level in labels):
                continue
            value = parse_float_text(row[column])
            if value is None:
                continue
            incomes.append(value * 100_000_000)  # 억원 → 원
        if incomes:
            return incomes
    return []


def get_naver_ticker_meta(ticker):
    if ticker in _naver_meta_cache:
        return _naver_meta_cache[ticker]

    meta = {
        "marketCap": None,
        "sharesOutstanding": None,
        "per": None,
        "pbr": None,
        "foreignRatio": None,
        "annualNetIncomes": [],
        "naverDividendYield": None,
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
    # 본주 건전성(투자매력도) 지표 — 같은 페이지에서 추가 파싱 (추가 요청 없음)
    meta["per"] = extract_naver_indicator(html, "_per")
    meta["pbr"] = extract_naver_indicator(html, "_pbr")
    meta["foreignRatio"] = extract_naver_foreign_ratio(html)
    meta["annualNetIncomes"] = parse_naver_annual_net_incomes(html)
    # 공식 배당수익률 — 배당액 소스(내부 API/시트) 낡음 검증 기준값
    meta["naverDividendYield"] = extract_naver_indicator(html, "_dvr")

    _naver_meta_cache[ticker] = meta
    return meta


def get_ticker_meta(ticker):
    if ticker in _ticker_meta_cache:
        return _ticker_meta_cache[ticker]

    meta = {
        "dividendYield": 0,
        "marketCap": None,
        "sharesOutstanding": None,
        "per": None,
        "pbr": None,
        "foreignRatio": None,
        "annualNetIncomes": [],
        "naverDividendYield": None,
    }
    internal_meta = _internal_ticker_meta_cache.get(ticker, {})

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
        internal_meta.get("marketCap")
        or naver_meta["marketCap"]
        or info.get("marketCap")
        or _read_fast_info_value(fast_info, "marketCap", "market_cap")
    )
    meta["sharesOutstanding"] = (
        internal_meta.get("sharesOutstanding")
        or naver_meta["sharesOutstanding"]
        or info.get("sharesOutstanding")
        or _read_fast_info_value(fast_info, "sharesOutstanding", "shares", "shares_outstanding")
    )
    # 투자매력도(본주 건전성) 지표: 네이버 우선, PER/PBR은 yfinance 폴백
    meta["per"] = naver_meta["per"] or info.get("trailingPE")
    meta["pbr"] = naver_meta["pbr"] or info.get("priceToBook")
    meta["foreignRatio"] = naver_meta["foreignRatio"]
    meta["annualNetIncomes"] = naver_meta["annualNetIncomes"]
    meta["naverDividendYield"] = naver_meta["naverDividendYield"]

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
    if not PROXY_HISTORY_BASE_URL or not code:
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

    # 전체 구간(1989~현재)을 한 번에 조회하면 깊은 종목에서 서버 타임아웃이 나므로
    # 2년 단위 윈도우로 나눠 역방향 순회한다. 요청은 재시도하고, 실패해도 수집분은 보존.
    history_rows = []
    seen_dates = set()
    start_date = PROXY_HISTORY_START_DATE.date()
    window_end = datetime.now(KST).date()
    consecutive_window_failures = 0
    partial = False

    while window_end >= start_date:
        window_start = max(start_date, window_end - timedelta(days=PROXY_HISTORY_WINDOW_DAYS - 1))
        cursor_end = window_end
        window_failed = False

        while cursor_end >= window_start:
            url = (
                f"{PROXY_HISTORY_BASE_URL}/v1/stocks/{code}/history"
                f"?start_date={window_start.isoformat()}"
                f"&end_date={cursor_end.isoformat()}"
                f"&period=D&adjusted=true"
            )
            payload = None
            for attempt in range(PROXY_HISTORY_RETRIES):
                try:
                    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
                    with urlopen(request, timeout=PROXY_HISTORY_TIMEOUT_SECONDS) as response:
                        payload = json.loads(response.read().decode("utf-8", errors="replace"))
                    break
                except Exception:
                    if attempt < PROXY_HISTORY_RETRIES - 1:
                        time.sleep(3 * (attempt + 1))
            if payload is None:
                window_failed = True
                partial = True
                break

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

        if window_failed:
            consecutive_window_failures += 1
            if consecutive_window_failures >= 2:
                print(
                    f"  WARNING: PROXY {ticker} 연속 윈도우 실패, "
                    f"부분 수집 {len(history_rows)}일로 중단"
                )
                break
        else:
            consecutive_window_failures = 0

        window_end = window_start - timedelta(days=1)

    if history_rows:
        history = pd.DataFrame(history_rows).dropna(subset=["date", "close", "volume"])
        history = history.set_index("date").sort_index()
        history = history[~history.index.duplicated(keep="first")]
        if not partial:
            # 부분 수집본을 캐시에 남기면 다음 실행의 완전 수집을 막으므로 완주 시에만 저장
            PROXY_HISTORY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
            history.to_csv(cache_path, encoding="utf-8")
    else:
        history = pd.DataFrame(columns=["close", "volume"])

    _proxy_daily_history_cache[ticker] = history
    return history.copy()


def iter_date_windows(start_date, end_date, max_days=INTERNAL_PRICE_MAX_DAYS):
    cursor = pd.Timestamp(start_date).normalize()
    final = pd.Timestamp(end_date).normalize()
    while cursor <= final:
        window_end = min(cursor + pd.Timedelta(days=max_days - 1), final)
        yield cursor, window_end
        cursor = window_end + pd.Timedelta(days=1)


def fetch_internal_daily_history(tickers, start_date, end_date):
    tickers = sorted({ticker for ticker in tickers if ticker and ticker != "^KS11"})
    close = pd.DataFrame(columns=tickers)
    volume = pd.DataFrame(columns=tickers)
    if not INTERNAL_DAILY_API_URL or not tickers or not is_internal_price_api_available():
        return close, volume

    frames_close = []
    frames_volume = []
    for window_start, window_end in iter_date_windows(start_date, end_date):
        params = {
            "tickers": ",".join(ticker.split(".")[0] for ticker in tickers),
            "since": window_start.strftime("%Y-%m-%d"),
            "until": window_end.strftime("%Y-%m-%d"),
            "fields": "close,volume,market_cap,listed_shares",
        }
        request = Request(
            f"{INTERNAL_DAILY_API_URL}?{urlencode(params)}",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        try:
            with urlopen(request, timeout=INTERNAL_PRICE_TIMEOUT_SECONDS) as response:
                payload = json.loads(response.read().decode("utf-8", errors="replace"))
        except Exception as exc:
            print(
                "  WARNING: 내부 가격 API 일봉 수집 실패 "
                f"({window_start:%Y-%m-%d}~{window_end:%Y-%m-%d}, {exc})"
            )
            continue

        close_rows = {}
        volume_rows = {}
        prices = payload.get("prices") or {}
        for ticker in tickers:
            code = ticker.split(".")[0]
            for item in prices.get(code, []) or []:
                date = pd.to_datetime(item.get("date"), errors="coerce")
                close_value = pd.to_numeric(item.get("close"), errors="coerce")
                volume_value = pd.to_numeric(item.get("volume"), errors="coerce")
                if pd.isna(date):
                    continue
                if not pd.isna(close_value):
                    close_rows.setdefault(date, {})[ticker] = close_value
                if not pd.isna(volume_value):
                    volume_rows.setdefault(date, {})[ticker] = volume_value

                meta = _internal_ticker_meta_cache.setdefault(
                    ticker,
                    {"marketCap": None, "sharesOutstanding": None},
                )
                market_cap = pd.to_numeric(item.get("market_cap"), errors="coerce")
                listed_shares = pd.to_numeric(item.get("listed_shares"), errors="coerce")
                if not pd.isna(market_cap):
                    meta["marketCap"] = int(market_cap)
                if not pd.isna(listed_shares):
                    meta["sharesOutstanding"] = int(listed_shares)

        if close_rows:
            frames_close.append(pd.DataFrame.from_dict(close_rows, orient="index"))
        if volume_rows:
            frames_volume.append(pd.DataFrame.from_dict(volume_rows, orient="index"))

    if frames_close:
        close = pd.concat(frames_close).sort_index()
        close = close[~close.index.duplicated(keep="last")]
    if frames_volume:
        volume = pd.concat(frames_volume).sort_index()
        volume = volume[~volume.index.duplicated(keep="last")]

    return close, volume


def latest_series_date(frame, ticker):
    if frame is None or ticker not in frame.columns:
        return None
    series = frame[ticker].dropna()
    if series.empty:
        return None
    return pd.Timestamp(series.index.max()).normalize()


def select_yahoo_fallback_tickers(tickers, close, end_date):
    # yfinance treats end as exclusive. At the daily 05:00 KST run, the
    # expected latest completed Korean trading date is usually yesterday.
    expected_recent_date = pd.Timestamp(end_date).normalize() - pd.Timedelta(days=1)
    targets = []
    for ticker in tickers:
        latest_date = latest_series_date(close, ticker)
        if latest_date is None or latest_date < expected_recent_date:
            targets.append(ticker)
    return targets


def merge_missing_price_frame(base, extra):
    if extra is None or extra.empty:
        return base
    if base is None or base.empty:
        return extra.copy()

    merged = base.copy()
    combined_index = merged.index.union(extra.index).sort_values()
    combined_columns = merged.columns.union(extra.columns)
    merged = merged.reindex(index=combined_index, columns=combined_columns)
    extra_aligned = extra.reindex(index=combined_index, columns=combined_columns)
    return merged.combine_first(extra_aligned)


def fetch_internal_index_history(series_id):
    if series_id not in _internal_index_history_cache:
        rows = []
        if INTERNAL_INDICES_API_URL and is_internal_price_api_available():
            params = {"series_id": series_id}
            request = Request(
                f"{INTERNAL_INDICES_API_URL}?{urlencode(params)}",
                headers={"User-Agent": "Mozilla/5.0"},
            )
            try:
                with urlopen(request, timeout=INTERNAL_PRICE_TIMEOUT_SECONDS) as response:
                    payload = json.loads(response.read().decode("utf-8", errors="replace"))
                rows = payload.get("indices") or []
            except Exception as exc:
                print(f"  WARNING: 내부 지수 API 수집 실패 ({series_id}, {exc})")
                try:
                    request = Request(INTERNAL_INDICES_API_URL, headers={"User-Agent": "Mozilla/5.0"})
                    with urlopen(request, timeout=INTERNAL_PRICE_TIMEOUT_SECONDS) as response:
                        payload = json.loads(response.read().decode("utf-8", errors="replace"))
                    rows = [
                        item for item in payload.get("indices") or []
                        if item.get("series_id") == series_id
                    ]
                except Exception as fallback_exc:
                    print(f"  WARNING: 내부 지수 API 전체 fallback 실패 ({fallback_exc})")
        _internal_index_history_cache[series_id] = rows

    rows = []
    for item in _internal_index_history_cache[series_id]:
        if item.get("series_id") != series_id:
            continue
        rows.append(
            {
                "date": pd.to_datetime(item.get("date"), errors="coerce"),
                "close": pd.to_numeric(item.get("value"), errors="coerce"),
            }
        )
    if not rows:
        return pd.Series(dtype="float64")

    history = pd.DataFrame(rows).dropna(subset=["date", "close"])
    history = history.set_index("date").sort_index()
    history = history[~history.index.duplicated(keep="last")]
    return history["close"]


def fetch_internal_close_history(ticker, since_date, until_date):
    code = ticker.split(".")[0]
    empty = pd.DataFrame(columns=["close"])
    if not INTERNAL_CLOSE_API_URL or not code or not is_internal_price_api_available():
        return empty.copy()

    since_text = pd.Timestamp(since_date).strftime("%Y-%m-%d")
    until_text = pd.Timestamp(until_date).strftime("%Y-%m-%d")
    cache_key = (code, since_text, until_text)
    if cache_key in _internal_close_history_cache:
        return _internal_close_history_cache[cache_key].copy()

    params = {
        "ticker": code,
        "since": since_text,
        "until": until_text,
    }
    url = f"{INTERNAL_CLOSE_API_URL}?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})

    try:
        with urlopen(request, timeout=INTERNAL_CLOSE_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode("utf-8", errors="replace"))
    except Exception as exc:
        print(f"  WARNING: INTERNAL_CLOSE {ticker} 수집 실패 ({exc})")
        _internal_close_history_cache[cache_key] = empty
        return empty.copy()

    rows = []
    for item in payload.get("prices", []) or []:
        rows.append(
            {
                "date": pd.to_datetime(item.get("date"), errors="coerce"),
                "close": pd.to_numeric(item.get("close"), errors="coerce"),
            }
        )

    if rows:
        history = pd.DataFrame(rows).dropna(subset=["date", "close"])
        history = history.set_index("date").sort_index()
        history = history[~history.index.duplicated(keep="last")]
    else:
        history = empty

    _internal_close_history_cache[cache_key] = history
    return history.copy()


def merge_internal_close_fallback(close_series, volume_series, ticker, start_date, end_date, enabled=True):
    if not enabled or close_series is None or volume_series is None:
        return close_series, None

    volume_numeric = pd.to_numeric(volume_series, errors="coerce")
    traded_dates = pd.DatetimeIndex(pd.to_datetime(volume_numeric[volume_numeric > 0].index)).normalize()
    if traded_dates.empty:
        return close_series, None

    close_dates = pd.DatetimeIndex(pd.to_datetime(close_series.index)).normalize()
    missing_dates = traded_dates.difference(close_dates)
    if missing_dates.empty:
        return close_series, None

    fallback = fetch_internal_close_history(
        ticker,
        max(pd.Timestamp(start_date).normalize(), missing_dates.min()),
        min(pd.Timestamp(end_date).normalize(), missing_dates.max()),
    )
    if fallback.empty:
        return close_series, None

    fallback.index = pd.DatetimeIndex(pd.to_datetime(fallback.index)).normalize()
    fill_dates = missing_dates.intersection(fallback.index)
    if fill_dates.empty:
        return close_series, None

    merged_close = pd.concat([close_series, fallback.loc[fill_dates, "close"]]).sort_index()
    merged_close = merged_close[~merged_close.index.duplicated(keep="last")]
    info = {
        "source": "internal_close",
        "ticker": ticker,
        "filledDays": int(len(fill_dates)),
        "since": fill_dates.min().strftime("%Y-%m-%d"),
        "until": fill_dates.max().strftime("%Y-%m-%d"),
    }
    return merged_close, info


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
    # 비활성 시 외부 수집 호출 자체를 막는다 (인자 즉시 평가로 인한 불필요 크롤 방지)
    if not enabled or yahoo_close.empty:
        return yahoo_close, yahoo_vol, None
    return merge_external_backfill(
        yahoo_close,
        yahoo_vol,
        fetch_naver_daily_history(ticker),
        ticker,
        "naver",
        enabled=enabled,
    )


def merge_proxy_backfill(yahoo_close, yahoo_vol, ticker, enabled=False):
    # 비활성 시 외부 수집 호출 자체를 막는다 (백필 대상이 아닌 전 티커가 매 실행 프록시를 크롤하던 버그)
    if not enabled or yahoo_close.empty:
        return yahoo_close, yahoo_vol, None
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

    internal_rows = fetch_internal_dividend_rows(tickers)
    internal_count = sum(len(rows) for rows in internal_rows.values())
    if internal_count:
        print(f"내부 배당 API 수집: {internal_count}건")

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
    parser.add_argument(
        "--disable-internal-close-fallback",
        action="store_true",
        help="Yahoo/Naver/프록시 종가 누락 시 내부 종가 API 백업 사용을 끕니다",
    )
    parser.add_argument(
        "--allow-history-truncation",
        action="store_true",
        help="기존 데이터보다 과거 구간이 줄어드는 것을 허용합니다 (의도적 재구축용)",
    )
    args = parser.parse_args()

    explicit_naver_backfill_pair_ids = set(DEFAULT_NAVER_BACKFILL_PAIR_IDS)
    if args.naver_backfill is not None:
        explicit_naver_backfill_pair_ids.update(args.naver_backfill)
    use_internal_close_fallback = not args.disable_internal_close_fallback
    explicit_proxy_backfill_pair_ids = set(DEFAULT_PROXY_BACKFILL_PAIR_IDS)
    if args.proxy_backfill is not None:
        explicit_proxy_backfill_pair_ids.update(args.proxy_backfill)
    previous_data = load_existing_data()
    existing_data = None if args.full else previous_data
    previous_hist_map = {
        p["id"]: p.get("history", [])
        for p in (previous_data or {}).get("pairs", [])
        if not p.get("isAverage")
    }
    proxy_backfill_progress = load_proxy_backfill_progress()
    if args.full:
        explicit_proxy_backfill_pair_ids.update(proxy_backfill_progress["completedPairIds"])
    auto_proxy_backfill_pair_ids = select_next_proxy_backfill_pairs(
        existing_data,
        set(proxy_backfill_progress["completedPairIds"]),
        args.auto_proxy_backfill_batch_size,
    )
    if not PROXY_HISTORY_BASE_URL:
        auto_proxy_backfill_pair_ids = []
        if args.auto_proxy_backfill_batch_size > 0:
            print("WARNING: PROXY_HISTORY_BASE_URL 미설정, 자동 프록시 백필 건너뜀")
    explicit_proxy_backfill_pair_ids.update(auto_proxy_backfill_pair_ids)
    existing_pair_ids = set()
    if existing_data:
        existing_pair_ids = {
            pair["id"]
            for pair in existing_data.get("pairs", [])
            if not pair.get("isAverage")
        }

    # 모든 주식 티커 수집 (중복 제거) + KOSPI 지수
    KOSPI_TICKER = "^KS11"
    stock_tickers = list(
        dict.fromkeys(
            ticker
            for pair in PAIRS
            for ticker in [pair["commonTicker"], pair["preferredTicker"]]
        )
    )

    # GitHub Actions runners use UTC. Use naive KST here so the 05:00 KST
    # daily job requests Yahoo with an exclusive end date of the KST date,
    # which includes the previous Korean close.
    end_date = datetime.now(KST).replace(tzinfo=None)
    configured_pair_ids = {pair["id"] for pair in PAIRS if not pair.get("isAverage")}
    missing_pair_ids = sorted(configured_pair_ids - existing_pair_ids)

    full_fetch_pair_ids = set()
    if existing_data:
        if missing_pair_ids:
            full_fetch_pair_ids = set(missing_pair_ids)
            print(
                "신규 종목 감지, 해당 종목만 전체 수집: "
                + ", ".join(missing_pair_ids)
            )
        pair_last_dates = get_pair_last_dates(existing_data)
        if pair_last_dates:
            # 가장 뒤처진 종목 기준 -5일 (최신 종목 기준 90일 하한)
            start_date = incremental_start(list(pair_last_dates.values()))
            laggard_id = min(pair_last_dates, key=lambda pid: pair_last_dates[pid])
            print(
                f"증분 갱신 모드: {start_date.strftime('%Y-%m-%d')}부터 가져옵니다 "
                f"(최후미 {laggard_id} {pair_last_dates[laggard_id]} 기준)"
            )
        else:
            start_date = datetime(2000, 1, 1)
            print("기존 히스토리 없음, 전체 다운로드")
            existing_data = None
    else:
        start_date = datetime(2000, 1, 1)
        print("전체 다운로드 모드")

    print(f"{len(stock_tickers)}개 주식 티커 다운로드 중...")
    print(f"기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")

    close, volume = fetch_internal_daily_history(stock_tickers, start_date, end_date)
    if not close.empty:
        print(f"내부 가격 API 수집: {len(close)}일 x {len(close.columns)}개 티커")

    for ticker in stock_tickers:
        if ticker not in close.columns:
            close[ticker] = pd.NA
        if ticker not in volume.columns:
            volume[ticker] = 0

    kospi_internal = fetch_internal_index_history("KOSPI")
    if not kospi_internal.empty:
        close[KOSPI_TICKER] = kospi_internal

    yahoo_tickers = select_yahoo_fallback_tickers(stock_tickers, close, end_date)
    if (
        KOSPI_TICKER not in close.columns
        or close[KOSPI_TICKER].dropna().empty
        or latest_series_date(close, KOSPI_TICKER) < pd.Timestamp(end_date).normalize() - pd.Timedelta(days=1)
    ):
        yahoo_tickers.insert(0, KOSPI_TICKER)
    yahoo_tickers = list(dict.fromkeys(yahoo_tickers))
    if yahoo_tickers:
        print("Yahoo fallback 대상: " + ", ".join(yahoo_tickers))
        yahoo_data = yf.download(
            yahoo_tickers,
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            auto_adjust=False,
            progress=True,
        )
        yahoo_close = yahoo_data["Close"]
        yahoo_volume = yahoo_data["Volume"]
        if isinstance(yahoo_close, pd.Series):
            yahoo_close = yahoo_close.to_frame(yahoo_tickers[0])
        if isinstance(yahoo_volume, pd.Series):
            yahoo_volume = yahoo_volume.to_frame(yahoo_tickers[0])

        close = merge_missing_price_frame(close, yahoo_close)
        volume = merge_missing_price_frame(volume, yahoo_volume)
    if KOSPI_TICKER not in close.columns:
        close[KOSPI_TICKER] = pd.NA

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
    attractiveness_financials = {}
    dividend_histories = {}

    for pair in PAIRS:
        ct = pair["commonTicker"]
        pt = pair["preferredTicker"]
        apply_naver_backfill = pair["id"] in naver_backfill_pair_ids
        apply_proxy_backfill = pair["id"] in proxy_backfill_pair_ids

        # 거래정지일(volume=0) 제외
        if (apply_proxy_backfill or pair["id"] in full_fetch_pair_ids) and not args.full:
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

        common_close, common_internal_fallback = merge_internal_close_fallback(
            common_close,
            common_vol,
            ct,
            start_date,
            end_date,
            enabled=use_internal_close_fallback,
        )
        preferred_close, preferred_internal_fallback = merge_internal_close_fallback(
            preferred_close,
            preferred_vol,
            pt,
            start_date,
            end_date,
            enabled=use_internal_close_fallback,
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

        if common_internal_fallback or preferred_internal_fallback:
            common_msg = (
                f"보통주 {common_internal_fallback['filledDays']}일 "
                f"({common_internal_fallback['since']}~{common_internal_fallback['until']})"
                if common_internal_fallback
                else "보통주 변화 없음"
            )
            preferred_msg = (
                f"우선주 {preferred_internal_fallback['filledDays']}일 "
                f"({preferred_internal_fallback['since']}~{preferred_internal_fallback['until']})"
                if preferred_internal_fallback
                else "우선주 변화 없음"
            )
            print(f"  INFO: {pair['name']} 내부 종가 백업 {common_msg}, {preferred_msg}")

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
        cv = common_vol.loc[common_dates]
        pv = preferred_vol.loc[common_dates]

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
            cv = cv.loc[common_dates]
            pv = pv.loc[common_dates]
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

        # 증분 모드: 기존 히스토리와 날짜 기준 비파괴 병합. 같은 날짜는 새 값이 이기고,
        # 새 데이터에 없는 날짜의 기존 레코드는 보존한다 — 소스가 축소된 히스토리를
        # 반환해도(예: 2026-06 Yahoo가 00279K.KS 과거 구간을 잃어 백필 재구축이 기존보다
        # 성기게 나온 사고) 기존 구간이 유실되지 않는다.
        # 의도적 재구축(--allow-history-truncation)일 때만 새 구간이 기존 창을 대체한다.
        if pair["id"] in existing_pairs_map and new_history:
            existing_hist = existing_pairs_map[pair["id"]]["history"]
            if args.allow_history_truncation:
                first_new_date = new_history[0]["date"]
                kept = [h for h in existing_hist if h["date"] < first_new_date]
                history = kept + new_history
            else:
                history = history_rules.merge_history_by_date(new_history, existing_hist)
        else:
            history = new_history

        # 모드와 무관하게 기존 데이터의 더 오래된 과거 구간(프록시/네이버 백필분)을 보존
        if not args.allow_history_truncation and history:
            earlier, history = history_rules.merge_preserved_history(
                history, previous_hist_map.get(pair["id"])
            )
            if earlier:
                print(f"  INFO: {pair['name']} 기존 과거 구간 보존 {len(earlier)}일 ({earlier[0]['date']}~{earlier[-1]['date']})")

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
        internal_dividend_amounts = get_internal_dividend_amounts(ct, pt)
        sheet_dividend_amounts = get_sheet_dividend_amounts(ct, pt) or {}
        # 소스 우선순위(내부 API > 시트)는 유지하되, 네이버 공식 배당수익률(_dvr)과
        # 함의 수익률이 크게 어긋나는 낡은 값(분할 미반영·전년도 값)은 걸러낸다
        common_dividend_per_share, common_dividend_source = dividend_sources.choose_dividend_per_share(
            latest["commonPrice"],
            [
                ("internal", internal_dividend_amounts.get("commonDividendPerShare")),
                ("sheet", sheet_dividend_amounts.get("commonDividendPerShare")),
            ],
            common_meta.get("naverDividendYield"),
        )
        preferred_dividend_per_share, preferred_dividend_source = dividend_sources.choose_dividend_per_share(
            latest["preferredPrice"],
            [
                ("internal", internal_dividend_amounts.get("preferredDividendPerShare")),
                ("sheet", sheet_dividend_amounts.get("preferredDividendPerShare")),
            ],
            preferred_meta.get("naverDividendYield"),
        )
        for side, chosen_source, rejected in (
            ("보통주", common_dividend_source, internal_dividend_amounts.get("commonDividendPerShare")),
            ("우선주", preferred_dividend_source, internal_dividend_amounts.get("preferredDividendPerShare")),
        ):
            if rejected is not None and chosen_source not in (None, "internal"):
                print(
                    f"  INFO: {pair['name']} {side} 배당액 소스 교정: "
                    f"internal {rejected} 기각 -> {chosen_source} 채택"
                )
        dividend_override = DIVIDEND_AMOUNT_OVERRIDES.get((ct, pt), {})
        if "commonDividendPerShare" in dividend_override:
            common_dividend_per_share = dividend_override["commonDividendPerShare"]
        if "preferredDividendPerShare" in dividend_override:
            preferred_dividend_per_share = dividend_override["preferredDividendPerShare"]
        c_dy = (
            common_dividend_per_share / latest["commonPrice"] * 100
            if common_dividend_per_share is not None and latest["commonPrice"]
            else get_div_yield(ct)
        )
        p_dy = (
            preferred_dividend_per_share / latest["preferredPrice"] * 100
            if preferred_dividend_per_share is not None and latest["preferredPrice"]
            else get_div_yield(pt)
        )
        common_avg_traded_value_20 = calculate_average_traded_value(c, cv)
        preferred_avg_traded_value_20 = calculate_average_traded_value(p, pv)

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
                "commonDividendPerShare": common_dividend_per_share,
                "preferredDividendPerShare": preferred_dividend_per_share,
                "commonMarketCap": common_meta["marketCap"],
                "preferredMarketCap": preferred_meta["marketCap"],
                "commonSharesOutstanding": common_meta["sharesOutstanding"],
                "preferredSharesOutstanding": preferred_meta["sharesOutstanding"],
                "commonAvgTradedValue20": common_avg_traded_value_20,
                "preferredAvgTradedValue20": preferred_avg_traded_value_20,
            },
            "history": history,
        }
        # 투자매력도는 전 종목 최고 괴리율(상대 스케일 기준)이 필요해 루프 종료 후 일괄 계산
        attractiveness_financials[pair["id"]] = {
            "per": common_meta.get("per"),
            "pbr": common_meta.get("pbr"),
            "foreignRatio": common_meta.get("foreignRatio"),
            "annualNetIncomes": common_meta.get("annualNetIncomes"),
        }
        pairs_result.append(pair_data)

        print(
            f"  {pair['name']}: {len(history)}일, "
            f"현재 괴리율 {latest['spread']:.2f}% "
            f"({'↑' if spread_change > 0 else '↓'}{abs(spread_change):.2f}%p) "
            f"배당: {pair_data['current']['commonDivYield']:.1f}%/{pair_data['current']['preferredDivYield']:.1f}%"
        )

    # 새 데이터가 없는 기존 종목 유지 — 거래정지/상장폐지/소스 장애로 이번 회차에
    # 결과가 비어도 그 종목만 직전 기록으로 넘어가고, 나머지 종목의 갱신은 계속된다.
    # (예전에는 한 종목이 비면 아래 품질 가드가 전체 실행을 중단시켜 60개 전 종목의
    #  히스토리가 통째로 멈췄다 — 2026-08 한화 거래정지 사고)
    carried_pair_ids = carry_forward_missing_pairs(
        pairs_result, PAIRS, previous_data, dividend_histories
    )
    for message in find_stale_pair_warnings(pairs_result):
        print(f"WARNING: {message}")

    # 투자매력도 일괄 계산 — 괴리율 축은 전 종목 최고 괴리율 기준 상대 스케일
    max_spread = max(
        (
            p["current"]["spread"]
            for p in pairs_result
            if p["current"].get("spread") is not None
        ),
        default=None,
    )
    if max_spread is not None:
        print(f"투자매력도 괴리율 만점 기준(전 종목 최고): {max_spread:.2f}%")
    for pair_data in pairs_result:
        # 유지된 종목은 이번 회차 재무/배당 입력이 없으므로 직전 점수를 그대로 둔다
        if pair_data["id"] in carried_pair_ids and pair_data.get("attractiveness"):
            continue
        pair_data["attractiveness"] = attractiveness.compute_attractiveness(
            pair_data["history"],
            pair_data["current"],
            attractiveness_financials.get(pair_data["id"]),
            dividend_histories.get(pair_data["id"], {}).get("preferred"),
            max_spread=max_spread,
        )

    # KOSPI 지수 데이터 준비
    kospi_close = close[KOSPI_TICKER].dropna()

    # Issuer-level sqrt preferred-market-cap weighted spread index.
    issuer_names = {pair_data["commonName"] for pair_data in pairs_result}
    daily_issuer_inputs = defaultdict(lambda: defaultdict(list))
    for pair_data in pairs_result:
        issuer = pair_data["commonName"]
        preferred_shares = pair_data["current"].get("preferredSharesOutstanding") or 0
        fallback_mcap = pair_data["current"].get("preferredMarketCap") or 0
        for h in pair_data["history"]:
            spread = h.get("spread")
            preferred_price = h.get("preferredPrice")
            if spread is None:
                continue
            preferred_mcap = 0
            if preferred_price is not None and preferred_shares > 0:
                preferred_mcap = float(preferred_price) * float(preferred_shares)
            if preferred_mcap <= 0:
                preferred_mcap = fallback_mcap
            if preferred_mcap <= 0:
                continue
            daily_issuer_inputs[h["date"]][issuer].append((float(spread), float(preferred_mcap)))

    avg_history = []
    n_issuers = len(issuer_names)
    for date in sorted(daily_issuer_inputs.keys()):
        issuer_values = []
        for inputs in daily_issuer_inputs[date].values():
            total_mcap = sum(mcap for _, mcap in inputs)
            if total_mcap <= 0:
                continue
            issuer_spread = sum(spread * mcap for spread, mcap in inputs) / total_mcap
            issuer_values.append((issuer_spread, math.sqrt(total_mcap)))
        # 종목 수가 절반 미만인 날은 휴장일 오류 데이터이므로 제외
        if len(issuer_values) < n_issuers / 2:
            continue
        total_weight = sum(weight for _, weight in issuer_values)
        if total_weight <= 0:
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
            "spread": round(sum(spread * weight for spread, weight in issuer_values) / total_weight, 2),
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
            "name": "괴리율 지수",
            "commonName": "",
            "preferredName": "",
            "isAverage": True,
            "methodology": "issuer_sqrt_preferred_market_cap",
            "current": {
                "commonPrice": 0,
                "preferredPrice": 0,
                "spread": latest_avg["spread"],
                "spreadChange": avg_change,
            },
            "history": avg_history,
        }
        print(
            f"  괴리율 지수: {len(avg_history)}일, "
            f"현재 괴리율 {latest_avg['spread']:.2f}% "
            f"({'↑' if avg_change > 0 else '↓'}{abs(avg_change):.2f}%p)"
        )

    # 전체 평균도 포함하여 괴리율 높은 순 정렬
    if avg_history:
        pairs_result.append(avg_pair)
    pairs_result.sort(key=lambda p: p["current"]["spread"], reverse=True)

    # 품질 가드: 기존 데이터 대비 히스토리 유실/후퇴가 감지되면 쓰기 전에 중단
    result_hist_map = {
        p["id"]: p.get("history", [])
        for p in pairs_result
        if not p.get("isAverage")
    }
    violations = history_rules.find_quality_violations(
        result_hist_map, previous_hist_map, configured_pair_ids
    )
    if violations and not args.allow_history_truncation:
        for violation in violations:
            print(f"ERROR: {violation}")
        print("ERROR: 히스토리 보존 위반으로 중단합니다 (--allow-history-truncation 으로 무시 가능)")
        sys.exit(1)

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

    info = data_writer.write_stock_data_outputs(stock_data, Path(__file__).parent)
    print(f"\n출력 완료: data.js {info['dataJsBytes']:,} bytes, summary {info['summaryBytes']:,} bytes, history {info['historyFiles']}개 파일 ({info['totalPoints']:,} 포인트)")


if __name__ == "__main__":
    main()
