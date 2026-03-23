#!/usr/bin/env python3
"""
Build current.json using Korea Investment Open API where possible,
with safe fallbacks for unsupported or temporarily unavailable metrics.
"""

from __future__ import annotations

import json
import os
import re
import ssl
import tempfile
import threading
import time
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    import websocket
except ImportError:  # pragma: no cover - optional runtime dependency
    websocket = None

KST = timezone(timedelta(hours=9))

CONFIG_PATH = Path(__file__).parent / "config.json"
OUTPUT_PATH = Path(__file__).parent / "current.json"

KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"
KIS_TOKEN_URL = f"{KIS_BASE_URL}/oauth2/tokenP"
KIS_APPROVAL_URL = f"{KIS_BASE_URL}/oauth2/Approval"
KIS_APP_KEY = os.environ.get("KIS_APP_KEY", "").strip()
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "").strip()
KIS_AUTH_CACHE_PATH = Path(
    os.environ.get("KIS_AUTH_CACHE_PATH")
    or Path(tempfile.gettempdir()) / "common_preferred_spread" / "kis_auth_cache.json"
)
KIS_CACHE_MARGIN_SECONDS = 300

NAVER_STOCK_API_URL = "https://polling.finance.naver.com/api/realtime/domestic/stock/{code}"
NAVER_INDEX_API_URL = "https://polling.finance.naver.com/api/realtime/domestic/index/{code}"
NAVER_WORLD_INDEX_API_URL = "https://polling.finance.naver.com/api/realtime/worldstock/index/{code}"
NAVER_MARKETINDEX_URL = "https://finance.naver.com/marketindex/"
HANKYUNG_KOSPI200_FUTURES_URL = "https://markets.hankyung.com/indices/kospi-future"
INVESTING_KOSPI200_FUTURES_URL = "https://kr.investing.com/indices/korea-200-futures"
DOMESTIC_CME_MASTER_URL = "https://new.real.download.dws.co.kr/common/master/fo_cme_code.mst.zip"

USER_AGENT = "Mozilla/5.0"
REQUEST_TIMEOUT = 10
MAX_WORKERS = 3

KIS_STOCK_QUOTE_PATH = "/uapi/domestic-stock/v1/quotations/inquire-price"
KIS_INDEX_QUOTE_PATH = "/uapi/domestic-stock/v1/quotations/inquire-index-price"
KIS_SP500_QUOTE_PATH = "/uapi/overseas-price/v1/quotations/inquire-time-indexchartprice"
KIS_FUTURES_QUOTE_PATH = "/uapi/domestic-futureoption/v1/quotations/inquire-price"

KIS_STOCK_TR_ID = "FHKST01010100"
KIS_INDEX_TR_ID = "FHPUP02100000"
KIS_SP500_TR_ID = "FHKST03030200"
KIS_FUTURES_QUOTE_TR_ID = "FHMIF10000000"
KIS_NIGHT_FUTURES_TRADE_TR_ID = "H0MFCNT0"
KIS_NIGHT_FUTURES_WS_URL = "ws://ops.koreainvestment.com:21000"
KIS_NIGHT_FUTURES_WS_WAIT_SECONDS = 12
KIS_NIGHT_FUTURES_WS_RETRIES = 2

AUTH_CACHE_LOCK = threading.Lock()

with open(CONFIG_PATH, encoding="utf-8") as f:
    PAIRS = json.load(f)


def ticker_to_code(ticker: str) -> str:
    return ticker.split(".")[0]


def parse_int(value):
    if value in (None, ""):
        return None
    return int(float(str(value).replace(",", "")))


def parse_float(value):
    if value in (None, ""):
        return None
    return float(str(value).replace(",", ""))


def round_or_none(value, digits=2):
    if value is None:
        return None
    return round(value, digits)


def first_not_none(*values):
    for value in values:
        if value is not None:
            return value
    return None


def fraction_digits(value):
    if value in (None, ""):
        return 0
    text = str(value).replace(",", "")
    if "." not in text:
        return 0
    return len(text.split(".", 1)[1])


def compute_spread(common_price, preferred_price):
    if common_price in (None, 0) or preferred_price is None:
        return None
    return round((common_price - preferred_price) / common_price * 100, 2)


def read_json_file(path: Path):
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def write_json_file(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp_path.replace(path)


def parse_kis_datetime(value: str):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)


def parse_iso_datetime(value: str):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def cache_entry_is_valid(expires_at: str, margin_seconds=KIS_CACHE_MARGIN_SECONDS):
    expires_dt = parse_iso_datetime(expires_at)
    if not expires_dt:
        return False
    if expires_dt.tzinfo is None:
        expires_dt = expires_dt.replace(tzinfo=timezone.utc)
    return expires_dt > datetime.now(timezone.utc) + timedelta(seconds=margin_seconds)


def load_auth_cache():
    return read_json_file(KIS_AUTH_CACHE_PATH) or {}


def save_auth_cache(cache):
    write_json_file(KIS_AUTH_CACHE_PATH, cache)


def has_kis_credentials():
    return bool(KIS_APP_KEY and KIS_APP_SECRET)


def http_request(url, *, method="GET", headers=None, params=None, payload=None):
    if params:
        encoded = urlencode(params)
        url = f"{url}?{encoded}" if "?" not in url else f"{url}&{encoded}"

    body = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")

    request = Request(url, data=body, method=method, headers=headers or {})

    try:
        with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="replace")
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{exc.code}: {error_body}") from exc
    except URLError as exc:
        raise RuntimeError(str(exc.reason)) from exc


def http_json(url, *, method="GET", headers=None, params=None, payload=None):
    return json.loads(
        http_request(
            url,
            method=method,
            headers=headers,
            params=params,
            payload=payload,
        )
    )


def http_text(url, *, headers=None):
    return http_request(url, headers=headers)


def get_kis_token():
    with AUTH_CACHE_LOCK:
        cache = load_auth_cache()
        if cache_entry_is_valid(cache.get("access_token_expires_at")) and cache.get(
            "access_token"
        ):
            return cache["access_token"]

        payload = http_json(
            KIS_TOKEN_URL,
            method="POST",
            headers={"content-type": "application/json; charset=UTF-8"},
            payload={
                "grant_type": "client_credentials",
                "appkey": KIS_APP_KEY,
                "appsecret": KIS_APP_SECRET,
            },
        )
        token = payload["access_token"]
        expires_at = (
            parse_kis_datetime(payload["access_token_token_expired"])
            .astimezone(timezone.utc)
            .isoformat()
        )
        cache["access_token"] = token
        cache["access_token_expires_at"] = expires_at
        save_auth_cache(cache)
        return token


def get_kis_approval_key():
    with AUTH_CACHE_LOCK:
        cache = load_auth_cache()
        if cache_entry_is_valid(cache.get("approval_key_expires_at")) and cache.get(
            "approval_key"
        ):
            return cache["approval_key"]

        payload = http_json(
            KIS_APPROVAL_URL,
            method="POST",
            headers={"content-type": "application/json; charset=UTF-8"},
            payload={
                "grant_type": "client_credentials",
                "appkey": KIS_APP_KEY,
                "secretkey": KIS_APP_SECRET,
            },
        )
        approval_key = payload["approval_key"]
        cache["approval_key"] = approval_key
        cache["approval_key_expires_at"] = (
            datetime.now(timezone.utc) + timedelta(hours=23, minutes=55)
        ).isoformat()
        save_auth_cache(cache)
        return approval_key


def kis_get_json(path, tr_id, params):
    payload = http_json(
        f"{KIS_BASE_URL}{path}",
        headers={
            "content-type": "application/json; charset=UTF-8",
            "authorization": f"Bearer {get_kis_token()}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "tr_id": tr_id,
            "custtype": "P",
        },
        params=params,
    )
    if payload.get("rt_cd") not in (None, "0"):
        raise RuntimeError(
            f"{payload.get('msg_cd', 'UNKNOWN')}: {payload.get('msg1', 'API call failed')}"
        )
    return payload


def parse_signed_number(value, sign_code=None):
    number = parse_float(value)
    if number is None:
        return None

    text = str(value).strip()
    if text.startswith("-"):
        return -abs(number)
    if text.startswith("+"):
        return abs(number)

    if sign_code in {"4", "5"}:
        return -abs(number)
    if sign_code in {"1", "2"}:
        return abs(number)
    if sign_code == "3":
        return 0.0 if abs(number) < 1e-12 else number
    return number


def fetch_json_from_naver(url):
    payload = http_json(url, headers={"User-Agent": USER_AGENT})
    datas = payload.get("datas") or []
    if not datas:
        raise ValueError("empty response")
    return datas[0]


def fetch_text_from_naver(url):
    return http_text(url, headers={"User-Agent": USER_AGENT})


def fetch_text_from_public_page(url):
    return http_text(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
        },
    )


def fetch_naver_stock_quote(code):
    return fetch_json_from_naver(NAVER_STOCK_API_URL.format(code=code))


def fetch_naver_index_quote(code):
    return fetch_json_from_naver(NAVER_INDEX_API_URL.format(code=code))


def fetch_naver_world_index_quote(code):
    return fetch_json_from_naver(NAVER_WORLD_INDEX_API_URL.format(code=code))


def fetch_kis_stock_quote(code):
    payload = kis_get_json(
        KIS_STOCK_QUOTE_PATH,
        KIS_STOCK_TR_ID,
        {
            "FID_COND_MRKT_DIV_CODE": "UN",
            "FID_INPUT_ISCD": code,
        },
    )
    return payload.get("output") or {}


def fetch_kis_index_quote(index_code):
    payload = kis_get_json(
        KIS_INDEX_QUOTE_PATH,
        KIS_INDEX_TR_ID,
        {
            "FID_COND_MRKT_DIV_CODE": "U",
            "FID_INPUT_ISCD": index_code,
        },
    )
    return payload.get("output") or {}


def fetch_kis_sp500_quote():
    payload = kis_get_json(
        KIS_SP500_QUOTE_PATH,
        KIS_SP500_TR_ID,
        {
            "FID_COND_MRKT_DIV_CODE": "N",
            "FID_INPUT_ISCD": "SPX",
            "FID_HOUR_CLS_CODE": "0",
            "FID_PW_DATA_INCU_YN": "Y",
        },
    )
    return payload.get("output1") or {}


def fetch_kis_future_quote(code):
    payload = kis_get_json(
        KIS_FUTURES_QUOTE_PATH,
        KIS_FUTURES_QUOTE_TR_ID,
        {
            "FID_COND_MRKT_DIV_CODE": "F",
            "FID_INPUT_ISCD": code,
        },
    )
    return payload.get("output1") or {}


def build_quote_metric(quote, metric_id, name, unit=None, price_digits=2):
    if not quote:
        return None
    return {
        "id": metric_id,
        "name": name,
        "price": round_or_none(
            parse_float(first_not_none(quote.get("closePriceRaw"), quote.get("closePrice"))),
            price_digits,
        ),
        "change": round_or_none(
            parse_float(
                first_not_none(
                    quote.get("compareToPreviousClosePriceRaw"),
                    quote.get("compareToPreviousClosePrice"),
                )
            )
        ),
        "changePct": round_or_none(
            parse_float(
                first_not_none(
                    quote.get("fluctuationsRatioRaw"),
                    quote.get("fluctuationsRatio"),
                )
            )
        ),
        "marketStatus": quote.get("marketStatus"),
        "unit": unit,
    }


def build_marketindex_metric(
    html, head_class, metric_id, name, unit=None, price_digits=None
):
    pattern = re.compile(
        rf'<a[^>]+class="head\s+[^"]*\b{re.escape(head_class)}\b[^"]*"[^>]*>'
        rf'[\s\S]*?<div class="head_info ([^"]+)"[\s\S]*?<span class="value">([^<]+)</span>'
        rf'[\s\S]*?<span class="change">\s*([^<]+)</span>[\s\S]*?</a>'
        rf'[\s\S]*?<div class="graph_info">[\s\S]*?<span class="time">([^<]+)</span>',
        re.IGNORECASE,
    )
    match = pattern.search(html)
    if not match:
        return None

    class_name, value_text, change_text, _ = match.groups()
    price = parse_float(value_text)
    raw_change = parse_float(change_text)
    if price is None or raw_change is None:
        return None

    display_price_digits = (
        fraction_digits(value_text) if price_digits is None else price_digits
    )

    if "point_up" in class_name:
        signed_change = abs(raw_change)
    elif "point_dn" in class_name:
        signed_change = -abs(raw_change)
    else:
        signed_change = 0.0

    previous_price = price - signed_change
    change_pct = (
        round_or_none(signed_change / previous_price * 100)
        if previous_price not in (None, 0)
        else None
    )

    return {
        "id": metric_id,
        "name": name,
        "price": round_or_none(price, display_price_digits),
        "change": round_or_none(signed_change),
        "changePct": change_pct,
        "marketStatus": None,
        "unit": unit,
    }


def build_kis_stock_quote(stock_output):
    if not stock_output:
        return None
    sign_code = stock_output.get("prdy_vrss_sign")
    return {
        "closePriceRaw": parse_int(stock_output.get("stck_prpr")),
        "compareToPreviousClosePriceRaw": parse_signed_number(
            stock_output.get("prdy_vrss"),
            sign_code,
        ),
        "fluctuationsRatioRaw": parse_signed_number(
            stock_output.get("prdy_ctrt"),
            sign_code,
        ),
        "marketStatus": None,
    }


def build_kis_index_metric(index_output, metric_id, name):
    if not index_output:
        return None
    sign_code = index_output.get("prdy_vrss_sign")
    return {
        "id": metric_id,
        "name": name,
        "price": round_or_none(parse_float(index_output.get("bstp_nmix_prpr"))),
        "change": round_or_none(
            parse_signed_number(index_output.get("bstp_nmix_prdy_vrss"), sign_code)
        ),
        "changePct": round_or_none(
            parse_signed_number(index_output.get("bstp_nmix_prdy_ctrt"), sign_code)
        ),
        "marketStatus": None,
        "unit": None,
    }


def build_kis_overseas_index_metric(output, metric_id, name):
    if not output:
        return None
    sign_code = output.get("prdy_vrss_sign")
    return {
        "id": metric_id,
        "name": name,
        "price": round_or_none(parse_float(output.get("ovrs_nmix_prpr"))),
        "change": round_or_none(
            parse_signed_number(output.get("ovrs_nmix_prdy_vrss"), sign_code)
        ),
        "changePct": round_or_none(
            parse_signed_number(output.get("prdy_ctrt"), sign_code)
        ),
        "marketStatus": None,
        "unit": None,
    }


def build_kis_futures_metric(row, metric_id, name, *, code=None):
    if not row:
        return None
    sign_code = row.get("prdy_vrss_sign")
    return {
        "id": metric_id,
        "name": name,
        "code": first_not_none(row.get("futs_shrn_iscd"), code),
        "price": round_or_none(parse_float(row.get("futs_prpr"))),
        "change": round_or_none(
            parse_signed_number(row.get("futs_prdy_vrss"), sign_code)
        ),
        "changePct": round_or_none(
            parse_signed_number(row.get("futs_prdy_ctrt"), sign_code)
        ),
        "marketStatus": None,
        "time": first_not_none(row.get("stck_cntg_hour"), row.get("bsop_hour")),
        "contractName": row.get("hts_kor_isnm"),
    }


KIS_NIGHT_FUTURES_TRADE_COLUMNS = [
    "futs_shrn_iscd",
    "bsop_hour",
    "futs_prdy_vrss",
    "prdy_vrss_sign",
    "futs_prdy_ctrt",
    "futs_prpr",
    "futs_oprc",
    "futs_hgpr",
    "futs_lwpr",
    "last_cnqn",
    "acml_vol",
    "acml_tr_pbmn",
    "hts_thpr",
    "mrkt_basis",
    "dprt",
    "nmsc_fctn_stpl_prc",
    "fmsc_fctn_stpl_prc",
    "spead_prc",
    "hts_otst_stpl_qty",
    "otst_stpl_qty_icdc",
    "oprc_hour",
    "oprc_vrss_prpr_sign",
    "oprc_vrss_nmix_prpr",
    "hgpr_hour",
    "hgpr_vrss_prpr_sign",
    "hgpr_vrss_nmix_prpr",
    "lwpr_hour",
    "lwpr_vrss_prpr_sign",
    "lwpr_vrss_nmix_prpr",
    "shnu_rate",
    "cttr",
    "esdg",
    "otst_stpl_rgbf_qty_icdc",
    "thpr_basis",
    "futs_askp1",
    "futs_bidp1",
    "askp_rsqn1",
    "bidp_rsqn1",
    "seln_cntg_csnu",
    "shnu_cntg_csnu",
    "ntby_cntg_csnu",
    "seln_cntg_smtn",
    "shnu_cntg_smtn",
    "total_askp_rsqn",
    "total_bidp_rsqn",
    "prdy_vol_vrss_acml_vol_rate",
    "dynm_mxpr",
    "dynm_llam",
    "dynm_prc_limt_yn",
]


def is_kst_night_session(now=None):
    now = (now or datetime.now(KST)).astimezone(KST)
    return now.hour >= 18 or now.hour < 6


def get_kst_night_session_date(now=None):
    now = (now or datetime.now(KST)).astimezone(KST)
    if now.hour < 6:
        now -= timedelta(days=1)
    return now.strftime("%Y-%m-%d")


def get_kst_date_string(now=None):
    now = (now or datetime.now(KST)).astimezone(KST)
    return now.strftime("%Y-%m-%d")


def get_kst_day_month(now=None):
    now = (now or datetime.now(KST)).astimezone(KST)
    return now.strftime("%d/%m")


def get_kst_night_session_day_month(now=None):
    session_date = datetime.strptime(get_kst_night_session_date(now), "%Y-%m-%d").replace(
        tzinfo=KST
    )
    return session_date.strftime("%d/%m")


def extract_contract_year_month(contract_name):
    if not contract_name:
        return None
    match = re.search(r"(\d{6})", contract_name)
    return match.group(1) if match else None


def build_kospi200_night_future_keys(contract_name=None):
    candidates = []
    year_month = extract_contract_year_month(contract_name)
    month_text = year_month[-2:] if year_month else None

    if month_text and month_text.isdigit():
        short_month = str(int(month_text))
        for prefix in ("W", "V", "T"):
            candidates.extend(
                [
                    f"101{prefix}{month_text}",
                    f"101{prefix}{short_month}000",
                ]
            )

    candidates.extend(["101W09", "101W9000", "101V06"])

    deduped = []
    for candidate in candidates:
        if candidate and candidate not in deduped:
            deduped.append(candidate)
    return deduped


def parse_kis_ws_rows(raw_message, expected_tr_id, columns):
    if not raw_message or raw_message[0] not in {"0", "1"}:
        return []

    parts = raw_message.split("|", 3)
    if len(parts) < 4 or parts[1] != expected_tr_id:
        return []

    values = parts[3].split("^")
    if not values:
        return []

    try:
        row_count = int(parts[2])
    except (TypeError, ValueError):
        row_count = 1

    width = len(columns)
    rows = []
    for index in range(max(row_count, 1)):
        start = index * width
        end = start + width
        if end > len(values):
            break
        rows.append(dict(zip(columns, values[start:end])))
    return rows


def build_kis_night_futures_metric_from_trade_row(row, session_date):
    metric = build_kis_futures_metric(
        row,
        "KOSPI200_FUTURES",
        "KOSPI200 선물",
    )
    if not metric or metric.get("price") is None:
        return None

    metric["source"] = "kis_websocket_trade"
    metric["sessionTradeDate"] = session_date
    return metric


def fetch_kis_night_futures_metric(contract_name):
    if websocket is None or not has_kis_credentials():
        return None

    approval_key = get_kis_approval_key()
    candidate_keys = build_kospi200_night_future_keys(contract_name)
    session_date = get_kst_night_session_date()

    for _ in range(KIS_NIGHT_FUTURES_WS_RETRIES):
        ws = None
        try:
            ws = websocket.create_connection(
                KIS_NIGHT_FUTURES_WS_URL,
                timeout=REQUEST_TIMEOUT,
            )

            for tr_key in candidate_keys:
                ws.send(
                    json.dumps(
                        {
                            "header": {
                                "approval_key": approval_key,
                                "custtype": "P",
                                "tr_type": "1",
                                "content-type": "utf-8",
                            },
                            "body": {
                                "input": {
                                    "tr_id": KIS_NIGHT_FUTURES_TRADE_TR_ID,
                                    "tr_key": tr_key,
                                }
                            },
                        }
                    )
                )
                time.sleep(0.2)

            deadline = time.monotonic() + KIS_NIGHT_FUTURES_WS_WAIT_SECONDS
            while time.monotonic() < deadline:
                remaining = max(1, int(deadline - time.monotonic()))
                ws.settimeout(remaining)

                try:
                    raw_message = ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue

                if not raw_message:
                    continue

                if raw_message.startswith("{"):
                    try:
                        payload = json.loads(raw_message)
                    except json.JSONDecodeError:
                        continue

                    if payload.get("header", {}).get("tr_id") == "PINGPONG":
                        ws.send(raw_message)
                    continue

                for row in parse_kis_ws_rows(
                    raw_message,
                    KIS_NIGHT_FUTURES_TRADE_TR_ID,
                    KIS_NIGHT_FUTURES_TRADE_COLUMNS,
                ):
                    metric = build_kis_night_futures_metric_from_trade_row(
                        row,
                        session_date,
                    )
                    if metric:
                        return metric
        except Exception:
            continue
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass

    return None


def previous_night_future_is_reusable(metric, now=None):
    if not metric or metric.get("id") not in {"KOSPI200_FUTURES", "KOSPI200_NIGHT_FUTURES"}:
        return False

    if metric.get("source") != "kis_websocket_trade":
        return True

    if not is_kst_night_session(now):
        return True

    return metric.get("sessionTradeDate") == get_kst_night_session_date(now)


def build_public_night_futures_metric_from_html(html):
    if not html:
        return None

    patterns = {
        "price": r'data-test="instrument-price-last"[^>]*>([^<]+)<',
        "change": r'data-test="instrument-price-change"[^>]*>([^<]+)<',
        "changePct": r'data-test="instrument-price-change-percent"[^>]*>([^<]+)<',
        "time": r'data-test="trading-time-label"[^>]*>([^<]+)<',
    }
    values = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
        values[key] = match.group(1).strip() if match else None

    price = parse_float(values["price"])
    change = parse_float(values["change"])
    change_pct_text = (values["changePct"] or "").strip("() ")
    change_pct = parse_float(change_pct_text.replace("%", ""))
    time_text = (values["time"] or "").strip()

    if price is None or change is None or change_pct is None:
        return None
    if time_text and re.fullmatch(r"\d{2}/\d{2}", time_text):
        if time_text != get_kst_night_session_day_month():
            return None

    metric = {
        "id": "KOSPI200_FUTURES",
        "name": "KOSPI200 선물",
        "price": round_or_none(price),
        "change": round_or_none(change),
        "changePct": round_or_none(change_pct),
        "marketStatus": None,
        "unit": None,
        "source": "investing_html",
        "sessionTradeDate": get_kst_night_session_date(),
        "time": time_text or None,
    }
    return metric


def fetch_public_night_futures_metric():
    try:
        html = fetch_text_from_public_page(INVESTING_KOSPI200_FUTURES_URL)
    except Exception:
        return None

    return build_public_night_futures_metric_from_html(html)


def build_hankyung_kospi200_futures_metric_from_html(html):
    if not html:
        return None

    match = re.search(
        r'<div class="stock-data(?:\s+(?P<direction>\w+))?">.*?'
        r'<p class="price">\s*(?P<price>[\d,]+\.\d+)\s*</p>.*?'
        r'<span class="stock-point">\s*(?P<change>[\d,]+\.\d+)\s*</span>.*?'
        r'<span class="rate">\s*(?P<changePct>[+-]?[\d,]+\.\d+%)\s*</span>.*?'
        r'<p class="txt-info txt-rt"[^>]*>\s*(?P<tradeDate>\d{4}\.\d{2}\.\d{2})\s*(?P<status>[^<]+?)\s*</p>',
        html,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None

    direction = (match.group("direction") or "").lower()
    price = parse_float(match.group("price"))
    change = parse_float(match.group("change"))
    change_pct = parse_float((match.group("changePct") or "").replace("%", ""))
    trade_date = (match.group("tradeDate") or "").replace(".", "-")
    status_text = re.sub(r"\s+", " ", (match.group("status") or "").strip())

    if price is None or change is None or change_pct is None:
        return None
    if trade_date != get_kst_date_string():
        return None

    if change_pct < 0 or direction == "down":
        change = -abs(change)
    elif change_pct > 0 or direction == "up":
        change = abs(change)

    return {
        "id": "KOSPI200_FUTURES",
        "name": "KOSPI200 선물",
        "price": round_or_none(price),
        "change": round_or_none(change),
        "changePct": round_or_none(change_pct),
        "marketStatus": status_text or None,
        "unit": None,
        "source": "hankyung_html",
        "sessionTradeDate": trade_date,
        "time": status_text or None,
    }


def fetch_hankyung_kospi200_futures_metric():
    try:
        html = fetch_text_from_public_page(HANKYUNG_KOSPI200_FUTURES_URL)
    except Exception:
        return None

    return build_hankyung_kospi200_futures_metric_from_html(html)


def merge_metric(primary, fallback):
    if not primary and not fallback:
        return None
    primary = primary or {}
    fallback = fallback or {}
    merged = {
        "id": primary.get("id") or fallback.get("id"),
        "name": primary.get("name") or fallback.get("name"),
        "price": primary.get("price")
        if primary.get("price") is not None
        else fallback.get("price"),
        "change": primary.get("change")
        if primary.get("change") is not None
        else fallback.get("change"),
        "changePct": primary.get("changePct")
        if primary.get("changePct") is not None
        else fallback.get("changePct"),
        "marketStatus": primary.get("marketStatus") or fallback.get("marketStatus"),
        "unit": primary.get("unit")
        if primary.get("unit") is not None
        else fallback.get("unit"),
    }
    for extra_key in ("code", "time", "contractName", "source", "sessionTradeDate"):
        value = primary.get(extra_key)
        merged[extra_key] = value if value is not None else fallback.get(extra_key)
    return merged


def fetch_combined_stock_quote(code):
    errors = []

    if has_kis_credentials():
        try:
            kis_quote = build_kis_stock_quote(fetch_kis_stock_quote(code))
            if kis_quote:
                return kis_quote, "kis"
        except Exception as exc:  # noqa: BLE001
            errors.append(f"KIS {exc}")

    try:
        return fetch_naver_stock_quote(code), "naver"
    except Exception as exc:  # noqa: BLE001
        errors.append(f"NAVER {exc}")

    raise RuntimeError(" / ".join(errors))


def fetch_all_quotes(codes):
    if not codes:
        return {}, {}, set()

    quotes = {}
    errors = {}
    providers = set()

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(codes))) as executor:
        future_map = {executor.submit(fetch_combined_stock_quote, code): code for code in codes}
        for future in as_completed(future_map):
            code = future_map[future]
            try:
                quote, provider = future.result()
                quotes[code] = quote
                providers.add(provider)
            except Exception as exc:  # noqa: BLE001
                errors[code] = str(exc)


    return quotes, errors, providers


def get_previous_market_metric(previous_snapshot, metric_id):
    market = (
        previous_snapshot.get("market")
        or previous_snapshot.get("summary", {}).get("market")
        or {}
    )
    if market.get("id") == metric_id:
        return market
    for metric in market.get("extras", []) or []:
        if metric.get("id") == metric_id:
            return metric
    return None


def get_previous_night_future(previous_snapshot):
    market = (
        previous_snapshot.get("market")
        or previous_snapshot.get("summary", {}).get("market")
        or {}
    )
    metric = market.get("nightFuture") or market.get("future")
    if not metric:
        return None
    return (
        metric
        if metric.get("id") in {"KOSPI200_FUTURES", "KOSPI200_NIGHT_FUTURES"}
        else None
    )


def fetch_market_metrics(previous_snapshot):
    market_extras = []
    providers = set()

    kospi_metric = None
    kosdaq_metric = None
    sp500_metric = None

    if has_kis_credentials():
        try:
            kospi_metric = build_kis_index_metric(fetch_kis_index_quote("0001"), "KOSPI", "코스피")
            providers.add("kis")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARNING: KOSPI 현재가 조회 실패: {exc}")

        try:
            kosdaq_metric = build_kis_index_metric(fetch_kis_index_quote("1001"), "KOSDAQ", "KOSDAQ")
            providers.add("kis")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARNING: KOSDAQ 현재가 조회 실패: {exc}")

        try:
            sp500_metric = build_kis_overseas_index_metric(
                fetch_kis_sp500_quote(),
                "SP500",
                "S&P500",
            )
            providers.add("kis")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARNING: S&P500 현재가 조회 실패: {exc}")

    if kospi_metric is None:
        try:
            kospi_metric = build_quote_metric(fetch_naver_index_quote("KOSPI"), "KOSPI", "코스피")
            providers.add("naver")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARNING: KOSPI 네이버 fallback 실패: {exc}")

    if kosdaq_metric is None:
        try:
            kosdaq_metric = build_quote_metric(fetch_naver_index_quote("KOSDAQ"), "KOSDAQ", "KOSDAQ")
            providers.add("naver")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARNING: KOSDAQ 네이버 fallback 실패: {exc}")

    if sp500_metric is None:
        try:
            sp500_metric = build_quote_metric(
                fetch_naver_world_index_quote(".INX"),
                "SP500",
                "S&P500",
            )
            providers.add("naver")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARNING: S&P500 네이버 fallback 실패: {exc}")

    marketindex_html = None
    try:
        marketindex_html = fetch_text_from_naver(NAVER_MARKETINDEX_URL)
        providers.add("naver")
    except Exception as exc:  # noqa: BLE001
        print(f"  WARNING: 시장지표 HTML 조회 실패: {exc}")

    usdkrw_metric = (
        build_marketindex_metric(
            marketindex_html,
            "usd",
            "USDKRW",
            "환율",
            price_digits=2,
        )
        if marketindex_html
        else None
    )
    gold_metric = (
        build_marketindex_metric(
            marketindex_html,
            "gold_inter",
            "GOLD",
            "금가격 (COMEX)",
            price_digits=2,
        )
        if marketindex_html
        else None
    )

    for metric_id, metric in (
        ("KOSDAQ", kosdaq_metric),
        ("USDKRW", usdkrw_metric),
        ("GOLD", gold_metric),
        ("SP500", sp500_metric),
    ):
        merged = merge_metric(metric, get_previous_market_metric(previous_snapshot, metric_id))
        if merged:
            market_extras.append(merged)

    kospi_metric = merge_metric(kospi_metric, get_previous_market_metric(previous_snapshot, "KOSPI"))
    return kospi_metric, market_extras, providers


def fetch_domestic_cme_master_rows():
    ssl._create_default_https_context = ssl._create_unverified_context
    workdir = Path(tempfile.mkdtemp(prefix="kis_cme_master_"))
    zip_path = workdir / "fo_cme_code.mst.zip"
    path = workdir / "fo_cme_code.mst"
    try:
        from urllib.request import urlretrieve

        urlretrieve(DOMESTIC_CME_MASTER_URL, zip_path)
        with zipfile.ZipFile(zip_path) as archive:
            archive.extractall(workdir)
        rows = []
        with open(path, encoding="cp949") as f:
            for row in f:
                rows.append(
                    {
                        "productType": row[0:1],
                        "shortCode": row[1:10].strip(),
                        "standardCode": row[10:22].strip(),
                        "name": row[22:63].strip(),
                        "strikePrice": row[63:72].strip(),
                        "baseShortCode": row[72:81].strip(),
                        "baseName": row[81:].strip(),
                    }
                )
        return rows
    except Exception:  # noqa: BLE001
        return []


def find_nearest_kospi200_contract_code():
    rows = [
        row
        for row in fetch_domestic_cme_master_rows()
        if row.get("productType") == "1" and row.get("baseName") == "KOSPI200"
    ]
    if not rows:
        return None
    rows.sort(key=lambda row: row.get("name", ""))
    return rows[0]["shortCode"]


def fetch_kospi200_metric(previous_snapshot):
    previous_metric = get_previous_night_future(previous_snapshot)
    reusable_previous_metric = (
        previous_metric if previous_night_future_is_reusable(previous_metric) else None
    )
    provider = None
    contract_name = None
    day_future_metric = None
    is_night_session = is_kst_night_session()

    if has_kis_credentials():
        try:
            contract_code = find_nearest_kospi200_contract_code()
            if contract_code:
                raw_snapshot = fetch_kis_future_quote(contract_code)
                contract_name = raw_snapshot.get("hts_kor_isnm")
                day_future_metric = build_kis_futures_metric(
                    raw_snapshot,
                    "KOSPI200_FUTURES",
                    "KOSPI200 선물",
                    code=contract_code,
                )
                if day_future_metric and day_future_metric.get("price") is not None:
                    day_future_metric["source"] = "kis_future_quote"
                    provider = "kis"
        except Exception as exc:  # noqa: BLE001
            print(f"  WARNING: KOSPI200 선물 현재가 조회 실패: {exc}")

    if is_night_session and has_kis_credentials():
        try:
            night_future_metric = fetch_kis_night_futures_metric(contract_name)
            if night_future_metric:
                provider = "kis"
                return merge_metric(night_future_metric, reusable_previous_metric), provider
        except Exception as exc:  # noqa: BLE001
            print(f"  WARNING: KOSPI200 야간선물 웹소켓 조회 실패: {exc}")

    if is_night_session:
        public_metric = fetch_public_night_futures_metric()
        if public_metric:
            provider = provider or "public"
            return merge_metric(public_metric, reusable_previous_metric), provider

    if day_future_metric:
        return merge_metric(day_future_metric, reusable_previous_metric), provider or "kis"

    public_metric = fetch_hankyung_kospi200_futures_metric()
    if public_metric:
        provider = provider or "public"
        return merge_metric(public_metric, reusable_previous_metric), provider

    public_metric = fetch_public_night_futures_metric()
    if public_metric:
        provider = provider or "public"
        return merge_metric(public_metric, reusable_previous_metric), provider

    return merge_metric(None, reusable_previous_metric), provider


def build_market_summary(market_quote, extras=None, night_future=None):
    market_summary = market_quote or {
        "id": "KOSPI",
        "name": "코스피",
        "price": None,
        "change": None,
        "changePct": None,
        "marketStatus": None,
        "unit": None,
    }
    if extras:
        market_summary["extras"] = extras
    if night_future:
        market_summary["nightFuture"] = night_future
    return market_summary


def build_summary(prices, market_summary=None):
    groups = defaultdict(list)
    for pair in PAIRS:
        price = prices.get(pair["id"])
        if price and price["spread"] is not None:
            groups[pair["commonName"]].append({"pair": pair, "price": price})

    representatives = [
        max(items, key=lambda item: item["price"]["spread"]) for items in groups.values()
    ]
    if not representatives:
        return None

    avg_spread = round(
        sum(item["price"]["spread"] for item in representatives) / len(representatives),
        2,
    )

    change_values = [
        item["price"]["spreadChange"]
        for item in representatives
        if item["price"]["spreadChange"] is not None
    ]
    avg_spread_change = (
        round(sum(change_values) / len(change_values), 2) if change_values else None
    )

    common_change_values = [
        item["price"]["commonChange"]
        for item in representatives
        if item["price"]["commonChange"] is not None
    ]
    avg_common_change = (
        round(sum(common_change_values) / len(common_change_values), 2)
        if common_change_values
        else None
    )

    preferred_change_values = [
        item["price"]["preferredChange"]
        for item in representatives
        if item["price"]["preferredChange"] is not None
    ]
    avg_preferred_change = (
        round(sum(preferred_change_values) / len(preferred_change_values), 2)
        if preferred_change_values
        else None
    )

    widening_candidates = [
        item
        for item in representatives
        if item["price"]["spreadChange"] is not None and item["price"]["spreadChange"] > 0
    ]
    narrowing_candidates = [
        item
        for item in representatives
        if item["price"]["spreadChange"] is not None and item["price"]["spreadChange"] < 0
    ]

    def serialize_leader(item):
        if not item:
            return None
        pair = item["pair"]
        price = item["price"]
        return {
            "id": pair["id"],
            "name": pair["name"],
            "commonName": pair["commonName"],
            "preferredName": pair["preferredName"],
            "spread": price["spread"],
            "spreadChange": price["spreadChange"],
        }

    widening_ranked = sorted(
        widening_candidates,
        key=lambda item: item["price"]["spreadChange"],
        reverse=True,
    )
    narrowing_ranked = sorted(
        narrowing_candidates,
        key=lambda item: item["price"]["spreadChange"],
    )
    ranked_by_widening = sorted(
        [
            item
            for item in representatives
            if item["price"]["spreadChange"] is not None
        ],
        key=lambda item: item["price"]["spreadChange"],
        reverse=True,
    )
    ranked_by_narrowing = sorted(
        [
            item
            for item in representatives
            if item["price"]["spreadChange"] is not None
        ],
        key=lambda item: item["price"]["spreadChange"],
    )
    top_widening = widening_ranked[0] if widening_ranked else (
        ranked_by_widening[0] if ranked_by_widening else None
    )
    top_narrowing = narrowing_ranked[0] if narrowing_ranked else (
        ranked_by_narrowing[0] if ranked_by_narrowing else None
    )

    return {
        "market": market_summary,
        "averageSpread": avg_spread,
        "averageSpreadChange": avg_spread_change,
        "averageCommonChange": avg_common_change,
        "averagePreferredChange": avg_preferred_change,
        "representativeCount": len(representatives),
        "topWidening": serialize_leader(top_widening),
        "topWideningRunners": [
            serialize_leader(item)
            for item in ranked_by_widening
            if top_widening is None or item["pair"]["id"] != top_widening["pair"]["id"]
        ][:4],
        "topNarrowing": serialize_leader(top_narrowing),
        "topNarrowingRunners": [
            serialize_leader(item)
            for item in ranked_by_narrowing
            if top_narrowing is None or item["pair"]["id"] != top_narrowing["pair"]["id"]
        ][:4],
    }


def main():
    previous_snapshot = read_json_file(OUTPUT_PATH) or {}
    previous_prices = previous_snapshot.get("prices", {})

    all_tickers = list(
        dict.fromkeys(
            ticker
            for pair in PAIRS
            for ticker in [pair["commonTicker"], pair["preferredTicker"]]
        )
    )
    all_codes = [ticker_to_code(ticker) for ticker in all_tickers]

    print(f"{len(all_codes)}개 종목 현재가 조회 중...")

    quotes, quote_errors, quote_providers = fetch_all_quotes(all_codes)
    market_quote, market_extras, market_providers = fetch_market_metrics(previous_snapshot)
    night_future_metric, night_future_provider = fetch_kospi200_metric(previous_snapshot)
    if night_future_provider:
        market_providers.add(night_future_provider)

    for code, error in quote_errors.items():
        print(f"  WARNING: {code} 현재가 조회 실패: {error}")

    prices = {}
    for pair in PAIRS:
        common_code = ticker_to_code(pair["commonTicker"])
        preferred_code = ticker_to_code(pair["preferredTicker"])
        common_quote = quotes.get(common_code)
        preferred_quote = quotes.get(preferred_code)

        if common_quote and preferred_quote:
            try:
                common_price = parse_int(
                    first_not_none(
                        common_quote.get("closePriceRaw"),
                        common_quote.get("closePrice"),
                    )
                )
                preferred_price = parse_int(
                    first_not_none(
                        preferred_quote.get("closePriceRaw"),
                        preferred_quote.get("closePrice"),
                    )
                )
                common_delta = parse_int(
                    first_not_none(
                        common_quote.get("compareToPreviousClosePriceRaw"),
                        common_quote.get("compareToPreviousClosePrice"),
                    )
                )
                preferred_delta = parse_int(
                    first_not_none(
                        preferred_quote.get("compareToPreviousClosePriceRaw"),
                        preferred_quote.get("compareToPreviousClosePrice"),
                    )
                )

                previous_common_price = (
                    common_price - common_delta
                    if common_price is not None and common_delta is not None
                    else None
                )
                previous_preferred_price = (
                    preferred_price - preferred_delta
                    if preferred_price is not None and preferred_delta is not None
                    else None
                )

                spread = compute_spread(common_price, preferred_price)
                previous_spread = compute_spread(
                    previous_common_price,
                    previous_preferred_price,
                )
                spread_change = (
                    round(spread - previous_spread, 2)
                    if spread is not None and previous_spread is not None
                    else None
                )

                prices[pair["id"]] = {
                    "commonPrice": common_price,
                    "preferredPrice": preferred_price,
                    "spread": spread,
                    "spreadChange": spread_change,
                    "commonChange": round_or_none(
                        parse_float(
                            first_not_none(
                                common_quote.get("fluctuationsRatioRaw"),
                                common_quote.get("fluctuationsRatio"),
                            )
                        )
                    ),
                    "preferredChange": round_or_none(
                        parse_float(
                            first_not_none(
                                preferred_quote.get("fluctuationsRatioRaw"),
                                preferred_quote.get("fluctuationsRatio"),
                            )
                        )
                    ),
                }
                continue
            except (TypeError, ValueError) as exc:
                print(f"  WARNING: {pair['name']} 현재가 파싱 실패: {exc}")

        if previous_prices.get(pair["id"]):
            prices[pair["id"]] = previous_prices[pair["id"]]
            print(f"  WARNING: {pair['name']} 기존 스냅샷 값 유지")
        else:
            print(f"  WARNING: {pair['name']} 현재가 조회 실패: 이전 값 없음")

    market_summary = build_market_summary(market_quote, market_extras, night_future_metric)
    summary = build_summary(prices, market_summary)
    avg_spread = summary["averageSpread"] if summary else None
    avg_spread_change = summary["averageSpreadChange"] if summary else None

    providers = quote_providers | market_providers
    if "kis" in providers and "naver" in providers:
        source = "한국투자증권 오픈 API + 네이버 보조지표"
    elif "kis" in providers:
        source = "한국투자증권 오픈 API"
    elif "public" in providers and "naver" in providers:
        source = "네이버 증권 + 공개 선물 시세"
    elif "public" in providers:
        source = "공개 선물 시세"
    else:
        source = "네이버 증권"

    result = {
        "source": source,
        "lastUpdated": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "prices": prices,
        "market": market_summary,
        "averageSpread": avg_spread,
        "averageSpreadChange": avg_spread_change,
        "summary": summary,
    }

    write_json_file(OUTPUT_PATH, result)

    print(
        "current.json 갱신 완료 "
        f"({len(prices)}개 종목, 평균 괴리율 {avg_spread}%, 전일비 {avg_spread_change}%p)"
    )


if __name__ == "__main__":
    main()
