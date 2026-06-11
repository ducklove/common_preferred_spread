"""fetch_current의 sqrt 우선주 시총가중 괴리율 지수 계산/요약 메타 로더 테스트."""

import json

# websocket 미설치 환경에서도 try/except 가드로 import가 가능해야 한다.
import fetch_current


def test_module_importable_without_websocket():
    """websocket이 없어도 모듈 import가 되고 지수 관련 함수가 노출된다."""
    assert hasattr(fetch_current, "compute_sqrt_index_spread")
    assert hasattr(fetch_current, "load_pair_meta_from_summary")
    assert hasattr(fetch_current, "build_index_entries")


# --- compute_sqrt_index_spread ---


def test_compute_sqrt_index_spread_hand_calculated():
    """손계산: 발행사 A spread 17.5/weight √400=20, 발행사 B spread 30/weight 20 → 23.75."""
    entries = [
        {"commonName": "A", "spread": 10.0, "preferredMarketCap": 100.0},
        {"commonName": "A", "spread": 20.0, "preferredMarketCap": 300.0},
        {"commonName": "B", "spread": 30.0, "preferredMarketCap": 400.0},
    ]
    # A: (10*100 + 20*300) / 400 = 17.5, weight √400 = 20
    # B: 30, weight √400 = 20
    # 지수: (17.5*20 + 30*20) / 40 = 23.75
    assert fetch_current.compute_sqrt_index_spread(entries) == 23.75


def test_compute_sqrt_index_spread_single_issuer():
    """단일 발행사면 발행사 내 시총 가중평균이 그대로 지수가 된다."""
    entries = [
        {"commonName": "A", "spread": 10.0, "preferredMarketCap": 100.0},
        {"commonName": "A", "spread": 20.0, "preferredMarketCap": 300.0},
    ]
    assert fetch_current.compute_sqrt_index_spread(entries) == 17.5


def test_compute_sqrt_index_spread_all_invalid_returns_none():
    """유효 표본이 하나도 없으면 None."""
    assert fetch_current.compute_sqrt_index_spread([]) is None
    assert fetch_current.compute_sqrt_index_spread(None) is None
    entries = [
        {"commonName": "A", "spread": None, "preferredMarketCap": 100.0},
        {"commonName": "B", "spread": "비숫자", "preferredMarketCap": 100.0},
        {"commonName": "C", "spread": 10.0, "preferredMarketCap": None},
        {"commonName": "D", "spread": 10.0, "preferredMarketCap": 0},
        {"commonName": "E", "spread": 10.0, "preferredMarketCap": -5.0},
        None,
        "not-a-dict",
    ]
    assert fetch_current.compute_sqrt_index_spread(entries) is None


def test_compute_sqrt_index_spread_excludes_zero_or_none_market_cap():
    """preferredMarketCap이 0/None인 entry는 제외하고 나머지로만 계산한다."""
    entries = [
        {"commonName": "A", "spread": 10.0, "preferredMarketCap": 0},
        {"commonName": "A", "spread": 99.0, "preferredMarketCap": None},
        {"commonName": "B", "spread": 30.0, "preferredMarketCap": 400.0},
    ]
    assert fetch_current.compute_sqrt_index_spread(entries) == 30.0


def test_compute_sqrt_index_spread_excludes_none_spread():
    """spread가 None인 entry는 발행사 내 가중치 계산에서도 제외된다."""
    entries = [
        {"commonName": "A", "spread": None, "preferredMarketCap": 1000000.0},
        {"commonName": "A", "spread": 10.0, "preferredMarketCap": 100.0},
        {"commonName": "B", "spread": 30.0, "preferredMarketCap": 400.0},
    ]
    # A: spread None인 거대 시총 entry 제외 → spread 10, weight √100 = 10
    # B: spread 30, weight √400 = 20
    # 지수: (10*10 + 30*20) / 30 = 700/30 = 23.333... → 23.33
    assert fetch_current.compute_sqrt_index_spread(entries) == 23.33


def test_compute_sqrt_index_spread_rounds_to_two_digits():
    """결과는 소수 둘째 자리로 반올림."""
    entries = [
        {"commonName": "A", "spread": 10.0, "preferredMarketCap": 1.0},
        {"commonName": "B", "spread": 20.0, "preferredMarketCap": 4.0},
    ]
    # (10*1 + 20*2) / 3 = 50/3 = 16.666... → 16.67
    assert fetch_current.compute_sqrt_index_spread(entries) == 16.67


# --- load_pair_meta_from_summary ---


def _write_summary(tmp_path, payload):
    path = tmp_path / "summary.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def test_load_pair_meta_from_summary_normal(tmp_path):
    payload = {
        "schemaVersion": 1,
        "pairs": [
            {
                "id": "_average",
                "name": "괴리율 지수",
                "commonName": "",
                "isAverage": True,
                "current": {"spread": 46.42, "spreadChange": -0.32},
            },
            {
                "id": "pair_a",
                "commonName": "발행사A",
                "current": {
                    "preferredSharesOutstanding": 13364200,
                    "preferredMarketCap": 107000000000,
                },
            },
            {
                "id": "pair_b",
                "commonName": "발행사B",
                "current": {"preferredMarketCap": 5000},
            },
        ],
    }
    path = _write_summary(tmp_path, payload)

    pair_meta, prev_index_spread = fetch_current.load_pair_meta_from_summary(path)

    assert prev_index_spread == 46.42
    assert "_average" not in pair_meta
    assert pair_meta["pair_a"]["commonName"] == "발행사A"
    assert pair_meta["pair_a"]["preferredSharesOutstanding"] == 13364200
    assert pair_meta["pair_a"]["preferredMarketCap"] == 107000000000
    assert pair_meta["pair_b"]["preferredSharesOutstanding"] is None
    assert pair_meta["pair_b"]["preferredMarketCap"] == 5000


def test_load_pair_meta_from_summary_missing_file(tmp_path):
    pair_meta, prev_index_spread = fetch_current.load_pair_meta_from_summary(
        tmp_path / "missing.json"
    )
    assert pair_meta == {}
    assert prev_index_spread is None


def test_load_pair_meta_from_summary_broken_json(tmp_path):
    path = tmp_path / "summary.json"
    path.write_text("{이건 JSON이 아님", encoding="utf-8")

    pair_meta, prev_index_spread = fetch_current.load_pair_meta_from_summary(path)
    assert pair_meta == {}
    assert prev_index_spread is None


# --- build_index_entries ---


def test_build_index_entries_prefers_live_market_cap_with_summary_fallback():
    pairs = [
        {"id": "p1", "commonName": "발행사A"},
        {"id": "p2", "commonName": "발행사B"},
        {"id": "p3", "commonName": "발행사C"},
    ]
    prices = {
        "p1": {"preferredPrice": 8000, "spread": 88.85},
        "p2": {"preferredPrice": None, "spread": 30.0},
        # p3: 시세 없음 → spread/mcap 모두 None entry
    }
    pair_meta = {
        "p1": {
            "commonName": "발행사A",
            "preferredSharesOutstanding": 100.0,
            "preferredMarketCap": 999.0,
        },
        "p2": {
            "commonName": "발행사B",
            "preferredSharesOutstanding": 100.0,
            "preferredMarketCap": 5000.0,
        },
    }

    entries = fetch_current.build_index_entries(prices, pair_meta, pairs)

    assert entries == [
        # 실시간 우선주가 × 상장주식수 우선
        {"commonName": "발행사A", "spread": 88.85, "preferredMarketCap": 800000.0},
        # 가격 없음 → summary의 preferredMarketCap 폴백
        {"commonName": "발행사B", "spread": 30.0, "preferredMarketCap": 5000.0},
        # 메타/시세 모두 없음 → compute에서 자연 제외되는 무효 entry
        {"commonName": "발행사C", "spread": None, "preferredMarketCap": None},
    ]
