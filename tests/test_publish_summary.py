"""publish_summary 테스트 — 생태계 summary.json/version.json (데이터 계약 v1). 네트워크 없음, tmp_path 기반."""

import json
import math
from pathlib import Path

import pytest

import publish_summary as ps
import vc_publish

REPO_ROOT = Path(__file__).resolve().parents[1]
GEN = "2026-09-30T09:00:00+09:00"

CONFIG = [
    {"id": "samsung_elec", "name": "삼성전자", "commonTicker": "005930.KS", "preferredTicker": "005935.KS",
     "commonName": "삼성전자", "preferredName": "삼성전자우"},
    {"id": "lg", "name": "LG", "commonTicker": "003550.KS", "preferredTicker": "003555.KS",
     "commonName": "LG", "preferredName": "LG우"},
    {"id": "new_pair", "name": "신규", "commonTicker": "000001.KS", "preferredTicker": "000005.KS",
     "commonName": "신규", "preferredName": "신규우"},
]


def make_current(**overrides):
    current = {
        "source": "한국투자증권 오픈 API + 네이버 증권 + eSignal 선물 시세",
        "lastUpdated": "2026-09-30 10:33:04",
        "prices": {
            "samsung_elec": {"date": "2026-09-30", "commonPrice": 286500, "preferredPrice": 222000,
                             "spread": 22.51, "spreadChange": -1.0, "commonChange": 3.62},
            "lg": {"date": "2026-09-30", "commonPrice": 80000, "preferredPrice": 60000.0,
                   "spread": 25.0, "spreadChange": float("nan")},
            "_average": {"spread": 30},
        },
        "market": {"KOSPI": 3000},
        "averageSpread": 46.4,
        "averageSpreadChange": -0.26,
        "indexSpread": 38.96,
        "indexSpreadChange": None,
        "summary": {},
    }
    current.update(overrides)
    return current


def write_inputs(tmp_path, current=None, config=None):
    cur = tmp_path / "current.json"
    cfg = tmp_path / "config.json"
    cur.write_text(json.dumps(current or make_current(), ensure_ascii=False, allow_nan=True), encoding="utf-8")
    cfg.write_text(json.dumps(config or CONFIG, ensure_ascii=False), encoding="utf-8")
    return cur, cfg


def run(tmp_path, cur, cfg):
    return ps.publish(cur, cfg, tmp_path / "summary.json", tmp_path / "version.json", generated_at=GEN)


def test_build_summary_data_keeps_config_order_and_nulls_unknowns():
    data = ps.build_summary_data(make_current(), CONFIG)
    assert [p["id"] for p in data["pairs"]] == ["samsung_elec", "lg", "new_pair"]
    samsung, lg, new = data["pairs"]
    assert samsung == {
        "id": "samsung_elec", "name": "삼성전자", "preferredName": "삼성전자우",
        "commonCode": "005930", "preferredCode": "005935",
        "spread": 22.51, "spreadChange": -1.0, "commonPrice": 286500, "preferredPrice": 222000,
        "date": "2026-09-30",
    }
    assert lg["spreadChange"] is None  # NaN → null (0으로 채우지 않는다)
    assert new["spread"] is None and new["commonPrice"] is None and new["date"] is None
    assert data["lastUpdated"] == "2026-09-30 10:33:04"
    assert data["averageSpread"] == 46.4 and data["indexSpreadChange"] is None
    assert "_average" not in {p["id"] for p in data["pairs"]}


def test_build_summary_data_drops_invalid_codes_and_rejects_empty():
    bad = [{"id": "x", "name": "X", "commonTicker": "BRK.B", "preferredTicker": "005935.KS"}]
    with pytest.raises(ps.SummaryError):
        ps.build_summary_data(make_current(), bad)
    data = ps.build_summary_data(make_current(), CONFIG + bad)
    assert [p["id"] for p in data["pairs"]] == ["samsung_elec", "lg", "new_pair"]


def test_envelope_matches_contract(tmp_path):
    cur, cfg = write_inputs(tmp_path)
    result = run(tmp_path, cur, cfg)
    assert result["summaryChanged"] is True and result["versionChanged"] is True
    text = (tmp_path / "summary.json").read_text(encoding="utf-8")
    env = json.loads(text)
    assert text == vc_publish.dumps_compact(env) + "\n"  # 발행 파일 = canonical envelope + 개행
    vc_publish.validate_envelope(env)
    assert env["tool"] == "common_preferred_spread" and env["kind"] == "summary"
    assert env["asOf"] == "2026-09-30T10:33:04+09:00"  # 스냅샷 시각, 실행 시각 아님
    assert env["generatedAt"] == GEN
    assert [s["id"] for s in env["sources"]] == ["naver", "kis"]
    for key in ("lastUpdated", "averageSpread", "averageSpreadChange", "pairs"):
        assert key in env["data"]
    version = json.loads((tmp_path / "version.json").read_text(encoding="utf-8"))
    assert version["files"] == {"summary.json": env["contentHash"]}
    assert version["tool"] == "common_preferred_spread"


def test_unchanged_rerun_does_not_rewrite(tmp_path):
    cur, cfg = write_inputs(tmp_path)
    run(tmp_path, cur, cfg)
    summary, version = tmp_path / "summary.json", tmp_path / "version.json"
    before = (summary.read_bytes(), version.read_bytes(), summary.stat().st_mtime_ns, version.stat().st_mtime_ns)

    result = ps.publish(cur, cfg, summary, version, generated_at="2026-09-30T11:00:00+09:00")
    assert result["summaryChanged"] is False and result["versionChanged"] is False
    assert (summary.read_bytes(), version.read_bytes(), summary.stat().st_mtime_ns, version.stat().st_mtime_ns) == before


def test_timestamp_only_change_carries_forward_last_updated(tmp_path):
    cur, cfg = write_inputs(tmp_path)
    run(tmp_path, cur, cfg)
    before = (tmp_path / "summary.json").read_bytes()
    # 장 마감 후 재실행: 시세는 같고 current.json lastUpdated(실행 시각)와 공급자만 바뀜
    cur, cfg = write_inputs(tmp_path, make_current(lastUpdated="2026-09-30 21:03:10", source="네이버 증권"))
    result = run(tmp_path, cur, cfg)
    assert result["summaryChanged"] is False
    assert (tmp_path / "summary.json").read_bytes() == before
    env = json.loads(before)
    assert env["data"]["lastUpdated"] == "2026-09-30 10:33:04"
    assert env["asOf"] == "2026-09-30T10:33:04+09:00"


def test_price_change_rewrites_with_new_snapshot_time(tmp_path):
    cur, cfg = write_inputs(tmp_path)
    first = run(tmp_path, cur, cfg)["envelope"]
    current = make_current(lastUpdated="2026-09-30 11:03:00")
    current["prices"]["samsung_elec"]["spread"] = 23.0
    cur, cfg = write_inputs(tmp_path, current)
    result = run(tmp_path, cur, cfg)
    assert result["summaryChanged"] is True and result["versionChanged"] is True
    env = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert env["contentHash"] != first["contentHash"]
    assert env["asOf"] == "2026-09-30T11:03:00+09:00"
    assert env["data"]["pairs"][0]["spread"] == 23.0


def test_as_of_falls_back_to_latest_quote_date():
    data = ps.build_summary_data(make_current(lastUpdated=None), CONFIG)
    assert ps.derive_as_of(data) == "2026-09-30"
    data["pairs"] = [dict(p, date=None) for p in data["pairs"]]
    with pytest.raises(ps.SummaryError):
        ps.derive_as_of(data)


def test_main_keeps_previous_file_on_bad_input(tmp_path, capsys):
    cur, cfg = write_inputs(tmp_path)
    assert ps.main(["--current", str(cur), "--config", str(cfg), "--out-dir", str(tmp_path)]) == 0
    before = (tmp_path / "summary.json").read_bytes()
    cfg.write_text("[]", encoding="utf-8")
    assert ps.main(["--current", str(cur), "--config", str(cfg), "--out-dir", str(tmp_path)]) == 0
    assert "::warning::" in capsys.readouterr().out
    assert (tmp_path / "summary.json").read_bytes() == before


def test_committed_summary_is_valid_and_matches_committed_inputs():
    env = json.loads((REPO_ROOT / "summary.json").read_text(encoding="utf-8"))
    vc_publish.validate_envelope(env)
    assert env["tool"] == ps.TOOL_ID
    assert len((REPO_ROOT / "summary.json").read_bytes()) < 64 * 1024  # 계약 §9 크기 예산
    for pair in env["data"]["pairs"]:
        assert all(isinstance(pair[k], str) and len(pair[k]) == 6 for k in ("commonCode", "preferredCode"))
        for key in ("spread", "spreadChange", "commonPrice", "preferredPrice"):
            assert pair[key] is None or (isinstance(pair[key], (int, float)) and math.isfinite(pair[key]))
    current = json.loads((REPO_ROOT / "current.json").read_text(encoding="utf-8"))
    config = json.loads((REPO_ROOT / "config.json").read_text(encoding="utf-8"))
    rebuilt = ps.build_summary_data(current, config)
    # lastUpdated 는 no-op 규칙으로 이전 값이 이어질 수 있어 비교에서 뺀다
    assert ps._without_last_updated(rebuilt) == ps._without_last_updated(env["data"])
    version = json.loads((REPO_ROOT / "version.json").read_text(encoding="utf-8"))
    assert version["files"]["summary.json"] == env["contentHash"]
