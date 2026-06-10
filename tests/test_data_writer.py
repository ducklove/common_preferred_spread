"""data_writer 테스트 (표준 라이브러리 + pytest만 사용, tmp_path 기반 합성 데이터)."""

import json

import data_writer
from data_writer import atomic_write_text, parse_stock_data_js, write_stock_data_outputs

PREFIX = "const STOCK_DATA = "

SAMPLE = {"lastUpdated": "2026-06-10 05:00:00", "pairs": []}


def build_stock_data():
    """일반 pair 1개 + kospiPrice를 가진 _average pair 1개 + 배당 히스토리."""
    return {
        "lastUpdated": "2026-06-10 05:00:00",
        "dividendHistories": {
            "pair1": {
                "startDate": "2020-01-02",
                "endDate": "2020-01-03",
                "commonTicker": "000001.KS",
                "preferredTicker": "000002.KS",
                "commonName": "테스트",
                "preferredName": "테스트우",
                "common": [{"date": "2020-01-02", "amount": 150.0}],
                "preferred": [{"date": "2020-01-03", "amount": 155.5}],
            }
        },
        "pairs": [
            {
                "id": "pair1",
                "name": "테스트",
                "commonName": "테스트",
                "preferredName": "테스트우",
                "current": {"commonPrice": 1010, "preferredPrice": 810, "spread": 19.8},
                "history": [
                    {
                        "date": "2020-01-02",
                        "commonPrice": 1000,
                        "preferredPrice": 800,
                        "spread": 20.0,
                    },
                    {
                        "date": "2020-01-03",
                        "commonPrice": 1010,
                        "preferredPrice": 810,
                        "spread": 19.8,
                    },
                ],
            },
            {
                "id": "_average",
                "name": "괴리율 지수",
                "commonName": "",
                "preferredName": "",
                "isAverage": True,
                "current": {"commonPrice": 0, "preferredPrice": 0, "spread": 15.0},
                "history": [
                    {
                        "date": "2020-01-02",
                        "commonPrice": 0,
                        "preferredPrice": 0,
                        "spread": 15.2,
                        "kospiPrice": 2175.17,
                    },
                    {
                        "date": "2020-01-03",
                        "commonPrice": 0,
                        "preferredPrice": 0,
                        "spread": 15.0,
                    },
                ],
            },
        ],
    }


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# parse_stock_data_js
# ---------------------------------------------------------------------------


class TestParseStockDataJs:
    def test_parses_with_semicolon_newline_tail(self, tmp_path):
        path = tmp_path / "data.js"
        path.write_text(
            PREFIX + json.dumps(SAMPLE, ensure_ascii=False) + ";\n", encoding="utf-8"
        )

        assert parse_stock_data_js(path) == SAMPLE

    def test_parses_with_semicolon_only_tail(self, tmp_path):
        path = tmp_path / "data.js"
        path.write_text(
            PREFIX + json.dumps(SAMPLE, ensure_ascii=False) + ";", encoding="utf-8"
        )

        assert parse_stock_data_js(path) == SAMPLE

    def test_prefix_mismatch_returns_none(self, tmp_path):
        path = tmp_path / "data.js"
        path.write_text(
            "var STOCK_DATA = " + json.dumps(SAMPLE) + ";\n", encoding="utf-8"
        )

        assert parse_stock_data_js(path) is None

    def test_missing_file_returns_none(self, tmp_path):
        assert parse_stock_data_js(tmp_path / "no_such.js") is None

    def test_invalid_json_returns_none(self, tmp_path):
        path = tmp_path / "data.js"
        path.write_text(PREFIX + "{broken;\n", encoding="utf-8")

        assert parse_stock_data_js(path) is None


# ---------------------------------------------------------------------------
# atomic_write_text
# ---------------------------------------------------------------------------


class TestAtomicWriteText:
    def test_creates_parent_dirs_and_replaces_content(self, tmp_path):
        path = tmp_path / "nested" / "dir" / "out.txt"

        atomic_write_text(path, "첫번째")
        atomic_write_text(path, "두번째")

        assert path.read_text(encoding="utf-8") == "두번째"
        assert not path.with_name(path.name + ".tmp").exists()


# ---------------------------------------------------------------------------
# write_stock_data_outputs
# ---------------------------------------------------------------------------


class TestWriteStockDataOutputs:
    def test_data_js_is_compact_and_roundtrips(self, tmp_path):
        stock_data = build_stock_data()

        write_stock_data_outputs(stock_data, tmp_path)

        content = (tmp_path / "data.js").read_text(encoding="utf-8")
        assert content.startswith(PREFIX)
        assert content.endswith(";\n")
        # 콤팩트 직렬화: 개행은 파일 끝 1개뿐
        assert content.count("\n") == 1
        assert parse_stock_data_js(tmp_path / "data.js") == stock_data

    def test_summary_has_no_history_and_exact_meta(self, tmp_path):
        stock_data = build_stock_data()

        write_stock_data_outputs(stock_data, tmp_path)

        summary = read_json(tmp_path / "data" / "summary.json")
        assert summary["schemaVersion"] == data_writer.SCHEMA_VERSION
        assert summary["lastUpdated"] == "2026-06-10 05:00:00"
        assert [p["id"] for p in summary["pairs"]] == ["pair1", "_average"]
        for pair in summary["pairs"]:
            assert "history" not in pair
        # history 제외 나머지 필드는 그대로 유지
        original_pair1 = build_stock_data()["pairs"][0]
        original_pair1.pop("history")
        assert summary["pairs"][0] == original_pair1
        assert summary["historyMeta"] == {
            "pair1": {"start": "2020-01-02", "end": "2020-01-03", "points": 2},
            "_average": {"start": "2020-01-02", "end": "2020-01-03", "points": 2},
        }

    def test_history_files_match_source_history(self, tmp_path):
        stock_data = build_stock_data()

        write_stock_data_outputs(stock_data, tmp_path)

        pair1 = read_json(tmp_path / "data" / "history" / "pair1.json")
        assert pair1["schemaVersion"] == data_writer.SCHEMA_VERSION
        assert pair1["id"] == "pair1"
        assert pair1["lastUpdated"] == "2026-06-10 05:00:00"
        assert pair1["dates"] == ["2020-01-02", "2020-01-03"]
        assert pair1["common"] == [1000, 1010]
        assert pair1["preferred"] == [800, 810]
        assert pair1["spread"] == [20.0, 19.8]
        # kospiPrice가 없는 일반 pair에는 kospi 배열이 없다
        assert "kospi" not in pair1

        average = read_json(tmp_path / "data" / "history" / "_average.json")
        assert average["dates"] == ["2020-01-02", "2020-01-03"]
        assert average["spread"] == [15.2, 15.0]
        # kospiPrice가 있는 _average만 kospi 배열을 가지며, 누락일은 null
        assert average["kospi"] == [2175.17, None]
        for key in ("dates", "common", "preferred", "spread", "kospi"):
            assert len(average[key]) == 2

    def test_dividends_json_matches_source(self, tmp_path):
        stock_data = build_stock_data()

        write_stock_data_outputs(stock_data, tmp_path)

        dividends = read_json(tmp_path / "data" / "dividends.json")
        assert dividends == {
            "schemaVersion": data_writer.SCHEMA_VERSION,
            "lastUpdated": "2026-06-10 05:00:00",
            "dividendHistories": stock_data["dividendHistories"],
        }

    def test_stale_history_file_is_deleted(self, tmp_path):
        history_dir = tmp_path / "data" / "history"
        history_dir.mkdir(parents=True)
        stale = history_dir / "old_pair.json"
        stale.write_text("{}", encoding="utf-8")

        write_stock_data_outputs(build_stock_data(), tmp_path)

        assert not stale.exists()
        assert (history_dir / "pair1.json").exists()
        assert (history_dir / "_average.json").exists()

    def test_returned_info_keys_and_values(self, tmp_path):
        info = write_stock_data_outputs(build_stock_data(), tmp_path)

        assert set(info.keys()) == {
            "dataJsBytes",
            "summaryBytes",
            "historyFiles",
            "totalPoints",
        }
        assert info["historyFiles"] == 2
        assert info["totalPoints"] == 4
        assert info["dataJsBytes"] == len((tmp_path / "data.js").read_bytes())
        assert info["summaryBytes"] == len(
            (tmp_path / "data" / "summary.json").read_bytes()
        )

    def test_non_ascii_pair_id_history_file_skipped(self, tmp_path):
        stock_data = build_stock_data()
        stock_data["pairs"].append(
            {
                "id": "한글id",
                "name": "비ASCII",
                "commonName": "비ASCII",
                "preferredName": "비ASCII우",
                "current": {"commonPrice": 500, "preferredPrice": 400, "spread": 20.0},
                "history": [
                    {
                        "date": "2020-01-02",
                        "commonPrice": 500,
                        "preferredPrice": 400,
                        "spread": 20.0,
                    }
                ],
            }
        )

        info = write_stock_data_outputs(stock_data, tmp_path)

        assert not (tmp_path / "data" / "history" / "한글id.json").exists()
        # 안전한 id 2개(pair1, _average)만 히스토리 파일로 작성된다
        assert info["historyFiles"] == 2
        written = {p.name for p in (tmp_path / "data" / "history").glob("*.json")}
        assert written == {"pair1.json", "_average.json"}
        # summary/data.js에는 해당 pair가 그대로 포함된다
        summary = read_json(tmp_path / "data" / "summary.json")
        assert "한글id" in summary["historyMeta"]
        assert parse_stock_data_js(tmp_path / "data.js") == stock_data
