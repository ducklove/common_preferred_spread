#!/usr/bin/env python3
"""
STOCK_DATA 출력 모듈
data.js(레거시 호환)와 data/ 분할 출력(summary/history/dividends)을 원자적으로 생성한다.

표준 라이브러리만 사용한다 (pandas/yfinance 불필요, fetch_data.py를 import하지 않음).
부트스트랩/복구: python3 data_writer.py --migrate (기존 data.js를 읽어 전체 출력 재생성)
"""

import argparse
import json
import math
import os
import re
from pathlib import Path

SCHEMA_VERSION = 1
SAFE_PAIR_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


def parse_stock_data_js(path):
    """data.js를 읽어 STOCK_DATA 딕셔너리로 파싱한다. 파일이 없거나 파싱 실패 시 None 반환."""
    path = Path(path)
    if not path.exists():
        return None
    try:
        content = path.read_text(encoding="utf-8")
        prefix = "const STOCK_DATA = "
        if not content.startswith(prefix):
            return None
        json_str = content[len(prefix):]
        if json_str.endswith(";\n"):
            json_str = json_str[:-2]
        elif json_str.endswith(";"):
            json_str = json_str[:-1]
        return json.loads(json_str)
    except (OSError, json.JSONDecodeError, ValueError):
        return None


def atomic_write_text(path, text):
    """같은 디렉터리에 tmp 파일을 쓴 뒤 os.replace로 원자적으로 교체한다."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp_path, path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise


def dump_compact_json(obj):
    """프런트엔드 계약에 맞춘 콤팩트 JSON 직렬화."""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def compute_spread_stats(history):
    """history의 spread 유효 값(숫자, NaN 제외)으로 분포 통계를 계산한다.

    프런트엔드 calculateMeanStd와 동일하게 모집단 표준편차(÷n)를 사용하며,
    유효 표본이 2개 미만이면 None을 반환한다(이때 spreadStats 키 자체를 생략).
    """
    values = []
    for record in history:
        value = record.get("spread")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        if math.isnan(value):
            continue
        values.append(value)
    count = len(values)
    if count < 2:
        return None
    mean = sum(values) / count
    variance = sum((v - mean) ** 2 for v in values) / count
    return {
        "mean": round(mean, 4),
        "std": round(math.sqrt(variance), 4),
        "min": round(min(values), 4),
        "max": round(max(values), 4),
        "count": count,
    }


def build_history_payload(pair, last_updated):
    """pair의 history 레코드를 컬럼 배열(dates/common/preferred/spread[/kospi])로 변환한다."""
    history = pair.get("history", [])
    payload = {
        "schemaVersion": SCHEMA_VERSION,
        "id": pair["id"],
        "lastUpdated": last_updated,
        "dates": [h["date"] for h in history],
        "common": [h["commonPrice"] for h in history],
        "preferred": [h["preferredPrice"] for h in history],
        "spread": [h["spread"] for h in history],
    }
    if any("kospiPrice" in h for h in history):
        payload["kospi"] = [h.get("kospiPrice") for h in history]
    return payload


def write_stock_data_outputs(stock_data, repo_root):
    """data.js + data/summary.json + data/history/*.json + data/dividends.json을 원자적으로 쓴다."""
    repo_root = Path(repo_root)
    data_dir = repo_root / "data"
    history_dir = data_dir / "history"
    last_updated = stock_data.get("lastUpdated")
    pairs = stock_data.get("pairs", [])

    # (a) data.js: 레거시 호환(전체 구조 유지), 콤팩트 직렬화
    js_content = "const STOCK_DATA = " + dump_compact_json(stock_data) + ";\n"
    atomic_write_text(repo_root / "data.js", js_content)
    data_js_bytes = len(js_content.encode("utf-8"))

    # (b) data/summary.json: history 제외 pair 목록 + 히스토리 메타
    summary_pairs = []
    history_meta = {}
    total_points = 0
    for pair in pairs:
        summary_pairs.append({k: v for k, v in pair.items() if k != "history"})
        history = pair.get("history", [])
        if history:
            history_meta[pair["id"]] = {
                "start": history[0]["date"],
                "end": history[-1]["date"],
                "points": len(history),
            }
            total_points += len(history)
    summary_text = dump_compact_json(
        {
            "schemaVersion": SCHEMA_VERSION,
            "lastUpdated": last_updated,
            "pairs": summary_pairs,
            "historyMeta": history_meta,
        }
    )
    atomic_write_text(data_dir / "summary.json", summary_text)
    summary_bytes = len(summary_text.encode("utf-8"))

    # (c) data/history/<pairId>.json: stale 파일 정리 후 pair별 작성
    history_dir.mkdir(parents=True, exist_ok=True)
    current_pair_ids = {pair["id"] for pair in pairs}
    for stale_path in history_dir.glob("*.json"):
        if stale_path.stem not in current_pair_ids:
            stale_path.unlink()
            print(f"  INFO: stale 히스토리 파일 삭제 {stale_path.name}")

    history_files = 0
    for pair in pairs:
        if not pair.get("history"):
            continue
        if not SAFE_PAIR_ID_PATTERN.match(pair["id"]):
            print(f"  WARNING: 파일명으로 쓸 수 없는 pair id 건너뜀: {pair['id']}")
            continue
        payload = build_history_payload(pair, last_updated)
        atomic_write_text(history_dir / f"{pair['id']}.json", dump_compact_json(payload))
        history_files += 1

    # (d) data/dividends.json: 배당 히스토리 그대로
    atomic_write_text(
        data_dir / "dividends.json",
        dump_compact_json(
            {
                "schemaVersion": SCHEMA_VERSION,
                "lastUpdated": last_updated,
                "dividendHistories": stock_data.get("dividendHistories", {}),
            }
        ),
    )

    return {
        "dataJsBytes": data_js_bytes,
        "summaryBytes": summary_bytes,
        "historyFiles": history_files,
        "totalPoints": total_points,
    }


def main():
    parser = argparse.ArgumentParser(description="STOCK_DATA 분할 출력 도구")
    parser.add_argument(
        "--migrate",
        action="store_true",
        help="repo 루트의 기존 data.js를 읽어 data.js(콤팩트)와 data/ 출력을 재생성합니다",
    )
    args = parser.parse_args()

    if not args.migrate:
        parser.print_help()
        return

    repo_root = Path(__file__).parent
    stock_data = parse_stock_data_js(repo_root / "data.js")
    if stock_data is None:
        print(f"ERROR: {repo_root / 'data.js'} 파싱 실패 또는 파일 없음")
        raise SystemExit(1)

    info = write_stock_data_outputs(stock_data, repo_root)
    print(
        f"마이그레이션 완료: data.js {info['dataJsBytes']:,} bytes, "
        f"summary {info['summaryBytes']:,} bytes, "
        f"history {info['historyFiles']}개 파일 ({info['totalPoints']:,} 포인트)"
    )


if __name__ == "__main__":
    main()
