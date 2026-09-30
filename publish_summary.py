"""Value Compass 생태계 발행물: summary.json + version.json (데이터 계약 v1).

value-invest 허브가 우선주 괴리율 카드(TOP)와 종목 딥링크·포트폴리오 신호를 만들 때 쓰는
작은 요약본이다. current.json(장중 스냅샷) + config.json(페어 정의)에서 만들고, 저장소 루트에
두어 GitHub Pages ``https://ducklove.github.io/common_preferred_spread/summary.json`` 으로 나간다.
(대시보드 최초 로드용 ``data/summary.json`` 과는 다른 파일이다.)

- payload 규격: value-invest ``docs/ecosystem/data-contract.md`` §6.2,
  ``config/schemas/summary/common_preferred_spread.schema.json``.
  ``pairs`` 는 config.json 순서의 전체 목록이고, 허브가 TOP(괴리율 내림차순, 보통주당 1행)과
  종목코드 조회(commonCode/preferredCode 첫 일치)를 직접 유도한다.
- envelope/해시/원자적 쓰기: 벤더링된 ``vc_publish.py`` (직접 수정 금지, 허브에서 sync).
- no-op 규칙: current.json 의 ``lastUpdated`` 는 실행 시각이라 매번 바뀐다. 시세·괴리율이 이전
  summary 와 같으면 이전 ``lastUpdated``(= 이 값들이 처음 관측된 시각)를 이어 써서 contentHash 와
  asOf 가 그대로 유지되고, ``write_if_changed`` 가 파일을 다시 쓰지 않는다(git diff 없음).

실행: ``python publish_summary.py`` (네트워크 없음 — 로컬 파일만 읽는다)
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from pathlib import Path
from typing import Any, Mapping, Optional

import vc_publish

TOOL_ID = "common_preferred_spread"
ROOT = Path(__file__).resolve().parent
CURRENT_PATH = ROOT / "current.json"
CONFIG_PATH = ROOT / "config.json"
SUMMARY_PATH = ROOT / "summary.json"
VERSION_PATH = ROOT / "version.json"

_CODE_RE = re.compile(r"^[0-9A-Z]{6}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_LAST_UPDATED_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}(?::\d{2})?)$")

# current.json "source" 의 공급자 라벨 → envelope sources 항목 (시세에 쓰인 공급자만; LAN 주소는 넣지 않는다)
_SOURCE_CATALOG = (
    ("네이버 증권", {"id": "naver", "name": "네이버 증권", "url": "https://finance.naver.com/"}),
    ("한국투자증권", {"id": "kis", "name": "한국투자증권 오픈 API", "url": "https://apiportal.koreainvestment.com/"}),
    ("내부 가격 API", {"id": "finance-pi", "name": "finance-pi 내부 가격 API"}),
    ("내부 종가 백업", {"id": "finance-pi", "name": "finance-pi 내부 가격 API"}),
)
_DEFAULT_SOURCE = _SOURCE_CATALOG[0][1]


class SummaryError(ValueError):
    """발행 입력이 계약을 만족하지 못함 (이전 summary.json 을 그대로 둔다)."""


def _num(value: Any) -> Optional[float]:
    """유한한 숫자만 통과. 모르는 값은 0이 아니라 None (계약 §2)."""
    if value is None or isinstance(value, bool) or value == "":
        return None
    if isinstance(value, int):
        return value
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _code(ticker: Any) -> str:
    return str(ticker or "").strip().split(".", 1)[0].upper()


def _date(value: Any) -> Optional[str]:
    return value if isinstance(value, str) and _DATE_RE.match(value) else None


def _text(value: Any) -> Optional[str]:
    return value.strip() if isinstance(value, str) and value.strip() else None


def build_summary_data(current: Mapping[str, Any], config: list) -> dict:
    """current.json + config.json → summary ``data`` (계약 §6.2)."""
    if not isinstance(current, Mapping):
        raise SummaryError("current.json must be an object")
    if not isinstance(config, list):
        raise SummaryError("config.json must be a list of pairs")
    prices = current.get("prices") if isinstance(current.get("prices"), Mapping) else {}
    pairs = []
    for item in config:
        if not isinstance(item, Mapping) or not _text(item.get("id")):
            continue
        pair_id = item["id"].strip()
        common_code = _code(item.get("commonTicker"))
        preferred_code = _code(item.get("preferredTicker"))
        if not (_CODE_RE.match(common_code) and _CODE_RE.match(preferred_code)):
            print(f"[publish_summary] {pair_id}: 종목코드 형식 불일치로 제외 ({common_code}/{preferred_code})", file=sys.stderr)
            continue
        quote = prices.get(pair_id) if isinstance(prices.get(pair_id), Mapping) else {}
        pairs.append({
            "id": pair_id,
            "name": _text(item.get("name")) or _text(item.get("commonName")) or pair_id,
            "preferredName": _text(item.get("preferredName")),
            "commonCode": common_code,
            "preferredCode": preferred_code,
            "spread": _num(quote.get("spread")),
            "spreadChange": _num(quote.get("spreadChange")),
            "commonPrice": _num(quote.get("commonPrice")),
            "preferredPrice": _num(quote.get("preferredPrice")),
            "date": _date(quote.get("date")),
        })
    if not pairs:
        raise SummaryError("no publishable pairs")
    return {
        "lastUpdated": _text(current.get("lastUpdated")),
        "averageSpread": _num(current.get("averageSpread")),
        "averageSpreadChange": _num(current.get("averageSpreadChange")),
        "indexSpread": _num(current.get("indexSpread")),
        "indexSpreadChange": _num(current.get("indexSpreadChange")),
        "pairs": pairs,
    }


def _without_last_updated(data: Mapping[str, Any]) -> dict:
    return {k: v for k, v in data.items() if k != "lastUpdated"}


def carry_forward_last_updated(data: dict, previous: Any) -> dict:
    """값이 이전 summary 와 같으면 이전 lastUpdated 를 이어 쓴다 (no-op 규칙 §8 ②)."""
    if not isinstance(previous, Mapping) or previous.get("tool") != TOOL_ID:
        return data
    prev_data = previous.get("data")
    if not isinstance(prev_data, Mapping) or not _text(prev_data.get("lastUpdated")):
        return data
    try:
        same = vc_publish.content_hash(_without_last_updated(prev_data)) == vc_publish.content_hash(_without_last_updated(data))
    except vc_publish.EnvelopeError:
        return data
    return {**data, "lastUpdated": prev_data["lastUpdated"]} if same else data


def derive_as_of(data: Mapping[str, Any]) -> str:
    """asOf = 시세 스냅샷 시각(KST). 실행 시각이 아니라 이 값들이 관측된 시각이다."""
    match = _LAST_UPDATED_RE.match(data.get("lastUpdated") or "")
    if match:
        clock = match.group(2) if len(match.group(2)) == 8 else match.group(2) + ":00"
        return f"{match.group(1)}T{clock}+09:00"
    dates = sorted(p["date"] for p in data.get("pairs", []) if p.get("date"))
    if dates:
        return dates[-1]
    raise SummaryError("cannot derive asOf (no lastUpdated and no quote dates)")


def build_sources(current: Mapping[str, Any]) -> list:
    label = str(current.get("source") or "")
    sources, seen = [], set()
    for keyword, source in _SOURCE_CATALOG:
        if keyword in label and source["id"] not in seen:
            sources.append(dict(source))
            seen.add(source["id"])
    return sources or [dict(_DEFAULT_SOURCE)]


def _read_json(path: Path) -> Any:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _read_previous(path: Path) -> Any:
    try:
        return _read_json(path)
    except (OSError, ValueError):
        return None


def publish(
    current_path: Path = CURRENT_PATH,
    config_path: Path = CONFIG_PATH,
    summary_path: Path = SUMMARY_PATH,
    version_path: Path = VERSION_PATH,
    *,
    generated_at: Optional[str] = None,
) -> dict:
    """summary.json/version.json 을 필요할 때만 쓴다. {'summaryChanged','versionChanged','envelope'} 반환."""
    current = _read_json(Path(current_path))
    config = _read_json(Path(config_path))
    data = carry_forward_last_updated(build_summary_data(current, config), _read_previous(Path(summary_path)))
    envelope = vc_publish.build_envelope(
        TOOL_ID, data,
        as_of=derive_as_of(data),
        sources=build_sources(current),
        generated_at=generated_at,
    )
    summary_changed = vc_publish.write_if_changed(summary_path, envelope)
    version_changed = vc_publish.write_version(version_path, {"summary.json": envelope}, generated_at=generated_at)
    return {"summaryChanged": summary_changed, "versionChanged": version_changed, "envelope": envelope}


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="생태계 summary.json/version.json 발행 (오프라인)")
    parser.add_argument("--current", type=Path, default=CURRENT_PATH)
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    parser.add_argument("--out-dir", type=Path, default=ROOT)
    args = parser.parse_args(argv)
    try:
        result = publish(args.current, args.config, args.out_dir / "summary.json", args.out_dir / "version.json")
    except (OSError, ValueError) as exc:  # SummaryError·EnvelopeError·JSON 오류 포함
        # 발행 실패 = 이전 파일 유지. current.json 커밋은 막지 않되 경고로 드러낸다.
        print(f"::warning::summary.json 발행 실패 — 이전 파일 유지: {exc}")
        return 0
    changed = result["summaryChanged"]
    envelope = result["envelope"]
    print(
        f"summary.json {'갱신' if changed else '변경 없음'} "
        f"(asOf {envelope['asOf']}, {len(envelope['data']['pairs'])}개 페어, {envelope['contentHash'][:19]}…)"
    )
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as fh:
            fh.write(f"summary_changed={'true' if changed else 'false'}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
