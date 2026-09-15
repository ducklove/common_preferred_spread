"""전문 프로젝트가 소유하는 연구 대상·권리·배당의 버전 JSON API."""

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from data_writer import atomic_write_text


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False, separators=(",", ":"))


def build_catalog(config, summary, terms, meta, dividends):
    available = {p["id"] for p in summary["pairs"]}
    pairs = []
    seen = set()
    for entry in config:
        a, b = entry["commonTicker"], entry["preferredTicker"]
        if not re.fullmatch(r"\d{6}\.KS", a) or not re.fullmatch(r"[0-9A-Z]{6}\.KS", b):
            continue
        if entry["id"] not in available:
            continue
        key = (a[:6], b[:6])
        if key in seen or key[0] == key[1]:
            raise ValueError("중복되거나 동일한 연구 종목 쌍입니다.")
        seen.add(key)
        override = terms.get("byId", {}).get(entry["id"], {})
        rights = {**terms.get("profiles", {}).get(override.get("profile"), {}), **override}
        pairs.append({
            "id": entry["id"], "common": key[0], "preferred": key[1],
            "name": entry["name"], "strategy": "preferred_switch",
            "classification": "common_preferred_research_candidate",
            "point_in_time_verified": False, "execution_eligible": False,
            "rights": rights,
            "listing_review": meta.get("byId", {}).get(entry["id"], {}),
            "reviewed_at": terms.get("lastReviewed"),
            "sources": {"rights": terms.get("sources", {}).get(rights.get("sourceKey")),
                        "listing": meta.get("sources", {})},
        })
    if not pairs:
        raise ValueError("게시할 국내 연구 대상이 없습니다.")
    return {
        "schema_version": 1, "provider": "common_preferred_spread",
        "data_as_of": summary["lastUpdated"], "pairs": pairs,
        "dividends": dividends.get("dividendHistories", {}),
        "dividend_basis": "source_observations_not_total_return_series",
        "point_in_time_verified": False, "execution_eligible": False,
        "limitations": ["현재 확인된 종목 관계와 권리 검토이며 과거 시점의 유효성을 보장하지 않습니다.",
                        "배당 이력은 권리일·지급일·수정 기준 검증 전이며 자동으로 총수익에 합산하지 않습니다."],
    }


def publish(root):
    def read(path):
        return json.loads((root / path).read_text(encoding="utf-8"))
    catalog = build_catalog(read("config.json"), read("data/summary.json"),
                            read("data/preferred_terms.json"), read("data/pair_meta.json"),
                            read("data/dividends.json"))
    content = encode(catalog)
    sha = hashlib.sha256(content.encode()).hexdigest()
    directory = root / "data/research/v1"
    snapshot = directory / f"snapshots/{sha}.json"
    if snapshot.exists() and snapshot.read_text(encoding="utf-8") != content:
        raise ValueError("기존 스냅샷을 덮어쓸 수 없습니다.")
    if not snapshot.exists():
        atomic_write_text(snapshot, content)
    manifest = {"schema_version": 1, "provider": catalog["provider"], "snapshot_id": sha,
                "path": f"snapshots/{sha}.json", "data_as_of": catalog["data_as_of"],
                "published_at": datetime.now(timezone.utc).isoformat(), "max_age_days": 14}
    current = directory / "manifest.json"
    # 같은 입력으로 다시 빌드해도 관측 가능 시각을 새로 꾸미지 않는다.
    if not current.exists() or read("data/research/v1/manifest.json")["snapshot_id"] != sha:
        atomic_write_text(current, encode(manifest))
    return sha, len(catalog["pairs"])


if __name__ == "__main__":
    print(publish(Path(__file__).parent))
