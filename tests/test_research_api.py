import hashlib
import json

import pytest

from research_api import build_catalog, publish


def inputs():
    return ([{"id":"samsung", "name":"삼성", "commonTicker":"005930.KS", "preferredTicker":"005935.KS"}],
            {"pairs":[{"id":"samsung"}], "lastUpdated":"2026-09-16 05:00:00"},
            {"profiles":{"standard":{"convertible":False}},"byId":{"samsung":{"profile":"standard","confidence":"medium"}}}, {}, {})


def test_owned_rights_are_exported_without_execution_approval():
    r = build_catalog(*inputs())
    assert r["pairs"][0]["rights"]["convertible"] is False
    assert r["pairs"][0]["point_in_time_verified"] is False
    assert r["execution_eligible"] is False


def test_duplicate_pair_is_rejected():
    values = list(inputs())
    values[0] *= 2
    with pytest.raises(ValueError):
        build_catalog(*values)


def test_repeat_publish_retains_immutable_snapshot_and_manifest(tmp_path):
    files = ["config.json","data/summary.json","data/preferred_terms.json","data/pair_meta.json","data/dividends.json"]
    for path, data in zip(files, inputs(), strict=True):
        p = tmp_path/path
        p.parent.mkdir(parents=True,exist_ok=True)
        p.write_text(json.dumps(data),encoding="utf-8")
    sha, count = publish(tmp_path)
    manifest = (tmp_path/'data/research/v1/manifest.json').read_bytes()
    assert count == 1
    assert hashlib.sha256((tmp_path/f'data/research/v1/snapshots/{sha}.json').read_bytes()).hexdigest() == sha
    assert publish(tmp_path)[0] == sha
    assert (tmp_path/'data/research/v1/manifest.json').read_bytes() == manifest
