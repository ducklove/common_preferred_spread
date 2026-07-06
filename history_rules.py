#!/usr/bin/env python3
"""
히스토리 보존/품질 가드 규칙 모듈 — fin-commons 재수출 파사드 + 로컬 병합 규칙

가드 구현은 fin-commons의 guards 모듈로 승격되었다
(https://github.com/ducklove/fin-commons — 함수명·시그니처·위반 메시지 동일).
기존 임포트 경로(`import history_rules`)를 보존하기 위한 재수출이며,
tests/test_history_rules.py가 동작 동일성을 계속 검증한다.

- merge_preserved_history: 기존 데이터의 더 이른 과거 구간을 새 히스토리 앞에 보존
- find_quality_violations: 이전 데이터 대비 히스토리 유실/시작일 후퇴/포인트 급감 검출
- merge_history_by_date: 날짜 기준 비파괴 병합 (로컬 구현, fin-commons 승격 후보)
"""

from fin_commons.guards import find_quality_violations, merge_preserved_history

__all__ = [
    "find_quality_violations",
    "merge_history_by_date",
    "merge_preserved_history",
]


def merge_history_by_date(new_history, old_history):
    """새 히스토리를 기존 히스토리에 날짜 기준으로 병합한다.

    같은 날짜는 새 레코드가 이기고, 새 데이터에 없는 날짜의 기존 레코드는 보존한다.
    데이터 소스가 축소된 히스토리를 반환해도 기존 구간이 유실되지 않는다.
    hodling-value의 merge_pair_history(pipeline/core.py)와 동일 규칙.
    """
    new_history = new_history or []
    old_history = old_history or []
    if not old_history:
        return list(new_history)
    if not new_history:
        return list(old_history)
    new_dates = {entry["date"] for entry in new_history}
    merged = [entry for entry in old_history if entry["date"] not in new_dates]
    merged.extend(new_history)
    merged.sort(key=lambda entry: entry["date"])
    return merged
