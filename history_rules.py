#!/usr/bin/env python3
"""
히스토리 보존/품질 가드 규칙 모듈 — fin-commons 재수출 파사드

구현은 전부 fin-commons의 guards 모듈로 승격되었다
(https://github.com/ducklove/fin-commons — 함수명·시그니처·위반 메시지 동일).
기존 임포트 경로(`import history_rules`)를 보존하기 위한 재수출이며,
tests/test_history_rules.py가 동작 동일성을 계속 검증한다.

- merge_preserved_history: 기존 데이터의 더 이른 과거 구간을 새 히스토리 앞에 보존
- find_quality_violations: 이전 데이터 대비 히스토리 유실/시작일 후퇴/포인트 급감 검출
- merge_history_by_date: 날짜 기준 비파괴 병합 (v0.2.0에서 승격)
"""

from fin_commons.guards import (
    find_quality_violations,
    merge_history_by_date,
    merge_preserved_history,
)

__all__ = [
    "find_quality_violations",
    "merge_history_by_date",
    "merge_preserved_history",
]
