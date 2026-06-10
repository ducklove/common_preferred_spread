#!/usr/bin/env python3
"""
히스토리 보존/품질 가드 규칙 모듈

fetch_data.py의 인라인 로직을 순수 함수로 분리한 것으로,
표준 라이브러리만 사용한다 (pandas/yfinance 불필요).

- merge_preserved_history: 기존 데이터의 더 이른 과거 구간을 새 히스토리 앞에 보존
- find_quality_violations: 이전 데이터 대비 히스토리 유실/시작일 후퇴/포인트 급감 검출
"""


def merge_preserved_history(history, previous_history):
    """기존 데이터의 더 이른 구간을 새 히스토리 앞에 보존한다.

    history: 새로 만든 히스토리 리스트 ([{"date": "YYYY-MM-DD", ...}, ...])
    previous_history: 이전 데이터의 히스토리 리스트 (None 허용)

    (prepended_entries, merged_history) 반환.
    - 이전 데이터에 history[0]["date"]보다 이른 항목이 있으면 그 구간을 앞에 붙인다.
    - 보존할 구간이 없으면 ([], history)를 그대로 반환한다 (같은 날짜 중복 없음).
    """
    history = history if history is not None else []
    previous_history = previous_history or []
    if not history or not previous_history:
        return [], history

    first_new_date = history[0]["date"]
    prepended = [h for h in previous_history if h["date"] < first_new_date]
    if not prepended:
        return [], history
    return prepended, prepended + history


def find_quality_violations(
    result_histories,
    previous_histories,
    configured_pair_ids,
    min_drop=20,
    drop_ratio=0.02,
):
    """이전 데이터 대비 히스토리 품질 위반 메시지 목록을 반환한다.

    result_histories: {pair_id: history list} (새 결과, isAverage 제외)
    previous_histories: {pair_id: history list} (이전 데이터, isAverage 제외)
    configured_pair_ids: config에 존재하는 pair id 집합 (제거된 pair는 검사 제외)
    min_drop / drop_ratio: 허용 포인트 감소 = max(min_drop, int(이전 길이 * drop_ratio))

    검출 항목 (메시지는 fetch_data.py 기존 한국어 포맷 그대로):
    - pair 소실 또는 빈 히스토리
    - 시작일 후퇴
    - 포인트 수 < 이전 - 허용 감소
    """
    violations = []
    for pair_id, prev_hist in previous_histories.items():
        if pair_id not in configured_pair_ids or not prev_hist:
            continue
        new_hist = result_histories.get(pair_id) or []
        if not new_hist:
            violations.append(f"{pair_id}: 결과에서 사라졌거나 히스토리가 비어 있음")
            continue
        if new_hist[0]["date"] > prev_hist[0]["date"]:
            violations.append(
                f"{pair_id}: 시작일 후퇴 {prev_hist[0]['date']} -> {new_hist[0]['date']}"
            )
        allowed_drop = max(min_drop, int(len(prev_hist) * drop_ratio))
        if len(new_hist) < len(prev_hist) - allowed_drop:
            violations.append(
                f"{pair_id}: 히스토리 포인트 감소 {len(prev_hist)} -> {len(new_hist)}"
            )
    return violations
