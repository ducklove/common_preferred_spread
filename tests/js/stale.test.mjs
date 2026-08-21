// node --test tests/js/ 로 실행. stale.js(거래정지/데이터 정체 판정 + 배지 렌더)를 검증한다.
// format.js에만 의존하는 leaf 모듈이라 document 스텁 없이 그대로 import 한다.
import { test } from 'node:test';
import assert from 'node:assert/strict';

const {
  STALE_BADGE_WARN_DAYS,
  formatStaleBadgeLabel,
  formatStaleBadgeTitle,
  getDateGapDays,
  getGroupStaleInfo,
  getLatestPairHistoryDate,
  getPairLastHistoryDate,
  getPairStaleInfo,
  renderStalePairBadge,
} = await import('../../js/stale.js');

function makePair(id, lastHistoryDate, extra = {}) {
  return { id, name: id, current: { spread: 30 }, history: [], lastHistoryDate, ...extra };
}

test('getPairLastHistoryDate: lastHistoryDate 우선, 없으면 history 마지막 레코드', () => {
  assert.equal(getPairLastHistoryDate(makePair('a', '2026-07-29')), '2026-07-29');
  // 실시간 시세가 history에 upsert돼도 파이프라인 기준일(lastHistoryDate)이 이긴다.
  const live = makePair('a', '2026-07-29', {
    history: [{ date: '2026-08-20' }, { date: '2026-08-21' }],
  });
  assert.equal(getPairLastHistoryDate(live), '2026-07-29');
  // 레거시 data.js 경로: 필드가 없으면 history 폴백
  const legacy = { id: 'b', history: [{ date: '2026-08-19' }, { date: '2026-08-20' }] };
  assert.equal(getPairLastHistoryDate(legacy), '2026-08-20');
  assert.equal(getPairLastHistoryDate({ id: 'c', history: [] }), null);
  assert.equal(getPairLastHistoryDate(null), null);
  // 20260729 같은 비하이픈 표기도 정규화한다
  assert.equal(getPairLastHistoryDate(makePair('d', '20260729')), '2026-07-29');
});

test('getLatestPairHistoryDate: 개별 종목 최대 날짜, 평균(지수) pair는 제외', () => {
  const pairs = [
    { id: '_average', isAverage: true, lastHistoryDate: '2026-08-31' },
    makePair('hanwha', '2026-07-29'),
    makePair('samsung', '2026-08-20'),
    makePair('daekyo', '2026-08-13'),
  ];
  assert.equal(getLatestPairHistoryDate(pairs), '2026-08-20');
  assert.equal(getLatestPairHistoryDate([]), null);
  assert.equal(getLatestPairHistoryDate([{ id: 'x' }]), null);
});

test('getDateGapDays: KST 자정 기준 일수 차, 파싱 실패는 null', () => {
  assert.equal(getDateGapDays('2026-08-20', '2026-07-29'), 22);
  assert.equal(getDateGapDays('2026-08-20', '2026-08-20'), 0);
  assert.equal(getDateGapDays('2026-08-20', '2026-08-21'), -1);
  assert.equal(getDateGapDays('2026-08-20', null), null);
  assert.equal(getDateGapDays('없음', '2026-08-20'), null);
});

test('getPairStaleInfo: 기준일 초과만 정체로 보고, 경계값은 정상 취급', () => {
  const latest = '2026-08-20';
  assert.equal(STALE_BADGE_WARN_DAYS, 5);
  // 22일 뒤처진 거래정지 종목
  assert.deepEqual(getPairStaleInfo(makePair('hanwha', '2026-07-29'), latest), {
    lastDate: '2026-07-29',
    gapDays: 22,
  });
  // 7일 뒤처진 거래정지 종목도 잡는다 (파이프라인 경고 기준 14일보다 촘촘)
  assert.deepEqual(getPairStaleInfo(makePair('daekyo', '2026-08-13'), latest), {
    lastDate: '2026-08-13',
    gapDays: 7,
  });
  // 정상 종목 / 경계(5일 = 기준 이하)는 배지 없음
  assert.equal(getPairStaleInfo(makePair('samsung', '2026-08-20'), latest), null);
  assert.equal(getPairStaleInfo(makePair('holiday', '2026-08-15'), latest), null);
  assert.deepEqual(getPairStaleInfo(makePair('holiday', '2026-08-14'), latest)?.gapDays, 6);
  // 평균(지수) pair, 기준일/날짜 결측은 판정하지 않는다
  assert.equal(getPairStaleInfo({ id: '_average', isAverage: true, lastHistoryDate: '2026-01-01' }, latest), null);
  assert.equal(getPairStaleInfo(makePair('x', null), latest), null);
  assert.equal(getPairStaleInfo(makePair('x', '2026-07-29'), null), null);
  assert.equal(getPairStaleInfo(null, latest), null);
  // warnDays 인자로 파이프라인 기준(14일)을 그대로 쓸 수도 있다
  assert.equal(getPairStaleInfo(makePair('daekyo', '2026-08-13'), latest, 14), null);
});

test('getGroupStaleInfo: 그룹 내 가장 오래 정체된 종목 기준', () => {
  const latest = '2026-08-20';
  const items = [
    { pair: makePair('alpha', '2026-08-20') },
    { pair: makePair('alpha2', '2026-08-13') },
    { pair: makePair('alpha3', '2026-07-29') },
  ];
  assert.deepEqual(getGroupStaleInfo(items, latest), { lastDate: '2026-07-29', gapDays: 22 });
  // 전원 최신이면 배지 없음
  assert.equal(getGroupStaleInfo([{ pair: makePair('a', latest) }], latest), null);
  assert.equal(getGroupStaleInfo([], latest), null);
  // pair를 감싸지 않고 직접 넘겨도 동작한다
  assert.deepEqual(getGroupStaleInfo([makePair('a', '2026-07-29')], latest)?.gapDays, 22);
});

test('formatStaleBadgeLabel/Title: 기준일 라벨과 설명 문구', () => {
  const info = { lastDate: '2026-07-29', gapDays: 22 };
  assert.equal(formatStaleBadgeLabel(info), '2026.07.29 기준');
  assert.equal(formatStaleBadgeLabel(null), '');
  assert.match(formatStaleBadgeTitle(info), /거래정지/);
  assert.match(formatStaleBadgeTitle(info), /2026-07-29/);
  assert.match(formatStaleBadgeTitle(info), /22일/);
  assert.equal(formatStaleBadgeTitle(null), '');
});

test('renderStalePairBadge: 정체 정보가 없으면 빈 문자열, 있으면 배지 마크업', () => {
  assert.equal(renderStalePairBadge(null), '');
  const html = renderStalePairBadge({ lastDate: '2026-07-29', gapDays: 22 });
  assert.match(html, /class="stale-badge"/);
  assert.match(html, /2026\.07\.29 기준/);
  assert.match(html, /title="[^"]*2026-07-29[^"]*"/);
  assert.match(html, /aria-label="[^"]*거래정지[^"]*"/);
});
