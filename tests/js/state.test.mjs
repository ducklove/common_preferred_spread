// node --test tests/js/ 로 실행. state.js의 순수 헬퍼(히스토리 upsert, 메타 정규화,
// 카드 정렬 비교자 등)만 검증한다. 모듈 최상위 document 접근용 최소 스텁 주입 후 동적 import.
import { test } from 'node:test';
import assert from 'node:assert/strict';

globalThis.document = { documentElement: { dataset: { theme: 'light' } } };

const {
  buildHistoryRecords,
  upsertHistoryPoint,
  resolvePreferredTerm,
  normalizePairMeta,
  getPrevDaySpread,
  getCardSortMetric,
  compareCardItems,
  stampPairLastHistoryDates,
} = await import('../../js/state.js');

test('buildHistoryRecords: 배열 길이는 최소 공통 길이, kospi는 값이 있을 때만 부착', () => {
  const records = buildHistoryRecords({
    dates: ['2024-05-01', '2024-05-02', '2024-05-03'],
    common: [100, 110],
    preferred: [80, 88],
    spread: [20, 20],
    kospi: [2700, null],
  });
  assert.equal(records.length, 2); // min(3, 2, 2, 2)
  assert.deepEqual(records[0], {
    date: '2024-05-01', commonPrice: 100, preferredPrice: 80, spread: 20, kospiPrice: 2700,
  });
  assert.equal('kospiPrice' in records[1], false); // null이면 키 자체가 없음

  assert.deepEqual(buildHistoryRecords(null), []);
  assert.deepEqual(buildHistoryRecords({ dates: 'x', common: [1], preferred: [1], spread: [1] }), []);
});

test('upsertHistoryPoint: 같은 날짜는 병합 갱신, 최신 날짜는 append, 과거 날짜는 거부', () => {
  const pair = { history: [{ date: '2024-05-01', spread: 10, commonPrice: 100 }] };

  assert.equal(upsertHistoryPoint(pair, '2024-05-01', { spread: 12 }), true);
  assert.equal(pair.history.length, 1);
  assert.equal(pair.history[0].spread, 12);
  assert.equal(pair.history[0].commonPrice, 100); // 미전달 필드는 유지

  assert.equal(upsertHistoryPoint(pair, '2024-05-02', { spread: 13 }), true);
  assert.deepEqual(pair.history[1], { date: '2024-05-02', spread: 13 });

  assert.equal(upsertHistoryPoint(pair, '2024-04-30', { spread: 9 }), false); // 마지막보다 과거 신규 날짜
  assert.equal(pair.history.length, 2);

  assert.equal(upsertHistoryPoint(pair, '20240503', { spread: 14 }), true); // 8자리도 정규화
  assert.equal(pair.history[2].date, '2024-05-03');

  assert.equal(upsertHistoryPoint(pair, 'nope', { spread: 1 }), false);
  assert.equal(upsertHistoryPoint({ history: null }, '2024-05-01', {}), false);

  const fresh = { history: [] };
  assert.equal(upsertHistoryPoint(fresh, '2024-05-01', { spread: 1 }), true); // 빈 히스토리에 첫 점
  assert.equal(fresh.history.length, 1);
});

test('resolvePreferredTerm: profile 필드를 바탕으로 raw가 덮어쓴다', () => {
  assert.equal(resolvePreferredTerm(null), null);
  assert.equal(resolvePreferredTerm('문자열'), null);
  assert.deepEqual(resolvePreferredTerm({ rate: 2 }), { rate: 2, profile: null });
  assert.deepEqual(
    resolvePreferredTerm({ profile: 'p1', rate: 9 }, { p1: { rate: 1, floor: true } }),
    { rate: 9, floor: true, profile: 'p1' },
  );
  assert.deepEqual(resolvePreferredTerm({ profile: 'missing' }, {}), { profile: 'missing' });
});

test('normalizePairMeta: 상장일 정규화, conversion ratio는 숫자화 실패 시 1', () => {
  assert.equal(normalizePairMeta(null), null);
  assert.equal(normalizePairMeta('문자열'), null);

  const meta = normalizePairMeta({ listing: { common: '20200102' } });
  assert.equal(meta.listing.common, '2020-01-02');
  assert.equal(meta.listing.preferred, null);
  assert.equal(meta.conversion, null);

  const converted = normalizePairMeta({
    conversion: { scheduledDate: '2026-12-31 00:00', ratio: '0.5' },
  });
  assert.equal(converted.conversion.scheduledDate, '2026-12-31');
  assert.equal(converted.conversion.ratio, 0.5);
  assert.equal(converted.listing.common, null); // listing 자체가 없어도 기본 형태 보장

  assert.equal(normalizePairMeta({ conversion: { ratio: 'abc' } }).conversion.ratio, 1);
  assert.equal(normalizePairMeta({ conversion: {} }).conversion.ratio, 1);
});

test('getPrevDaySpread: today보다 과거인 가장 최근 spread', () => {
  const pair = {
    history: [
      { date: '2024-05-01', spread: 1 },
      { date: '2024-05-02', spread: 2 },
      { date: '2024-05-03', spread: 3 },
    ],
  };
  assert.equal(getPrevDaySpread(pair, '2024-05-03'), 2);
  assert.equal(getPrevDaySpread(pair, '2024-06-01'), 3);
  assert.equal(getPrevDaySpread(pair, '2024-05-01'), null); // 과거 데이터 없음
  assert.equal(getPrevDaySpread({}, '2024-05-03'), null); // history 없음
});

test('getCardSortMetric: 정렬 모드별 지표 매핑, current 없으면 null', () => {
  const pair = {
    current: { spread: 1, spreadChange: 2, preferredDivYield: 3 },
    attractiveness: { total: 4 },
  };
  assert.equal(getCardSortMetric(pair, 'spread'), 1);
  assert.equal(getCardSortMetric(pair, 'spreadWidening'), 2);
  assert.equal(getCardSortMetric(pair, 'spreadNarrowing'), 2);
  assert.equal(getCardSortMetric(pair, 'preferredYield'), 3);
  assert.equal(getCardSortMetric(pair, 'attractiveness'), 4);
  assert.equal(getCardSortMetric({ current: {} }, 'attractiveness'), null); // attractiveness 미산출
  assert.equal(getCardSortMetric({}, 'spread'), null);
  assert.equal(getCardSortMetric(null, 'spread'), null);
});

test('compareCardItems: 기본 내림차순, 결측은 항상 뒤로, 동률은 spread → 이름(ko) 순', () => {
  const item = (name, current) => ({ pair: { name, current } });

  // spread 내림차순 → 큰 값이 앞
  assert.ok(compareCardItems(item('A', { spread: 20 }), item('B', { spread: 10 }), 'spread') < 0);
  assert.ok(compareCardItems(item('A', { spread: 10 }), item('B', { spread: 20 }), 'spread') > 0);

  // 결측은 뒤로
  assert.equal(compareCardItems(item('A', { spread: null }), item('B', { spread: 5 }), 'spread'), 1);
  assert.equal(compareCardItems(item('A', { spread: 5 }), item('B', { spread: NaN }), 'spread'), -1);

  // 둘 다 결측이면 한글 이름 오름차순
  assert.ok(compareCardItems(item('가나', {}), item('다라', {}), 'spread') < 0);

  // 지표 동률이면 spread가 큰 쪽이 앞
  assert.ok(
    compareCardItems(
      item('A', { preferredDivYield: 5, spread: 1 }),
      item('B', { preferredDivYield: 5, spread: 9 }),
      'preferredYield',
    ) > 0,
  );

  // spreadNarrowing은 오름차순 → 축소폭 큰(음수) 쪽이 앞
  assert.ok(
    compareCardItems(
      item('A', { spreadChange: -2, spread: 0 }),
      item('B', { spreadChange: 1, spread: 0 }),
      'spreadNarrowing',
    ) < 0,
  );
});

test('stampPairLastHistoryDates: historyMeta.end 우선, 없으면 history 마지막, 기존 값은 보존', () => {
  const pairs = [
    { id: 'summaryPair', history: [] },                                  // history 없음 → meta로 보완
    { id: 'legacyPair', history: [{ date: '2026-08-19' }, { date: '2026-08-20' }] },
    { id: 'alreadyStamped', lastHistoryDate: '2026-07-29', history: [{ date: '2026-08-21' }] },
    { id: 'empty', history: [] },
    null,
  ];

  stampPairLastHistoryDates(pairs, { summaryPair: { end: '20260813' } });

  assert.equal(pairs[0].lastHistoryDate, '2026-08-13'); // 8자리도 정규화
  assert.equal(pairs[1].lastHistoryDate, '2026-08-20');
  assert.equal(pairs[2].lastHistoryDate, '2026-07-29'); // summary.json이 준 값을 덮어쓰지 않는다
  assert.equal('lastHistoryDate' in pairs[3], false);   // 근거가 없으면 심지 않는다
  assert.deepEqual(stampPairLastHistoryDates([]), []);
});
