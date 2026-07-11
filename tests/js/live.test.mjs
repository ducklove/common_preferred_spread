// node --test tests/js/ 로 실행. live.js의 DOM 비의존 로직(스냅샷 날짜 결정,
// 시세 파싱/부호 적용, 프록시 URL 조립, 동시성 매퍼, 시장 메트릭 병합 규칙)을 검증한다.
// state.test.mjs와 동일하게 모듈 최상위 document 접근용 최소 스텁 주입 후 동적 import.
import { test } from 'node:test';
import assert from 'node:assert/strict';

globalThis.document = { documentElement: { dataset: { theme: 'light' } }, activeElement: null };

const {
  getSnapshotPriceDate,
  extractQuoteTradeDate,
  parseRawNumber,
  pickDefined,
  applySignedDirection,
  getProxyTimestamp,
  buildInternalStockQuote,
  buildLiveMarketMetric,
  chunkArray,
  mapWithConcurrency,
  withCacheBustingParam,
  buildInternalProxyUrl,
  isSnapshotMsStale,
  buildLiveMarketSummary,
  extractNightFutureMetricFromHtml,
  buildEsignalNightFutureMetric,
} = await import('../../js/live.js');

test('getSnapshotPriceDate: 명시 날짜 우선, 보통주/우선주 일치 시 채택, 불일치는 폴백', () => {
  assert.equal(getSnapshotPriceDate({ date: '2026-07-10', commonTradeDate: '2026-07-09' }, '2026-07-08'), '2026-07-10');
  assert.equal(getSnapshotPriceDate({ commonTradeDate: '2026-07-10', preferredTradeDate: '2026-07-10' }, null), '2026-07-10');
  assert.equal(getSnapshotPriceDate({ commonTradeDate: '2026-07-10' }, null), '2026-07-10'); // 한쪽만 있으면 그 날짜
  assert.equal(getSnapshotPriceDate({ preferredTradeDate: '2026-07-09' }, null), '2026-07-09');
  // 서로 다르면 폴백 사용 (평일)
  assert.equal(
    getSnapshotPriceDate({ commonTradeDate: '2026-07-10', preferredTradeDate: '2026-07-09' }, '2026-07-08'),
    '2026-07-08',
  );
  // 폴백이 주말이면 null
  assert.equal(getSnapshotPriceDate({}, '2026-07-11'), null); // 2026-07-11은 토요일
  assert.equal(getSnapshotPriceDate({}, null), null);
});

test('extractQuoteTradeDate: date → tradeDate → priceDate → localTradedAt 순 우선', () => {
  assert.equal(extractQuoteTradeDate(null), null);
  assert.equal(extractQuoteTradeDate({ date: '2026-07-10', tradeDate: '2026-07-09' }), '2026-07-10');
  assert.equal(extractQuoteTradeDate({ tradeDate: '20260709' }), '2026-07-09'); // 8자리 정규화
  assert.equal(extractQuoteTradeDate({ localTradedAt: '2026-07-08 15:30:00' }), '2026-07-08');
  assert.equal(extractQuoteTradeDate({ overMarketPriceInfo: { localTradedAt: '2026-07-07 18:00' } }), '2026-07-07');
  assert.equal(extractQuoteTradeDate({ date: 'nope' }), null);
});

test('parseRawNumber: 콤마 제거 숫자화, 결측/비숫자는 null', () => {
  assert.equal(parseRawNumber('1,234.5'), 1234.5);
  assert.equal(parseRawNumber('-0.5'), -0.5);
  assert.equal(parseRawNumber(0), 0);
  assert.equal(parseRawNumber(null), null);
  assert.equal(parseRawNumber(''), null);
  assert.equal(parseRawNumber('abc'), null);
});

test('pickDefined: null/undefined만 건너뛰고 0·빈문자열은 유효값', () => {
  assert.equal(pickDefined(null, undefined, 0, 5), 0);
  assert.equal(pickDefined(undefined, '', 'x'), '');
  assert.equal(pickDefined(null, undefined), null);
  assert.equal(pickDefined(), null);
});

test('applySignedDirection: 부호 코드 우선, 없으면 기준 등락률 부호로 보정', () => {
  assert.equal(applySignedDirection('100', '4'), -100); // 하락 코드
  assert.equal(applySignedDirection('-100', '2'), 100); // 상승 코드가 절댓값 강제
  assert.equal(applySignedDirection('50', 'FALLING'), -50);
  assert.equal(applySignedDirection('50', 'UPPER_LIMIT'), 50);
  assert.equal(applySignedDirection('30', null, '-1.5'), -30); // 기준 pct 음수
  assert.equal(applySignedDirection('30', null, '1.5'), 30);
  assert.equal(applySignedDirection('30', null, '0'), 0); // 보합
  assert.equal(applySignedDirection('30', null, null), 30); // 정보 없으면 원값
  assert.equal(applySignedDirection('abc', '4'), null);
});

test('getProxyTimestamp: 체결시각 우선, 없으면 polled_at(KST) 폴백', () => {
  assert.equal(
    getProxyTimestamp({ raw: { overMarketPriceInfo: { localTradedAt: '2026-07-10 15:30:00' } } }),
    '2026-07-10 15:30:00',
  );
  assert.equal(
    getProxyTimestamp({
      raw: { nxtOverMarketPriceInfo: { localTradedAt: 'NXT' }, overMarketPriceInfo: { localTradedAt: 'KRX' } },
    }),
    'NXT', // nxt 우선
  );
  const fromPolledAt = getProxyTimestamp({ meta: { polled_at: Date.UTC(2026, 6, 10, 0, 0, 0) } });
  assert.equal(fromPolledAt, '2026-07-10 09:00:00'); // UTC 00시 = KST 09시
  assert.equal(getProxyTimestamp({}), null);
});

test('buildInternalStockQuote: summary/raw 병합 시세, 가격 없으면 null', () => {
  const quote = buildInternalStockQuote({
    summary: { current_price: '1,000', change: '10', change_rate: '1.0', name: '삼성전자' },
    raw: { prdy_vrss_sign: '5', overMarketPriceInfo: { localTradedAt: '2026-07-10 15:30:00' } },
  }, '005930');
  assert.equal(quote.itemCode, '005930');
  assert.equal(quote.stockName, '삼성전자');
  assert.equal(quote.closePrice, 1000);
  assert.equal(quote.compareToPreviousClosePrice, -10); // 하락 부호 코드 적용
  assert.equal(quote.fluctuationsRatio, -1);
  assert.equal(quote.tradeDate, '2026-07-10');

  assert.equal(buildInternalStockQuote(null, '005930'), null);
  assert.equal(buildInternalStockQuote({ summary: { name: '가격없음' } }, '005930'), null);
});

test('buildLiveMarketMetric: defaults 병합과 부호 적용, 유효값 전무 시 null', () => {
  assert.equal(buildLiveMarketMetric(null), null);
  assert.equal(buildLiveMarketMetric({ summary: {}, raw: {} }, { id: 'KOSPI' }), null);

  const metric = buildLiveMarketMetric({
    closePriceRaw: '3,100.5',
    compareToPreviousClosePriceRaw: '10.5',
    fluctuationsRatioRaw: '0.34',
    raw: { rf: '5' },
  }, { id: 'KOSPI', name: '코스피', priceDecimals: 2 });
  assert.equal(metric.id, 'KOSPI');
  assert.equal(metric.name, '코스피');
  assert.equal(metric.price, 3100.5);
  assert.equal(metric.change, -10.5); // 하락 코드
  assert.equal(metric.changePct, -0.34);
  assert.equal(metric.priceDecimals, 2);
});

test('chunkArray: size 단위 분할, 마지막 조각은 나머지', () => {
  assert.deepEqual(chunkArray([1, 2, 3, 4, 5], 2), [[1, 2], [3, 4], [5]]);
  assert.deepEqual(chunkArray([1], 3), [[1]]);
  assert.deepEqual(chunkArray([], 3), []);
});

test('mapWithConcurrency: 결과 순서 보존 + 동시 실행 수는 limit 이하', async () => {
  let active = 0;
  let maxActive = 0;
  const mapper = async value => {
    active += 1;
    maxActive = Math.max(maxActive, active);
    await null; // 마이크로태스크 양보로 워커 인터리빙 유도
    active -= 1;
    return value * 10;
  };

  const results = await mapWithConcurrency([1, 2, 3, 4, 5], 2, mapper);
  assert.deepEqual(results, [10, 20, 30, 40, 50]);
  assert.ok(maxActive <= 2, `동시 실행 ${maxActive} > 2`);

  maxActive = 0;
  await mapWithConcurrency([1, 2, 3], 10, mapper); // limit이 커도 워커는 항목 수만큼만
  assert.ok(maxActive <= 3);
});

test('withCacheBustingParam / buildInternalProxyUrl: 쿼리 조립 규칙', () => {
  assert.match(withCacheBustingParam('a.json'), /^a\.json\?_ts=\d+$/);
  assert.match(withCacheBustingParam('a.json?x=1'), /^a\.json\?x=1&_ts=\d+$/);

  const url = new globalThis.URL(buildInternalProxyUrl('/v1/stocks/005930/quote', {
    market: 'UN',
    empty: '',
    missing: null,
    zero: 0,
  }));
  assert.equal(url.pathname, '/v1/stocks/005930/quote');
  assert.equal(url.searchParams.get('market'), 'UN');
  assert.equal(url.searchParams.get('zero'), '0'); // 0은 유효값
  assert.equal(url.searchParams.has('empty'), false); // 빈문자열/누락은 제외
  assert.equal(url.searchParams.has('missing'), false);
});

test('isSnapshotMsStale: 타임스탬프 없으면 항상 stale, 임계 초과만 stale', () => {
  assert.equal(isSnapshotMsStale(0), true);
  assert.equal(isSnapshotMsStale(null), true);
  assert.equal(isSnapshotMsStale(Date.now(), 60_000), false);
  assert.equal(isSnapshotMsStale(Date.now() - 61_000, 60_000), true);
});

test('buildLiveMarketSummary: 시세 전무 시 null, extras/야간선물은 있을 때만 부착', () => {
  assert.equal(buildLiveMarketSummary(null, []), null);

  const extrasOnly = buildLiveMarketSummary(null, [{ id: 'KOSDAQ' }]);
  assert.equal(extrasOnly.id, 'KOSPI'); // 본체 시세 없어도 KOSPI 골격 유지
  assert.equal(extrasOnly.price, null);
  assert.deepEqual(extrasOnly.extras, [{ id: 'KOSDAQ' }]);
  assert.equal('nightFuture' in extrasOnly, false);

  const night = { id: 'KOSPI200_FUTURES' };
  const withNight = buildLiveMarketSummary({ closePriceRaw: '3,000' }, [], night);
  assert.equal(withNight.price, 3000);
  assert.equal(withNight.nightFuture, night);
});

test('extractNightFutureMetricFromHtml: data-test 속성 파싱, 필수값 결측 시 null', () => {
  const html = `
    <span data-test="instrument-price-last">431.55</span>
    <span data-test="instrument-price-change">+2.15</span>
    <span data-test="instrument-price-change-percent">(0.50%)</span>
    <span data-test="trading-time-label">17:30:12</span>
  `;
  const metric = extractNightFutureMetricFromHtml(html);
  assert.equal(metric.id, 'KOSPI200_FUTURES');
  assert.equal(metric.price, 431.55);
  assert.equal(metric.change, 2.15);
  assert.equal(metric.changePct, 0.5);
  assert.equal(metric.time, '17:30:12');
  assert.equal(metric.source, 'investing_html');

  assert.equal(extractNightFutureMetricFromHtml(''), null);
  assert.equal(extractNightFutureMetricFromHtml('<span data-test="instrument-price-last">431.55</span>'), null);
});

test('buildEsignalNightFutureMetric: 현재 세션 payload만 메트릭 생성, 기준가 0은 null', () => {
  const nowMs = Date.now();
  const metric = buildEsignalNightFutureMetric({
    value: '430.00',
    value_diff: '4.30',
    value_day: '430.00',
    tstamp: nowMs,
    ttime: 173012,
    symbol: '101W9000',
  });
  assert.equal(metric.price, 430);
  assert.equal(metric.change, 4.3);
  assert.equal(metric.changePct, 0); // (price - basePrice) / basePrice
  assert.equal(metric.time, '17:30:12'); // ttime 6자리 → HH:MM:SS
  assert.equal(metric.marketStatus, '야간');
  assert.equal(metric.code, '101W9000');

  assert.equal(buildEsignalNightFutureMetric(null), null);
  assert.equal(buildEsignalNightFutureMetric({ value: '430', value_diff: '1', value_day: '0' }), null);
  assert.equal(buildEsignalNightFutureMetric({ value_diff: '1', value_day: '430' }), null);
});
