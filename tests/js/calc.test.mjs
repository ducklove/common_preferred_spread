// node --test tests/js/ 로 실행. calc.js가 import하는 state.js는 모듈 최상위에서
// document를 읽으므로, jsdom 없이 최소 스텁만 주입한 뒤 동적 import 한다.
// (state.js 최상위의 fetch('config.json…')는 내부 try/catch로 흡수된다)
import { test } from 'node:test';
import assert from 'node:assert/strict';

globalThis.document = { documentElement: { dataset: { theme: 'dark' } } };

const {
  getIndexPreferredMarketCap,
  calculateSimpleAverageMetrics,
  calculateSqrtPreferredMarketCapSpreadIndex,
  calculatePairReturns,
  calculatePairBeta,
  calculatePairCorrelation,
  calculateEmaSeries,
  calculateLatestEma,
  calculateSma,
  calculateSmaSeries,
  calculateMeanStd,
  calculatePercentileRank,
  calculateLiveMarketCap,
  calculatePreferredRatio,
} = await import('../../js/calc.js');

const approx = (actual, expected, eps = 1e-9) => {
  assert.ok(
    actual != null && Math.abs(actual - expected) < eps,
    `기대 ${expected} ± ${eps}, 실제 ${actual}`,
  );
};

test('calculateLiveMarketCap: 가격×주식수 우선, 결측/0/음수는 fallback, 둘 다 없으면 null', () => {
  assert.equal(calculateLiveMarketCap(100, 1000), 100000);
  assert.equal(calculateLiveMarketCap(100, 1000, 999), 100000); // 계산값이 fallback보다 우선
  assert.equal(calculateLiveMarketCap(null, 1000, 5e8), 5e8);
  assert.equal(calculateLiveMarketCap(100, 0, 5e8), 5e8);
  assert.equal(calculateLiveMarketCap(NaN, 1000, 5e8), 5e8);
  assert.equal(calculateLiveMarketCap(0, 1000), null);
  assert.equal(calculateLiveMarketCap(-5, 10, -3), null); // 음수 fallback도 무효
  assert.equal(calculateLiveMarketCap(null, null, null), null);
  assert.equal(calculateLiveMarketCap(NaN, NaN, NaN), null);
});

test('calculatePreferredRatio: 우선주/보통주 ×100, 보통주 0 이하·우선주 음수는 null', () => {
  assert.equal(calculatePreferredRatio(1000, 250), 25);
  assert.equal(calculatePreferredRatio(4e8, 1e8), 25);
  assert.equal(calculatePreferredRatio(1000, 0), 0); // 우선주 0은 유효
  assert.equal(calculatePreferredRatio(0, 100), null);
  assert.equal(calculatePreferredRatio(-1, 100), null);
  assert.equal(calculatePreferredRatio(null, 100), null);
  assert.equal(calculatePreferredRatio(100, null), null);
  assert.equal(calculatePreferredRatio(100, -1), null);
  assert.equal(calculatePreferredRatio(NaN, 5), null);
});

test('getIndexPreferredMarketCap: 실시간 계산 우선, 양수 아니면 null', () => {
  assert.equal(getIndexPreferredMarketCap({ current: { preferredMarketCap: 2e8 } }), 2e8);
  assert.equal(
    getIndexPreferredMarketCap({
      current: { preferredPrice: 500, preferredSharesOutstanding: 1e6, preferredMarketCap: 7 },
    }),
    5e8,
  );
  assert.equal(getIndexPreferredMarketCap({}), null);
  assert.equal(getIndexPreferredMarketCap(null), null);
  assert.equal(getIndexPreferredMarketCap({ current: { preferredMarketCap: -1 } }), null);
});

test('calculatePairReturns: 연속 유효 구간만 수익률 계산, 유효 수익률 2개 미만은 null', () => {
  const hist = [
    { commonPrice: 100, preferredPrice: 50 },
    { commonPrice: null, preferredPrice: 55 }, // 무효 → 인접 두 구간 모두 제외
    { commonPrice: 110, preferredPrice: 60 },
    { commonPrice: 121, preferredPrice: 66 },
    { commonPrice: 133.1, preferredPrice: 72.6 },
  ];
  const returns = calculatePairReturns(hist);
  assert.equal(returns.commonReturns.length, 2);
  assert.equal(returns.preferredReturns.length, 2);
  approx(returns.commonReturns[0], 0.1);
  approx(returns.preferredReturns[0], 0.1);

  assert.equal(calculatePairReturns(null), null);
  assert.equal(calculatePairReturns([{ commonPrice: 100, preferredPrice: 50 }]), null);
  // 유효 수익률 1개뿐 → null
  assert.equal(
    calculatePairReturns([
      { commonPrice: 100, preferredPrice: 50 },
      { commonPrice: 110, preferredPrice: 55 },
    ]),
    null,
  );
  // 0 이하 가격은 무효 처리
  assert.equal(
    calculatePairReturns([
      { commonPrice: 100, preferredPrice: 50 },
      { commonPrice: -1, preferredPrice: 55 },
      { commonPrice: 110, preferredPrice: 60 },
    ]),
    null,
  );
});

test('calculatePairBeta: 우선주 수익률 = 보통주 ×2면 베타 2, 보통주 분산 0이면 null', () => {
  const hist = [
    { commonPrice: 100, preferredPrice: 100 },
    { commonPrice: 110, preferredPrice: 120 }, // +10% / +20%
    { commonPrice: 99, preferredPrice: 96 }, // -10% / -20%
  ];
  approx(calculatePairBeta(hist), 2);

  const flatCommon = [
    { commonPrice: 100, preferredPrice: 100 },
    { commonPrice: 100, preferredPrice: 120 },
    { commonPrice: 100, preferredPrice: 96 },
  ];
  assert.equal(calculatePairBeta(flatCommon), null);
  assert.equal(calculatePairBeta([]), null);
});

test('calculatePairCorrelation: 완전 동행 +1, 완전 역행 -1, 분산 0이면 null', () => {
  const together = [
    { commonPrice: 100, preferredPrice: 100 },
    { commonPrice: 110, preferredPrice: 120 },
    { commonPrice: 99, preferredPrice: 96 },
  ];
  approx(calculatePairCorrelation(together), 1);

  const opposite = [
    { commonPrice: 100, preferredPrice: 100 },
    { commonPrice: 110, preferredPrice: 90 },
    { commonPrice: 99, preferredPrice: 99 },
  ];
  approx(calculatePairCorrelation(opposite), -1);

  const flatPreferred = [
    { commonPrice: 100, preferredPrice: 100 },
    { commonPrice: 110, preferredPrice: 100 },
    { commonPrice: 99, preferredPrice: 100 },
  ];
  assert.equal(calculatePairCorrelation(flatPreferred), null);
});

test('calculateEmaSeries: 첫 유효값으로 시작, null 구간은 직전 EMA 유지', () => {
  assert.deepEqual(calculateEmaSeries([], 0.5), []);
  assert.deepEqual(calculateEmaSeries(null, 0.5), []);
  assert.deepEqual(calculateEmaSeries([10], 0.5), [10]);
  assert.deepEqual(calculateEmaSeries([10, 20], 0.5), [10, 15]);
  assert.deepEqual(calculateEmaSeries([10, null, 20], 0.5), [10, 10, 15]);
  assert.deepEqual(calculateEmaSeries([null, 10], 0.5), [null, 10]);
  approx(calculateEmaSeries([10, 20])[1], 11); // 기본 alpha = 0.1
});

test('calculateLatestEma: 마지막 유효 EMA 반환, 전부 무효면 null', () => {
  assert.equal(calculateLatestEma([10, 20], 0.5), 15);
  assert.equal(calculateLatestEma([10, 20, null], 0.5), 15); // 꼬리 null 무시
  assert.equal(calculateLatestEma([null, NaN], 0.5), null);
  assert.equal(calculateLatestEma([], 0.5), null);
});

test('calculateSma: 뒤에서 windowSize개 중 유효값 평균', () => {
  assert.equal(calculateSma([1, 2, 3, 4], 2), 3.5);
  assert.equal(calculateSma([1, 2, 3, 4], 10), 2.5); // 윈도가 더 커도 전체 평균
  assert.equal(calculateSma([1, null, 3], 3), 2);
  assert.equal(calculateSma([null, NaN], 2), null);
  assert.equal(calculateSma([], 3), null);
  assert.equal(calculateSma('nope', 3), null);
});

test('calculateSmaSeries: 롤링 평균, null은 표본에서 제외하되 윈도 자리는 차지', () => {
  assert.deepEqual(calculateSmaSeries([1, 2, 3, 4], 2), [1, 1.5, 2.5, 3.5]);
  assert.deepEqual(calculateSmaSeries([1, null, 3], 2), [1, 1, 3]);
  assert.deepEqual(calculateSmaSeries([null, null], 2), [null, null]);
  assert.deepEqual(calculateSmaSeries([], 2), []);
});

test('calculateMeanStd: 모집단 표준편차, 유효 표본 2개 미만은 null', () => {
  assert.deepEqual(calculateMeanStd([2, 4, 4, 4, 5, 5, 7, 9]), { mean: 5, std: 2 });
  assert.deepEqual(calculateMeanStd([2, 4, 4, 4, 5, 5, 7, 9, null, NaN]), { mean: 5, std: 2 });
  assert.deepEqual(calculateMeanStd([1, 1]), { mean: 1, std: 0 });
  assert.equal(calculateMeanStd([1]), null);
  assert.equal(calculateMeanStd(null), null);
});

test('calculatePercentileRank: 현재값 이하 비율(%), 동률 포함', () => {
  const values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  assert.equal(calculatePercentileRank(values, 5), 50);
  assert.equal(calculatePercentileRank(values, 10), 100);
  assert.equal(calculatePercentileRank(values, 0.5), 0);
  assert.equal(calculatePercentileRank([1, 2, 2, 3], 2), 75);
  assert.equal(calculatePercentileRank(values, null), null);
  assert.equal(calculatePercentileRank(values, NaN), null);
  assert.equal(calculatePercentileRank([null, NaN], 1), null);
  assert.equal(calculatePercentileRank('nope', 1), null);
});

test('calculateSimpleAverageMetrics: isAverage/current 없는 pair 제외, 키별 유효값만 평균', () => {
  const pairs = [
    { isAverage: true, current: { spread: 999 } },
    { current: { spread: 10, spreadChange: 1, commonChange: 2, preferredChange: 3 } },
    { current: { spread: 20, spreadChange: null, commonChange: 4, preferredChange: NaN } },
    { current: null },
    null,
  ];
  const metrics = calculateSimpleAverageMetrics(pairs);
  assert.equal(metrics.spread, 15);
  assert.equal(metrics.spreadChange, 1); // 유효값 1개뿐
  assert.equal(metrics.commonChange, 3);
  assert.equal(metrics.preferredChange, 3); // NaN 제외
  assert.equal(metrics.count, 2);

  const empty = calculateSimpleAverageMetrics([]);
  assert.equal(empty.spread, null);
  assert.equal(empty.count, 0);
});

test('sqrt 지수: 발행사별 √우선주총액 가중 괴리율', () => {
  const pairs = [
    { id: 'a', name: 'A우', commonName: 'A', current: { spread: 10, preferredMarketCap: 4e8, spreadChange: 1 } },
    { id: 'b', name: 'B우', commonName: 'B', current: { spread: 40, preferredMarketCap: 1e8 } },
  ];
  const index = calculateSqrtPreferredMarketCapSpreadIndex(pairs);
  // 가중치 √4e8=20000, √1e8=10000 → (10·20000 + 40·10000) / 30000 = 20
  assert.equal(index.spread, 20);
  assert.equal(index.issuerCount, 2);
  assert.equal(index.spreadChange, 1); // A만 보유 → A 가중치로만 평균
  assert.equal(index.commonChange, null);
  assert.equal(index.methodLabel, '제곱근 총액가중');
  assert.equal(index.constituents.length, 2);
  assert.equal(index.constituents[0].name, 'A'); // 비중 내림차순
  approx(index.constituents[0].weight, 20000 / 30000 * 100);
});

test('sqrt 지수: 같은 발행사(commonName)의 복수 우선주는 시총가중으로 합산', () => {
  const pairs = [
    { id: 'a1', name: 'A우', commonName: 'A', current: { spread: 10, preferredMarketCap: 1e8 } },
    { id: 'a2', name: 'A2우B', commonName: 'A', current: { spread: 20, preferredMarketCap: 3e8 } },
  ];
  const index = calculateSqrtPreferredMarketCapSpreadIndex(pairs);
  assert.equal(index.issuerCount, 1);
  assert.equal(index.spread, 17.5); // (10·1e8 + 20·3e8) / 4e8
  const parts = index.constituents[0].parts;
  assert.equal(parts[0].id, 'a2'); // 시총 내림차순
  assert.equal(parts[0].issuerShare, 75);
  assert.equal(parts[1].issuerShare, 25);
  assert.equal(index.constituents[0].totalMarketCap, 4e8);
});

test('sqrt 지수: 시총은 가격×주식수 실시간 계산 우선, 유효 구성이 없으면 null', () => {
  const live = calculateSqrtPreferredMarketCapSpreadIndex([
    {
      id: 'a', name: 'A우', commonName: 'A',
      current: { spread: 10, preferredPrice: 1000, preferredSharesOutstanding: 400000, preferredMarketCap: 7 },
    },
  ]);
  assert.equal(live.constituents[0].totalMarketCap, 4e8);

  assert.equal(calculateSqrtPreferredMarketCapSpreadIndex([]), null);
  assert.equal(calculateSqrtPreferredMarketCapSpreadIndex([{ id: 'x', current: { spread: 5 } }]), null); // 시총 결측
  assert.equal(
    calculateSqrtPreferredMarketCapSpreadIndex([
      { id: 'avg', isAverage: true, current: { spread: 5, preferredMarketCap: 1e8 } },
    ]),
    null,
  ); // 평균 pair 제외
});
