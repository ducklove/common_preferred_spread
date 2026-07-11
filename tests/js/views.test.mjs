// node --test tests/js/ 로 실행. views.js의 DOM 비의존 로직(테이블 정렬 비교자,
// 배당차/전환수익률 계산, 지수 비중 라벨, 스파크라인 데이터 가공, 모달 포커스 순환)을 검증한다.
// state.test.mjs와 동일하게 모듈 최상위 document 접근용 최소 스텁 주입 후 동적 import.
import { test } from 'node:test';
import assert from 'node:assert/strict';

globalThis.document = { documentElement: { dataset: { theme: 'light' } }, activeElement: null };

const { app } = await import('../../js/state.js');
const {
  compareTableMetric,
  compareTableRows,
  getDivYieldGap,
  formatDivYieldGap,
  formatSpreadRecoveryYears,
  calculateAnnualizedConversionReturn,
  formatConversionReturn,
  getIndexWeightPartLabel,
  getAverageSpreadTrend,
  renderAverageSpreadSparkline,
  getModalFocusableElements,
  trapIndexWeightModalTab,
  getPairTableName,
  getFutureSessionLabel,
  renderSessionBadge,
  renderFutureTimeText,
  renderOverviewStat,
} = await import('../../js/views.js');

test('compareTableMetric: 결측은 항상 뒤로, 방향에 따라 부호 반전', () => {
  assert.equal(compareTableMetric(null, null, 'asc'), 0);
  assert.equal(compareTableMetric(null, 5, 'asc'), 1); // a 결측 → 뒤로
  assert.equal(compareTableMetric(5, NaN, 'desc'), -1); // b 결측 → 앞으로
  assert.equal(compareTableMetric(3, 3, 'asc'), 0);
  assert.ok(compareTableMetric(1, 2, 'asc') < 0);
  assert.ok(compareTableMetric(1, 2, 'desc') > 0);
});

test('compareTableRows: name 키는 한글 로케일 정렬, direction 전환 시 반전', () => {
  const a = { name: '가나' };
  const b = { name: '다라' };
  app.tableSortState = { key: 'name', direction: 'asc' };
  assert.ok(compareTableRows(a, b) < 0);
  app.tableSortState = { key: 'name', direction: 'desc' };
  assert.ok(compareTableRows(a, b) > 0);
});

test('compareTableRows: 지표 정렬 + 동률이면 이름(ko) 오름차순 폴백', () => {
  app.tableSortState = { key: 'spread', direction: 'desc' };
  assert.ok(compareTableRows({ name: 'A', spread: 20 }, { name: 'B', spread: 10 }) < 0);
  assert.ok(compareTableRows({ name: 'A', spread: 10 }, { name: 'B', spread: 20 }) > 0);
  // 동률 → 이름 폴백 (direction과 무관하게 오름차순)
  assert.ok(compareTableRows({ name: '다라', spread: 10 }, { name: '가나', spread: 10 }) > 0);
  // 결측 지표는 방향과 무관하게 뒤로
  app.tableSortState = { key: 'spread', direction: 'asc' };
  assert.equal(compareTableRows({ name: 'A', spread: null }, { name: 'B', spread: 1 }), 1);
});

test('getDivYieldGap: 우선주-보통주 배당수익률 차, 비유한 값은 null', () => {
  assert.equal(getDivYieldGap({ preferredDivYield: 3.5, commonDivYield: 1.5 }), 2);
  assert.equal(getDivYieldGap({ preferredDivYield: 3.5 }), null);
  assert.equal(getDivYieldGap({ preferredDivYield: NaN, commonDivYield: 1 }), null);
  assert.equal(getDivYieldGap(null), null);
});

test('formatDivYieldGap: 부호 포함 %p 포맷, 결측은 대시', () => {
  assert.equal(formatDivYieldGap(null), '-');
  assert.equal(formatDivYieldGap(NaN), '-');
  assert.equal(formatDivYieldGap(1.234), '+1.23%p');
  assert.equal(formatDivYieldGap(-0.5), '-0.50%p');
  assert.equal(formatDivYieldGap(0), '0.00%p');
});

test('formatSpreadRecoveryYears: 배당차 0.05 이하·결측·음수는 대시, 99년 초과는 99+년', () => {
  assert.equal(formatSpreadRecoveryYears(null, 2), '-');
  assert.equal(formatSpreadRecoveryYears(30, null), '-');
  assert.equal(formatSpreadRecoveryYears(30, 0.05), '-'); // 임계값 이하
  assert.equal(formatSpreadRecoveryYears(30, 2), '15.0년');
  assert.equal(formatSpreadRecoveryYears(500, 1), '99+년');
  assert.equal(formatSpreadRecoveryYears(-10, 2), '-'); // 음수 연수
});

test('calculateAnnualizedConversionReturn: 1년 후 전환·10% 괴리는 연 10%', () => {
  const now = new Date('2026-01-01T09:00:00+09:00');
  const pair = { current: { commonPrice: 110, preferredPrice: 100 } };
  const value = calculateAnnualizedConversionReturn(pair, { scheduledDate: '2027-01-01', ratio: 1 }, now);
  assert.ok(Math.abs(value - 10) < 1e-9);

  // 전환비율 반영: 0.5주 전환이면 총수익률 0.55 → 연환산 음수
  const halved = calculateAnnualizedConversionReturn(pair, { scheduledDate: '2027-01-01', ratio: 0.5 }, now);
  assert.ok(halved < 0);
});

test('calculateAnnualizedConversionReturn: 과거 예정일·결측 가격·비양수 비율은 null', () => {
  const now = new Date('2026-01-01T09:00:00+09:00');
  const pair = { current: { commonPrice: 110, preferredPrice: 100 } };
  assert.equal(calculateAnnualizedConversionReturn(pair, { scheduledDate: '2025-12-31' }, now), null);
  assert.equal(calculateAnnualizedConversionReturn(pair, {}, now), null); // 예정일 없음
  assert.equal(calculateAnnualizedConversionReturn({ current: { commonPrice: 110 } }, { scheduledDate: '2027-01-01' }, now), null);
  assert.equal(calculateAnnualizedConversionReturn({ current: { commonPrice: 110, preferredPrice: 0 } }, { scheduledDate: '2027-01-01' }, now), null);
  assert.equal(calculateAnnualizedConversionReturn(pair, { scheduledDate: '2027-01-01', ratio: 0 }, now), null);
});

test('formatConversionReturn: 부호 포함 % 포맷, 결측은 대시', () => {
  assert.equal(formatConversionReturn(null), '-');
  assert.equal(formatConversionReturn(NaN), '-');
  assert.equal(formatConversionReturn(5), '+5.00%');
  assert.equal(formatConversionReturn(-3.2), '-3.20%');
});

test('getIndexWeightPartLabel: 단일 구성은 이름만, 복수 구성은 발행사 내 비중 병기 + escape', () => {
  assert.equal(getIndexWeightPartLabel([]), '');
  assert.equal(getIndexWeightPartLabel(null), '');
  assert.equal(getIndexWeightPartLabel([{ preferredName: '삼성우', issuerShare: 60 }]), '삼성우');
  assert.equal(
    getIndexWeightPartLabel([
      { preferredName: 'A우', issuerShare: 60 },
      { name: 'B우', issuerShare: 40 },
    ]),
    'A우 60.0% · B우 40.0%',
  );
  assert.equal(getIndexWeightPartLabel([{ name: '<b>우' }]), '&lt;b&gt;우');
});

test('getAverageSpreadTrend: 평균쌍 히스토리에서 유효 spread만, 현재값으로 마지막 점 대체', () => {
  app.pairs = [{
    isAverage: true,
    history: [
      { date: '2026-06-01', spread: 30 },
      { date: '2026-06-02', spread: null }, // 결측 제외
      { date: '2026-06-03', spread: '31.5' }, // 문자열 숫자화
      { date: '2026-06-04', spread: 32 },
    ],
  }];
  const trend = getAverageSpreadTrend();
  assert.deepEqual(trend.map(point => point.spread), [30, 31.5, 32]);

  const overridden = getAverageSpreadTrend(33.3);
  assert.equal(overridden[overridden.length - 1].spread, 33.3);
  assert.equal(overridden[0].spread, 30); // 앞쪽 점은 그대로
  // 원본 히스토리는 변경되지 않는다
  assert.equal(app.pairs[0].history[3].spread, 32);

  app.pairs = [{ isAverage: true, history: [] }];
  assert.deepEqual(getAverageSpreadTrend(), []);
  app.pairs = [];
  assert.deepEqual(getAverageSpreadTrend(), []);
});

test('renderAverageSpreadSparkline: 점 2개 미만은 빈 상태, 이상이면 방향 클래스 svg', () => {
  app.pairs = [{ isAverage: true, history: [{ date: '2026-06-01', spread: 30 }] }];
  assert.match(renderAverageSpreadSparkline(30), /추이 없음/);

  app.pairs = [{
    isAverage: true,
    history: [
      { date: '2026-06-01', spread: 30 },
      { date: '2026-06-02', spread: 32 },
    ],
  }];
  const rising = renderAverageSpreadSparkline(32);
  assert.match(rising, /<svg/);
  assert.match(rising, /average-sparkline up/); // 상승 추세
  const falling = renderAverageSpreadSparkline(28);
  assert.match(falling, /average-sparkline down/);
  app.pairs = [];
});

function makeFocusable(name) {
  return {
    name,
    focusCount: 0,
    focus() {
      this.focusCount += 1;
      globalThis.document.activeElement = this;
    },
  };
}

function makeModal(elements) {
  return {
    lastSelector: null,
    querySelectorAll(selector) {
      this.lastSelector = selector;
      return elements;
    },
    contains(el) {
      return elements.includes(el);
    },
  };
}

function makeTabEvent(shiftKey = false) {
  return {
    shiftKey,
    prevented: false,
    preventDefault() {
      this.prevented = true;
    },
  };
}

test('getModalFocusableElements: disabled 제외 셀렉터로 조회해 배열 반환', () => {
  const first = makeFocusable('first');
  const modal = makeModal([first]);
  const result = getModalFocusableElements(modal);
  assert.deepEqual(result, [first]);
  assert.match(modal.lastSelector, /button:not\(\[disabled\]\)/);
  assert.match(modal.lastSelector, /\[tabindex\]:not\(\[tabindex="-1"\]\)/);
});

test('trapIndexWeightModalTab: 마지막에서 Tab이면 첫 요소로 순환', () => {
  const [first, mid, last] = ['first', 'mid', 'last'].map(makeFocusable);
  const modal = makeModal([first, mid, last]);
  globalThis.document.activeElement = last;
  const event = makeTabEvent(false);
  trapIndexWeightModalTab(event, modal);
  assert.equal(event.prevented, true);
  assert.equal(first.focusCount, 1);
});

test('trapIndexWeightModalTab: 첫 요소에서 Shift+Tab이면 마지막으로 순환', () => {
  const [first, mid, last] = ['first', 'mid', 'last'].map(makeFocusable);
  const modal = makeModal([first, mid, last]);
  globalThis.document.activeElement = first;
  const event = makeTabEvent(true);
  trapIndexWeightModalTab(event, modal);
  assert.equal(event.prevented, true);
  assert.equal(last.focusCount, 1);
});

test('trapIndexWeightModalTab: 중간 요소에서는 기본 Tab 이동 유지', () => {
  const [first, mid, last] = ['first', 'mid', 'last'].map(makeFocusable);
  const modal = makeModal([first, mid, last]);
  globalThis.document.activeElement = mid;
  const forward = makeTabEvent(false);
  trapIndexWeightModalTab(forward, modal);
  assert.equal(forward.prevented, false);
  const backward = makeTabEvent(true);
  trapIndexWeightModalTab(backward, modal);
  assert.equal(backward.prevented, false);
  assert.equal(first.focusCount + mid.focusCount + last.focusCount, 0);
});

test('trapIndexWeightModalTab: 포커스가 모달 밖이면 방향에 맞는 끝으로 회수, 포커스 대상 없으면 이동 차단만', () => {
  const [first, last] = ['first', 'last'].map(makeFocusable);
  const modal = makeModal([first, last]);
  const outside = makeFocusable('outside');

  globalThis.document.activeElement = outside;
  const forward = makeTabEvent(false);
  trapIndexWeightModalTab(forward, modal);
  assert.equal(forward.prevented, true);
  assert.equal(first.focusCount, 1);

  globalThis.document.activeElement = outside;
  const backward = makeTabEvent(true);
  trapIndexWeightModalTab(backward, modal);
  assert.equal(backward.prevented, true);
  assert.equal(last.focusCount, 1);

  const emptyModal = makeModal([]);
  const blocked = makeTabEvent(false);
  trapIndexWeightModalTab(blocked, emptyModal);
  assert.equal(blocked.prevented, true);
});

test('getPairTableName: preferredName → name → commonName 순 폴백', () => {
  assert.equal(getPairTableName({ preferredName: '삼성우', name: '삼성', commonName: '삼성보' }), '삼성우');
  assert.equal(getPairTableName({ name: '삼성', commonName: '삼성보' }), '삼성');
  assert.equal(getPairTableName({ commonName: '삼성보' }), '삼성보');
  assert.equal(getPairTableName(null), '');
});

test('getFutureSessionLabel: source 기반 야간/주간 라벨, 판별 불가 시 null 아님', () => {
  assert.equal(getFutureSessionLabel({ source: 'esignal_socket' }), '야간');
  assert.equal(getFutureSessionLabel({ source: 'hankyung_html' }), '주간');
  assert.equal(getFutureSessionLabel({ marketStatus: '야간' }), '야간');
  assert.equal(getFutureSessionLabel({ marketStatus: '장중' }), '주간');
  assert.equal(getFutureSessionLabel(null), null); // metric 없음
});

test('renderSessionBadge / renderFutureTimeText: 배지 클래스와 시간 표시 규칙', () => {
  assert.match(renderSessionBadge('야간'), /session-badge night/);
  assert.match(renderSessionBadge('주간'), /session-badge day/);
  assert.equal(renderFutureTimeText(null), '');
  assert.equal(renderFutureTimeText({ time: '  ' }), '');
  assert.equal(renderFutureTimeText({ time: '장마감', marketStatus: '장마감' }), ''); // 상태 문구와 중복이면 생략
  assert.match(renderFutureTimeText({ time: '17:30' }), /<span class="session-time">17:30<\/span>/);
});

test('renderOverviewStat: 방향성 값은 색상 클래스, directional=false는 중립', () => {
  assert.match(renderOverviewStat('라벨', '▲2.00%'), /up-color/);
  assert.match(renderOverviewStat('라벨', '▼1.00%'), /down-color/);
  assert.match(renderOverviewStat('라벨', '1.23%', { directional: false }), /neutral-color/);
});
