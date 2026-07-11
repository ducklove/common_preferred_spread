// node --test tests/js/ 로 실행. js/format.js는 의존성 없는 leaf ES 모듈이라 직접 import 한다.
// (루트 package.json의 "type": "module" 덕에 .js가 ESM으로 해석된다)
import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  toFiniteNumber,
  escapeHtml,
  formatIndexWeightPercent,
  formatIndexSpread,
  formatPairName,
  formatMarketPrice,
  formatCompactMetricValue,
  getDirectionClass,
  getTextColorClass,
  formatPercentChange,
  formatSignedPoints,
  parseDirectionalNumber,
  formatPointChange,
  formatAxisPrice,
  getHistoryDateMs,
  buildHistoryCsv,
  formatPrice,
  parseSnapshotTimestamp,
  formatKstTimestamp,
  getCurrentKstDateString,
  normalizeDateText,
  formatDateShort,
  isWeekendDateText,
  getCurrentKstDayMonth,
  isCurrentKstNightSession,
  getCurrentKstNightSessionDateString,
  getCurrentKstNightSessionDayMonth,
  formatMarketCap,
  formatTradedValue,
  formatRatioPercent,
  formatYield,
  formatStatPrice,
  fmtChange,
  getTickerCode,
  normalizeTickerCode,
} from '../../js/format.js';

test('toFiniteNumber: 숫자화 가능한 값만 통과, null/빈문자열/NaN/Infinity는 null', () => {
  assert.equal(toFiniteNumber(null), null);
  assert.equal(toFiniteNumber(undefined), null);
  assert.equal(toFiniteNumber(''), null);
  assert.equal(toFiniteNumber('abc'), null);
  assert.equal(toFiniteNumber(NaN), null);
  assert.equal(toFiniteNumber(Infinity), null);
  assert.equal(toFiniteNumber(-Infinity), null);
  assert.equal(toFiniteNumber(0), 0);
  assert.equal(toFiniteNumber('0'), 0);
  assert.equal(toFiniteNumber('12.5'), 12.5);
  assert.equal(toFiniteNumber(' 3 '), 3);
  assert.equal(toFiniteNumber(-7.25), -7.25);
});

test('escapeHtml: & < > " \' 다섯 문자 모두 이스케이프, null/undefined는 빈 문자열', () => {
  assert.equal(
    escapeHtml('<a href="x">&\'</a>'),
    '&lt;a href=&quot;x&quot;&gt;&amp;&#39;&lt;/a&gt;',
  );
  assert.equal(escapeHtml(null), '');
  assert.equal(escapeHtml(undefined), '');
  assert.equal(escapeHtml(123), '123');
  assert.equal(escapeHtml('평문 한글'), '평문 한글');
});

test('formatIndexWeightPercent: 10 이상은 소수 1자리, 미만은 2자리, 비수치는 -', () => {
  assert.equal(formatIndexWeightPercent(12.34), '12.3%');
  assert.equal(formatIndexWeightPercent(10), '10.0%');
  assert.equal(formatIndexWeightPercent(9.876), '9.88%');
  assert.equal(formatIndexWeightPercent(0), '0.00%');
  assert.equal(formatIndexWeightPercent(-5), '-5.00%');
  assert.equal(formatIndexWeightPercent(null), '-');
  assert.equal(formatIndexWeightPercent(NaN), '-');
});

test('formatIndexSpread / formatRatioPercent / formatYield: 소수 2자리 %, 비수치는 -', () => {
  assert.equal(formatIndexSpread(3.456), '3.46%');
  assert.equal(formatIndexSpread(0), '0.00%');
  assert.equal(formatIndexSpread(null), '-');
  assert.equal(formatRatioPercent(42.567), '42.57%');
  assert.equal(formatRatioPercent(-3.2), '-3.20%');
  assert.equal(formatRatioPercent(0), '0.00%');
  assert.equal(formatRatioPercent(null), '-');
  assert.equal(formatRatioPercent(NaN), '-');
  assert.equal(formatYield(7.5), '7.50%');
  assert.equal(formatYield(3.14159), '3.14%');
  assert.equal(formatYield(null), '-');
});

test('formatPairName: 첫 번째 /만 공백으로 치환', () => {
  assert.equal(formatPairName('삼성전자/우'), '삼성전자 우');
  assert.equal(formatPairName('a/b/c'), 'a b/c');
  assert.equal(formatPairName(123), '123');
});

test('formatMarketPrice: ko-KR 콤마 + 소수 2자리 고정, 비수치는 -', () => {
  assert.equal(formatMarketPrice(1234567.891), '1,234,567.89');
  assert.equal(formatMarketPrice(0), '0.00');
  assert.equal(formatMarketPrice(null), '-');
  assert.equal(formatMarketPrice(NaN), '-');
});

test('formatCompactMetricValue: priceDecimals 지정(0 포함) 존중, 기본 2자리, 결측은 -', () => {
  assert.equal(formatCompactMetricValue(null), '-');
  assert.equal(formatCompactMetricValue({}), '-');
  assert.equal(formatCompactMetricValue({ price: null }), '-');
  assert.equal(formatCompactMetricValue({ price: NaN }), '-');
  assert.equal(formatCompactMetricValue({ price: 2500.5 }), '2,500.50');
  assert.equal(formatCompactMetricValue({ price: 1234.5678, priceDecimals: 0 }), '1,235');
  assert.equal(formatCompactMetricValue({ price: 1450.4, priceDecimals: 2 }), '1,450.40');
});

test('getDirectionClass: |v| < 0.005는 flat, 부호에 따라 up/down, 비수치는 flat', () => {
  assert.equal(getDirectionClass(0), 'flat');
  assert.equal(getDirectionClass(0.004), 'flat');
  assert.equal(getDirectionClass(-0.004), 'flat');
  assert.equal(getDirectionClass(0.005), 'up');
  assert.equal(getDirectionClass(-0.005), 'down');
  assert.equal(getDirectionClass(3), 'up');
  assert.equal(getDirectionClass(-2), 'down');
  assert.equal(getDirectionClass(null), 'flat');
  assert.equal(getDirectionClass(NaN), 'flat');
});

test('getTextColorClass: 방향 + -color 접미사', () => {
  assert.equal(getTextColorClass(1), 'up-color');
  assert.equal(getTextColorClass(-1), 'down-color');
  assert.equal(getTextColorClass(0), 'flat-color');
});

test('formatPercentChange: ▲/▼ 화살표 + 절댓값 2자리, flat은 화살표 없음', () => {
  assert.equal(formatPercentChange(2.5), '▲ 2.50%');
  assert.equal(formatPercentChange(-1.234), '▼ 1.23%');
  assert.equal(formatPercentChange(0), '0.00%');
  assert.equal(formatPercentChange(0.004), '0.00%');
  assert.equal(formatPercentChange(null), '-');
  assert.equal(formatPercentChange(NaN), '-');
});

test('formatSignedPoints: 콤마 + 소수 2자리 + 화살표, suffix 부착', () => {
  assert.equal(formatSignedPoints(1234.5), '▲ 1,234.50');
  assert.equal(formatSignedPoints(-0.5), '▼ 0.50');
  assert.equal(formatSignedPoints(0), '0.00');
  assert.equal(formatSignedPoints(3, 'pt'), '▲ 3.00pt');
  assert.equal(formatSignedPoints(null), '-');
});

test('formatPointChange: %p 단위, flat은 화살표 없음', () => {
  assert.equal(formatPointChange(1.5), '▲ 1.50%p');
  assert.equal(formatPointChange(-0.75), '▼ 0.75%p');
  assert.equal(formatPointChange(0), '0.00%p');
  assert.equal(formatPointChange(null), '-');
});

test('parseDirectionalNumber: 화살표가 부호를 결정, 숫자는 그대로, 숫자 없으면 null', () => {
  assert.equal(parseDirectionalNumber(5), 5);
  assert.equal(parseDirectionalNumber(-3.2), -3.2);
  assert.equal(parseDirectionalNumber('▼ 1.23%'), -1.23);
  assert.equal(parseDirectionalNumber('▲ 0.5'), 0.5);
  assert.equal(parseDirectionalNumber('12.5%'), 12.5);
  assert.equal(parseDirectionalNumber('-7'), -7);
  assert.equal(parseDirectionalNumber('abc'), null);
});

test('formatAxisPrice: 만 단위 축약(10만 이상 정수, 1만~10만 소수1), 천 단위 콤마, 소액 소수', () => {
  assert.equal(formatAxisPrice(123456), '12만');
  assert.equal(formatAxisPrice(150000), '15만');
  assert.equal(formatAxisPrice(45678), '4.6만');
  assert.equal(formatAxisPrice(10000), '1.0만');
  assert.equal(formatAxisPrice(9999), '9,999');
  assert.equal(formatAxisPrice(2345.6), '2,346');
  assert.equal(formatAxisPrice(999.4), '999');
  assert.equal(formatAxisPrice(10), '10');
  assert.equal(formatAxisPrice(9.876), '9.88');
  assert.equal(formatAxisPrice(0), '0.00');
});

test('getHistoryDateMs: KST 자정 기준 epoch ms, 파싱 실패는 null', () => {
  assert.equal(getHistoryDateMs('2024-01-02'), Date.UTC(2024, 0, 1, 15, 0, 0));
  assert.equal(getHistoryDateMs('bogus'), null);
});

test('buildHistoryCsv: UTF-8 BOM + 헤더 + 행, includeKospi 옵션, 결측은 빈 셀', () => {
  const hist = [
    { date: '2024-05-01', commonPrice: 1000, preferredPrice: 800, spread: 20, kospiPrice: 2700 },
    { date: '2024-05-02', commonPrice: null, preferredPrice: 810, spread: null },
  ];
  const csv = buildHistoryCsv(hist);
  assert.equal(csv.charCodeAt(0), 0xfeff); // 엑셀 한글 호환 BOM
  assert.equal(
    csv.slice(1),
    'date,commonPrice,preferredPrice,spread\n2024-05-01,1000,800,20\n2024-05-02,,810,',
  );
  const csvKospi = buildHistoryCsv(hist, { includeKospi: true });
  assert.equal(
    csvKospi.slice(1),
    'date,commonPrice,preferredPrice,spread,kospiPrice\n2024-05-01,1000,800,20,2700\n2024-05-02,,810,,',
  );
  assert.equal(buildHistoryCsv([]).slice(1), 'date,commonPrice,preferredPrice,spread');
});

test('formatPrice: ko-KR 천 단위 콤마', () => {
  assert.equal(formatPrice(1234567), '1,234,567');
  assert.equal(formatPrice(0), '0');
});

test('parseSnapshotTimestamp: 공백 구분/타임존 없는 문자열은 KST 가정, 명시 타임존은 존중', () => {
  assert.equal(parseSnapshotTimestamp(null), null);
  assert.equal(parseSnapshotTimestamp(''), null);
  assert.equal(parseSnapshotTimestamp('garbage'), null);
  assert.equal(
    parseSnapshotTimestamp('2024-05-01 09:30:00').getTime(),
    Date.UTC(2024, 4, 1, 0, 30, 0),
  );
  assert.equal(
    parseSnapshotTimestamp('2024-05-01T09:30:00Z').getTime(),
    Date.UTC(2024, 4, 1, 9, 30, 0),
  );
  assert.equal(
    parseSnapshotTimestamp('2024-05-01T09:30:00-05:00').getTime(),
    Date.UTC(2024, 4, 1, 14, 30, 0),
  );
});

test('formatKstTimestamp: 어떤 입력이든 KST 벽시계로 YYYY-MM-DD HH:mm:ss, 실패는 빈 문자열', () => {
  assert.equal(formatKstTimestamp(new Date(Date.UTC(2024, 0, 1, 15, 0, 0))), '2024-01-02 00:00:00');
  assert.equal(formatKstTimestamp('2024-03-10T23:59:59Z'), '2024-03-11 08:59:59');
  assert.equal(formatKstTimestamp('2024-05-01 09:30:05'), '2024-05-01 09:30:05'); // KST 왕복 보존
  assert.equal(formatKstTimestamp('nonsense'), '');
  assert.equal(formatKstTimestamp(null), '');
  assert.equal(formatKstTimestamp(new Date(NaN)), ''); // Invalid Date 인스턴스도 크래시 없이 빈 문자열
});

test('getCurrentKstDateString / getCurrentKstDayMonth: 주입한 시각의 KST 날짜', () => {
  const utcEve = new Date(Date.UTC(2024, 11, 31, 16, 0, 0)); // KST 2025-01-01 01:00
  assert.equal(getCurrentKstDateString(utcEve), '2025-01-01');
  assert.equal(getCurrentKstDayMonth(new Date(Date.UTC(2024, 0, 1, 15, 0, 0))), '02/01');
});

test('normalizeDateText: ISO 접두/8자리 숫자만 허용, 그 외 null', () => {
  assert.equal(normalizeDateText('2024-05-01'), '2024-05-01');
  assert.equal(normalizeDateText('2024-05-01T09:00'), '2024-05-01');
  assert.equal(normalizeDateText('20240501'), '2024-05-01');
  assert.equal(normalizeDateText(' 2024-05-01 '), '2024-05-01');
  assert.equal(normalizeDateText('24-05-01'), null);
  assert.equal(normalizeDateText('202405'), null);
  assert.equal(normalizeDateText(''), null);
  assert.equal(normalizeDateText(null), null);
});

test('formatDateShort: 점 구분 표기, 실패는 -', () => {
  assert.equal(formatDateShort('2024-05-01'), '2024.05.01');
  assert.equal(formatDateShort('20240501'), '2024.05.01');
  assert.equal(formatDateShort('bad'), '-');
  assert.equal(formatDateShort(null), '-');
});

test('isWeekendDateText: 토/일만 true, 파싱 실패는 false', () => {
  assert.equal(isWeekendDateText('2024-05-04'), true); // 토
  assert.equal(isWeekendDateText('2024-05-05'), true); // 일
  assert.equal(isWeekendDateText('2024-05-07'), false); // 화
  assert.equal(isWeekendDateText('20240504'), true);
  assert.equal(isWeekendDateText('nope'), false);
});

test('isCurrentKstNightSession: KST 18시~익일 6시 미만', () => {
  assert.equal(isCurrentKstNightSession(new Date(Date.UTC(2024, 4, 1, 9, 0))), true); // KST 18:00
  assert.equal(isCurrentKstNightSession(new Date(Date.UTC(2024, 4, 1, 8, 59))), false); // KST 17:59
  assert.equal(isCurrentKstNightSession(new Date(Date.UTC(2024, 3, 30, 20, 59))), true); // KST 05:59
  assert.equal(isCurrentKstNightSession(new Date(Date.UTC(2024, 3, 30, 21, 0))), false); // KST 06:00
});

test('getCurrentKstNightSessionDateString: 새벽(6시 미만)은 전일 세션 날짜로 귀속', () => {
  assert.equal(getCurrentKstNightSessionDateString(new Date(Date.UTC(2024, 4, 1, 16, 0))), '2024-05-01'); // KST 5/2 01:00
  assert.equal(getCurrentKstNightSessionDateString(new Date(Date.UTC(2024, 4, 1, 10, 0))), '2024-05-01'); // KST 5/1 19:00
  assert.equal(getCurrentKstNightSessionDateString(new Date(Date.UTC(2024, 3, 30, 17, 0))), '2024-04-30'); // 월 경계
  assert.equal(getCurrentKstNightSessionDateString(new Date(NaN)), '');
});

test('getCurrentKstNightSessionDayMonth: DD/MM 표기', () => {
  assert.equal(getCurrentKstNightSessionDayMonth(new Date(Date.UTC(2024, 4, 1, 16, 0))), '01/05');
});

test('formatMarketCap: 1조 이상 조(2자리), 그 외 억(정수), 0 이하/비수치는 -', () => {
  assert.equal(formatMarketCap(2.5e12), '2.50조');
  assert.equal(formatMarketCap(1e12), '1.00조');
  assert.equal(formatMarketCap(9.99e11), '9990억');
  assert.equal(formatMarketCap(5e8), '5억');
  assert.equal(formatMarketCap(0), '-');
  assert.equal(formatMarketCap(-3), '-');
  assert.equal(formatMarketCap(null), '-');
  assert.equal(formatMarketCap(NaN), '-');
});

test('formatTradedValue: 조/억 축약, 100억 미만만 소수 1자리', () => {
  assert.equal(formatTradedValue(1.5e12), '1.50조');
  assert.equal(formatTradedValue(5e10), '500억');
  assert.equal(formatTradedValue(1e10), '100억');
  assert.equal(formatTradedValue(9.99e9), '99.9억');
  assert.equal(formatTradedValue(2.5e9), '25.0억');
  assert.equal(formatTradedValue(0), '-');
  assert.equal(formatTradedValue(null), '-');
});

test('formatStatPrice: 양수만 콤마 포맷, 0 이하/비수치는 -', () => {
  assert.equal(formatStatPrice(12345), '12,345');
  assert.equal(formatStatPrice(0), '-');
  assert.equal(formatStatPrice(-5), '-');
  assert.equal(formatStatPrice(null), '-');
  assert.equal(formatStatPrice(NaN), '-');
});

test('fmtChange: 방향 색상 클래스 + 소수 1자리 span, null/undefined는 빈 문자열', () => {
  assert.equal(fmtChange(2.5), '<span class="up-color change-value">▲2.5%</span>');
  assert.equal(fmtChange(-1), '<span class="down-color change-value">▼1.0%</span>');
  assert.equal(fmtChange(0), '<span class="flat-color change-value">&nbsp;0.0%</span>');
  assert.equal(fmtChange(null), '');
  assert.equal(fmtChange(undefined), '');
});

test('getTickerCode: 접미사 제거(첫 . 앞), falsy는 빈 문자열', () => {
  assert.equal(getTickerCode('005930.KS'), '005930');
  assert.equal(getTickerCode('005930'), '005930');
  assert.equal(getTickerCode(null), '');
  assert.equal(getTickerCode(''), '');
});

test('normalizeTickerCode: trim + 대문자화 + .KS 접미사 제거', () => {
  assert.equal(normalizeTickerCode(' 005930.ks '), '005930');
  assert.equal(normalizeTickerCode('005935.KS'), '005935');
  assert.equal(normalizeTickerCode('aapl'), 'AAPL');
  assert.equal(normalizeTickerCode(null), '');
});
