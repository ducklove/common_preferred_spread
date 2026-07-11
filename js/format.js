// js/format.js — 포맷터·날짜/시간 유틸·escapeHtml·CSV 빌더 (의존성 없는 leaf 모듈)

export function toFiniteNumber(value) {
  if (value == null || value === '') return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

export function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, char => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  }[char]));
}

export function formatIndexWeightPercent(value) {
  if (value == null || Number.isNaN(value)) return '-';
  const decimals = value >= 10 ? 1 : 2;
  return `${value.toFixed(decimals)}%`;
}

export function formatIndexSpread(value) {
  if (value == null || Number.isNaN(value)) return '-';
  return `${value.toFixed(2)}%`;
}

export function formatPairName(name) {
  return String(name).replace('/', ' ');
}

export function formatMarketPrice(value) {
  if (value == null || Number.isNaN(value)) return '-';
  return value.toLocaleString('ko-KR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export function formatCompactMetricValue(metric) {
  if (!metric || metric.price == null || Number.isNaN(metric.price)) return '-';
  const decimals = metric.priceDecimals == null ? 2 : metric.priceDecimals;
  return metric.price.toLocaleString('ko-KR', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

export function getDirectionClass(value) {
  if (value == null || Number.isNaN(value) || Math.abs(value) < 0.005) return 'flat';
  return value > 0 ? 'up' : 'down';
}

export function getTextColorClass(value) {
  return getDirectionClass(value) + '-color';
}

export function formatPercentChange(value) {
  if (value == null || Number.isNaN(value)) return '-';
  const direction = getDirectionClass(value);
  if (direction === 'flat') return `${Math.abs(value).toFixed(2)}%`;
  const arrow = direction === 'up' ? '▲' : '▼';
  return `${arrow} ${Math.abs(value).toFixed(2)}%`;
}

export function formatSignedPoints(value, suffix = '') {
  if (value == null || Number.isNaN(value)) return '-';
  const direction = getDirectionClass(value);
  const formatted = Math.abs(value).toLocaleString('ko-KR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (direction === 'flat') return `${formatted}${suffix}`;
  const arrow = direction === 'up' ? '▲' : '▼';
  return `${arrow} ${formatted}${suffix}`;
}

export function parseDirectionalNumber(value) {
  if (typeof value === 'number') return value;
  const text = String(value);
  const match = text.match(/-?\d+(?:\.\d+)?/);
  if (!match) return null;
  const parsed = parseFloat(match[0]);
  if (text.includes('▼')) return -Math.abs(parsed);
  if (text.includes('▲')) return Math.abs(parsed);
  return parsed;
}

export function formatPointChange(value) {
  if (value == null || Number.isNaN(value)) return '-';
  const direction = getDirectionClass(value);
  if (direction === 'flat') return `${Math.abs(value).toFixed(2)}%p`;
  const arrow = direction === 'up' ? '▲' : '▼';
  return `${arrow} ${Math.abs(value).toFixed(2)}%p`;
}

export function formatAxisPrice(n) {
  if (n >= 10000) return (n / 10000).toFixed(n >= 100000 ? 0 : 1) + '만';
  if (n >= 1000) return Math.round(n).toLocaleString('ko-KR');
  return n.toFixed(n < 10 ? 2 : 0);
}

export function getHistoryDateMs(dateText) {
  const parsed = Date.parse(`${dateText}T00:00:00+09:00`);
  return Number.isNaN(parsed) ? null : parsed;
}

// --- CSV 내보내기 ---
export function buildHistoryCsv(hist, { includeKospi = false } = {}) {
  const header = ['date', 'commonPrice', 'preferredPrice', 'spread', ...(includeKospi ? ['kospiPrice'] : [])];
  const lines = hist.map(entry => {
    const cells = [entry.date ?? '', entry.commonPrice ?? '', entry.preferredPrice ?? '', entry.spread ?? ''];
    if (includeKospi) cells.push(entry.kospiPrice ?? '');
    return cells.join(',');
  });
  return '\ufeff' + [header.join(','), ...lines].join('\n'); // UTF-8 BOM (엑셀 한글 호환)
}

export function formatPrice(n) {
  return n.toLocaleString('ko-KR');
}

export function parseSnapshotTimestamp(timestamp) {
  if (!timestamp) return null;
  const normalized = String(timestamp).includes('T')
    ? String(timestamp)
    : String(timestamp).replace(' ', 'T');
  const withTimezone = /([+-]\d{2}:\d{2}|Z)$/.test(normalized)
    ? normalized
    : normalized + '+09:00';
  const parsed = new Date(withTimezone);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

export function formatKstTimestamp(value) {
  const parsed = value instanceof Date ? value : parseSnapshotTimestamp(value);
  if (!parsed || Number.isNaN(parsed.getTime())) return ''; // Invalid Date 포함 파싱 실패는 빈 문자열

  const parts = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Seoul',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).formatToParts(parsed);
  const partMap = Object.fromEntries(parts.map(part => [part.type, part.value]));
  return `${partMap.year}-${partMap.month}-${partMap.day} ${partMap.hour}:${partMap.minute}:${partMap.second}`;
}

export function getCurrentKstDateString(now = new Date()) {
  return formatKstTimestamp(now).slice(0, 10);
}

export function normalizeDateText(value) {
  if (!value) return null;
  const text = String(value).trim();
  let match = text.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (match) return `${match[1]}-${match[2]}-${match[3]}`;
  match = text.match(/^(\d{4})(\d{2})(\d{2})/);
  if (match) return `${match[1]}-${match[2]}-${match[3]}`;
  return null;
}

export function formatDateShort(value) {
  const normalized = normalizeDateText(value);
  return normalized ? normalized.replaceAll('-', '.') : '-';
}

export function isWeekendDateText(dateText) {
  const normalized = normalizeDateText(dateText);
  if (!normalized) return false;
  const [year, month, day] = normalized.split('-').map(Number);
  const weekday = new Date(Date.UTC(year, month - 1, day)).getUTCDay();
  return weekday === 0 || weekday === 6;
}

export function getCurrentKstDayMonth(now = new Date()) {
  const timestamp = formatKstTimestamp(now);
  return `${timestamp.slice(8, 10)}/${timestamp.slice(5, 7)}`;
}

export function isCurrentKstNightSession(now = new Date()) {
  const hour = Number(formatKstTimestamp(now).slice(11, 13));
  return hour >= 18 || hour < 6;
}

export function getCurrentKstNightSessionDateString(now = new Date()) {
  const base = parseSnapshotTimestamp(formatKstTimestamp(now));
  if (!base) return '';
  if (Number(formatKstTimestamp(base).slice(11, 13)) < 6) {
    base.setDate(base.getDate() - 1);
  }
  return formatKstTimestamp(base).slice(0, 10);
}

export function getCurrentKstNightSessionDayMonth(now = new Date()) {
  const dateText = getCurrentKstNightSessionDateString(now);
  return `${dateText.slice(8, 10)}/${dateText.slice(5, 7)}`;
}

export function formatMarketCap(value) {
  if (value == null || Number.isNaN(value) || value <= 0) return '-';
  if (value >= 1e12) return `${(value / 1e12).toFixed(2)}조`;
  return `${(value / 1e8).toFixed(0)}억`;
}

export function formatTradedValue(value) {
  if (value == null || Number.isNaN(value) || value <= 0) return '-';
  if (value >= 1e12) return `${(value / 1e12).toFixed(2)}조`;
  return `${(value / 1e8).toFixed(value >= 1e10 ? 0 : 1)}억`;
}

export function formatRatioPercent(value) {
  if (value == null || Number.isNaN(value)) return '-';
  return `${value.toFixed(2)}%`;
}

export function formatYield(value) {
  if (value == null || Number.isNaN(value)) return '-';
  return `${value.toFixed(2)}%`;
}

export function formatStatPrice(value) {
  if (value == null || Number.isNaN(value) || value <= 0) return '-';
  return formatPrice(value);
}

export function fmtChange(v) {
  if (v == null) return '';
  const direction = getDirectionClass(v);
  const cls = getTextColorClass(v);
  if (direction === 'flat') {
    return `<span class="${cls} change-value">&nbsp;${Math.abs(v).toFixed(1)}%</span>`;
  }
  const arrow = direction === 'up' ? '▲' : '▼';
  return `<span class="${cls} change-value">${arrow}${Math.abs(v).toFixed(1)}%</span>`;
}

export function getTickerCode(ticker) {
  return String(ticker || '').split('.')[0];
}

export function normalizeTickerCode(code) {
  return String(code || '').trim().toUpperCase().replace(/\.KS$/i, '');
}
