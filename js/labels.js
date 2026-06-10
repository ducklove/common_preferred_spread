// js/labels.js — 종목 라벨/배지 HTML 헬퍼 (charts/views 공용)
import { PREFERRED_ADDITIONAL_DIVIDEND_OVERRIDES, getGroupItemsByPairId } from './state.js';
import { escapeHtml, toFiniteNumber } from './format.js';

export function getPreferredShortLabel(pair) {
  if (!pair?.name) return '우';
  if (!pair.name.includes('/')) return '우';
  const suffix = pair.name.split('/')[1];
  return suffix ? suffix.trim() : '우';
}

export function getDetailLabels(pair, items = null) {
  const groupItems = items || getGroupItemsByPairId(pair?.id) || (pair ? [{ pair }] : []);
  return {
    common: '보통주',
    preferred: groupItems.length > 1 ? getPreferredShortLabel(pair) : '우선주',
  };
}

export function formatBadgeAmount(value) {
  if (value == null || Number.isNaN(value)) return '';
  const abs = Math.abs(value);
  const formatted = Number.isInteger(abs)
    ? abs.toLocaleString('ko-KR')
    : abs.toLocaleString('ko-KR', { maximumFractionDigits: 2 });
  return `${value > 0 ? '+' : '-'}${formatted}`;
}

export function hasConvertibleOption(pair) {
  const name = `${pair?.preferredName || ''} ${pair?.name || ''}`;
  return /전환/.test(name);
}

export function stripConvertibleMarker(label) {
  return String(label || '').replace(/\s*\(전환\)\s*/g, '').trim();
}

export function getAdditionalDividendAmount(pair) {
  const explicitDividendDiff = PREFERRED_ADDITIONAL_DIVIDEND_OVERRIDES[pair?.id];
  if (explicitDividendDiff != null) return Number(explicitDividendDiff);
  const commonDividend = toFiniteNumber(pair?.current?.commonDividendPerShare);
  const preferredDividend = toFiniteNumber(pair?.current?.preferredDividendPerShare);
  if (commonDividend == null || preferredDividend == null) return null;
  const dividendDiff = preferredDividend - commonDividend;
  return dividendDiff > 0.0001 ? dividendDiff : null;
}

export function renderConvertibleBadge(pair) {
  if (!hasConvertibleOption(pair)) return '';
  return '<span class="preferred-badge convertible">전환</span>';
}

export function renderAdditionalDividendBadge(pair) {
  const dividendDiff = getAdditionalDividendAmount(pair);
  if (dividendDiff == null || Number.isNaN(dividendDiff)) return '';
  return `<span class="preferred-badge dividend">${escapeHtml(formatBadgeAmount(dividendDiff))}</span>`;
}

export function renderPreferredInlineLabel(pair, fallbackLabel = null) {
  const rawLabel = fallbackLabel || pair?.preferredName || pair?.name || '우선주';
  const label = stripConvertibleMarker(rawLabel) || rawLabel;
  return `<span class="preferred-name-with-badges">
    <span class="preferred-name-text">${escapeHtml(label)}</span>
    ${renderConvertibleBadge(pair)}
  </span>`;
}

export function renderPreferredYieldLabel(pair) {
  const dividendBadge = renderAdditionalDividendBadge(pair);
  return `우선주${dividendBadge ? ` ${dividendBadge}` : ''}`;
}
