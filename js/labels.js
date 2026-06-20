// js/labels.js — 종목 라벨/배지 HTML 헬퍼 (charts/views 공용)
import {
  app,
  getGroupItemsByPairId,
  getPreferredTerm,
} from './state.js';
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

export function hasConvertibleOption(pair) {
  const name = `${pair?.preferredName || ''} ${pair?.name || ''}`;
  return /전환/.test(name);
}

export function stripConvertibleMarker(label) {
  return String(label || '').replace(/\s*\(전환\)\s*/g, '').trim();
}

export function renderConvertibleBadge(pair) {
  if (!hasConvertibleOption(pair)) return '';
  return '<span class="preferred-badge convertible">전환</span>';
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
  return '우선주';
}

export function formatPreferredTermConfidence(confidence) {
  if (confidence === 'high') return '높음';
  if (confidence === 'medium') return '중간';
  if (confidence === 'low') return '낮음';
  return '확인중';
}

export function formatPreferredTermShort(value, trueLabel, falseLabel) {
  if (value === true) return trueLabel;
  if (value === false) return falseLabel;
  return '확인중';
}

export function formatMinimumDividendLabel(term) {
  const amount = toFiniteNumber(term?.minimumDividendAmount);
  if (amount == null) return '';
  const label = Number.isInteger(amount)
    ? amount.toLocaleString('ko-KR')
    : amount.toLocaleString('ko-KR', { maximumFractionDigits: 2 });
  return `최저 ${label}원`;
}

export function hasPreferredTermSummary(pair) {
  const term = getPreferredTerm(pair);
  return !!formatMinimumDividendLabel(term);
}

export function renderPreferredTermBadges(pair) {
  const term = getPreferredTerm(pair);
  if (!term) return '<span class="preferred-condition-empty">조건 정보 없음</span>';
  const badges = [];
  const minimumLabel = formatMinimumDividendLabel(term);
  if (minimumLabel) badges.push({ className: 'minimum', label: minimumLabel });
  if (term.cumulative) badges.push({ className: 'positive', label: '누적' });
  if (term.convertible) badges.push({ className: 'convertible', label: '전환' });
  if (term.redeemable) badges.push({ className: 'redeemable', label: '상환' });
  if (!badges.length) return '<span class="preferred-condition-empty">표시할 최저배당 조건 없음</span>';

  return `<div class="preferred-condition-badges">${badges.map(badge => (
    `<span class="preferred-badge condition ${escapeHtml(badge.className)}">${escapeHtml(badge.label)}</span>`
  )).join('')}</div>`;
}

export function renderPreferredTermLabel(pair) {
  const term = getPreferredTerm(pair);
  const source = term?.sourceKey ? app.preferredTermSources?.[term.sourceKey] : null;
  if (!source?.url) return '배당 조건';
  const label = source.label || '근거 보기';
  return `<a class="stat-label-link" href="${escapeHtml(source.url)}" target="_blank" rel="noopener" title="${escapeHtml(label)}">배당 조건</a>`;
}

export function renderPreferredTermSummary(pair) {
  const term = getPreferredTerm(pair);
  if (!term) return '-';
  const minimumDividendLabel = formatMinimumDividendLabel(term).replace(/^최저\s*/, '');
  if (!minimumDividendLabel) return '-';
  const rows = [
    { label: '최저배당', value: minimumDividendLabel },
    { label: '누적', value: formatPreferredTermShort(term.cumulative, '있음', '없음') },
    { label: '기준', value: term.minimumDividend || '-' },
    { label: '신뢰도', value: formatPreferredTermConfidence(term.confidence), title: term.note || '' },
  ];

  return `<div class="preferred-term-summary">
    ${renderPreferredTermBadges(pair)}
    <div class="preferred-term-lines">
      ${rows.map(row => `<div class="preferred-term-row">
        <span>${escapeHtml(row.label)}</span>
        <strong${row.title ? ` class="preferred-term-confidence" title="${escapeHtml(row.title)}"` : ''}>${escapeHtml(row.value)}</strong>
      </div>`).join('')}
    </div>
  </div>`;
}
