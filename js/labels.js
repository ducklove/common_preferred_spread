// js/labels.js — 종목 라벨/배지 HTML 헬퍼 (charts/views 공용)
import {
  PREFERRED_ADDITIONAL_DIVIDEND_OVERRIDES,
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
  const raw = term?.minimumDividend || '';
  const label = raw
    .replace(/^액면가 기준\s*/, '')
    .replace(/^보통주 대비\s*/, '')
    .trim();
  return label ? `최저 ${label}` : '';
}

export function renderPreferredTermBadges(pair) {
  const term = getPreferredTerm(pair);
  if (!term) return '<span class="preferred-condition-empty">조건 정보 없음</span>';
  const badges = [
    {
      className: term.cumulative === true ? 'positive' : 'neutral',
      label: formatPreferredTermShort(term.cumulative, '누적', '비누적'),
    },
    {
      className: term.participating === true ? 'positive' : 'neutral',
      label: formatPreferredTermShort(term.participating, '참가', '비참가'),
    },
  ];
  if (term.convertible) badges.push({ className: 'convertible', label: '전환' });
  if (term.redeemable) badges.push({ className: 'redeemable', label: '상환' });
  const minimumLabel = formatMinimumDividendLabel(term);
  if (minimumLabel) badges.push({ className: 'minimum', label: minimumLabel });

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
  const rows = [
    { label: '종류', value: term.classText || '-' },
    {
      label: '배당',
      value: [
        formatPreferredTermShort(term.cumulative, '누적', '비누적'),
        formatPreferredTermShort(term.participating, '참가', '비참가'),
      ].join(' / '),
    },
    { label: '우선배당', value: term.minimumDividend || term.additionalDividend || '-' },
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
