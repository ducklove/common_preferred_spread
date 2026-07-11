// js/radar.js — 투자매력도 방사형(레이더) 차트 + 축별 점수 분해 패널
// 점수는 fetch_data.py(attractiveness.py)가 일별 계산해 summary.json에 싣는다.
// 축당 20점 × 5축 = 100점. 축 순서/라벨은 attractiveness.py의 AXIS_KEYS와 일치.
import { app } from './state.js';
import { getChartTheme, getThemeColor, hexToRgba } from './charts.js';
import { escapeHtml, formatMarketCap, formatPairName, formatTradedValue } from './format.js';
import { renderPreferredInlineLabel } from './labels.js';

export const ATTRACTIVENESS_AXES = [
  { key: 'spread', label: '괴리율' },
  { key: 'spreadPosition', label: '괴리율 이격도' },
  { key: 'liquidity', label: '유동성' },
  { key: 'dividend', label: '배당가치' },
  { key: 'health', label: '본주 건전성' },
];
export const ATTRACTIVENESS_AXIS_MAX = 20;

function formatScore(value) {
  return value == null || Number.isNaN(value) ? '-' : value.toFixed(1);
}

function formatDetailNumber(value, suffix = '', decimals = 1) {
  if (value == null || Number.isNaN(value)) return '-';
  return `${Number(value).toFixed(decimals)}${suffix}`;
}

// 각 축의 하위 지표 요약 문구 (분해 패널의 보조 라인)
function getAxisDetailText(axisKey, pair) {
  const a = pair.attractiveness;
  const c = pair.current || {};
  const d = a?.details || {};
  switch (axisKey) {
    case 'spread':
      return d.spreadAnchor == null
        ? `현재 괴리율 ${formatDetailNumber(c.spread, '%')}`
        : `현재 괴리율 ${formatDetailNumber(c.spread, '%')} · 만점 기준 ${formatDetailNumber(d.spreadAnchor, '%')} (전 종목 최고)`;
    case 'spreadPosition':
      return d.spreadPct3y == null
        ? '3년 분포 데이터 부족'
        : `최근 3년 분포의 ${formatDetailNumber(d.spreadPct3y, '')} 백분위 (${d.spreadWindowDays}일)`;
    case 'liquidity':
      return `우선주 시총 ${formatMarketCap(c.preferredMarketCap)} · 1개월 일평균 거래 ${formatTradedValue(c.preferredAvgTradedValue20)}`;
    case 'dividend': {
      const gap = c.preferredDivYield != null && c.commonDivYield != null
        ? c.preferredDivYield - c.commonDivYield
        : null;
      const fiveYear = d.divYield5y == null
        ? '5년 -'
        : `5년 평균 ${formatDetailNumber(d.divYield5y, '%')} (${d.divYield5yYears}개년)`;
      return `우선주 ${formatDetailNumber(c.preferredDivYield, '%')} · 본주 대비 ${formatDetailNumber(gap, '%p')} · ${fiveYear}`;
    }
    case 'health': {
      const profit = d.netIncomeYears
        ? `흑자 ${d.netIncomePositiveYears}/${d.netIncomeYears}개년`
        : '실적 -';
      return `시총 ${formatMarketCap(c.commonMarketCap)} · PER ${formatDetailNumber(d.per, '')} · PBR ${formatDetailNumber(d.pbr, '', 2)} · 외국인 ${formatDetailNumber(d.foreignRatio, '%')} · ${profit}`;
    }
    default:
      return '';
  }
}

function drawRadarChart(canvas, scores) {
  const theme = getChartTheme();
  const textColor = getThemeColor('--text');
  const container = canvas.parentElement;
  const dpr = window.devicePixelRatio || 1;
  const rect = container.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  canvas.style.width = rect.width + 'px';
  canvas.style.height = rect.height + 'px';
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  const W = rect.width;
  const H = rect.height;
  ctx.clearRect(0, 0, W, H);

  const cx = W / 2;
  const cy = H / 2 + 6; // 상단 라벨 여유
  // 좌우 수평 축 라벨("괴리율 이격도" 등 6자)이 잘리지 않도록 가로 여백을 더 크게 잡는다
  const radius = Math.max(40, Math.min(W / 2 - 82, H / 2 - 40));
  const count = ATTRACTIVENESS_AXES.length;
  const angleAt = i => -Math.PI / 2 + (i * 2 * Math.PI) / count;
  const pointAt = (i, r) => [cx + Math.cos(angleAt(i)) * r, cy + Math.sin(angleAt(i)) * r];

  // 눈금 오각형 (5/10/15/20) — 배경 그리드는 은은하게
  ctx.strokeStyle = theme.grid;
  ctx.lineWidth = 1;
  for (let level = 1; level <= 4; level++) {
    const r = (radius * level) / 4;
    ctx.beginPath();
    for (let i = 0; i <= count; i++) {
      const [x, y] = pointAt(i % count, r);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.stroke();
  }
  // 축 스포크
  for (let i = 0; i < count; i++) {
    const [x, y] = pointAt(i, radius);
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.lineTo(x, y);
    ctx.stroke();
  }
  // 눈금 값 (수직 축 위 10/20만 — 그리드는 조용하게 유지)
  ctx.fillStyle = theme.textDim;
  ctx.font = '9px sans-serif';
  ctx.textAlign = 'left';
  ctx.textBaseline = 'middle';
  ctx.fillText('10', cx + 3, cy - radius / 2);
  ctx.fillText('20', cx + 3, cy - radius);

  // 점수 폴리곤
  const values = ATTRACTIVENESS_AXES.map(axis => {
    const value = scores?.[axis.key];
    return Math.max(0, Math.min(ATTRACTIVENESS_AXIS_MAX, value == null ? 0 : value));
  });
  ctx.beginPath();
  values.forEach((value, i) => {
    const [x, y] = pointAt(i, (radius * value) / ATTRACTIVENESS_AXIS_MAX);
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.closePath();
  ctx.fillStyle = hexToRgba(theme.accent, 0.16);
  ctx.fill();
  ctx.strokeStyle = theme.accent;
  ctx.lineWidth = 2;
  ctx.stroke();
  values.forEach((value, i) => {
    const [x, y] = pointAt(i, (radius * value) / ATTRACTIVENESS_AXIS_MAX);
    ctx.beginPath();
    ctx.arc(x, y, 3, 0, Math.PI * 2);
    ctx.fillStyle = theme.accent;
    ctx.fill();
  });

  // 축 라벨 + 점수 직접 표기 (각도별 정렬)
  ATTRACTIVENESS_AXES.forEach((axis, i) => {
    const angle = angleAt(i);
    const [x, y] = pointAt(i, radius + 12);
    const cos = Math.cos(angle);
    ctx.textAlign = Math.abs(cos) < 0.3 ? 'center' : cos > 0 ? 'left' : 'right';
    ctx.textBaseline = Math.sin(angle) < -0.3 ? 'bottom' : Math.sin(angle) > 0.3 ? 'top' : 'middle';
    ctx.fillStyle = theme.textDim;
    ctx.font = '11px sans-serif';
    ctx.fillText(axis.label, x, y);
    const scoreOffset = ctx.textBaseline === 'bottom' ? -13 : 13;
    ctx.fillStyle = textColor;
    ctx.font = 'bold 11px sans-serif';
    ctx.fillText(formatScore(values[i]), x, y + scoreOffset);
  });
}

function renderBreakdown(container, pair) {
  const scores = pair.attractiveness?.scores || {};
  container.innerHTML = ATTRACTIVENESS_AXES.map(axis => {
    const score = scores[axis.key];
    const width = Math.max(0, Math.min(100, ((score || 0) / ATTRACTIVENESS_AXIS_MAX) * 100));
    return `<div class="attractiveness-row">
      <div class="attractiveness-row-head">
        <span class="attractiveness-row-label">${escapeHtml(axis.label)}</span>
        <span class="attractiveness-row-score">${formatScore(score)}<span class="attractiveness-row-max">/${ATTRACTIVENESS_AXIS_MAX}</span></span>
      </div>
      <div class="attractiveness-bar"><div class="attractiveness-bar-fill" style="width:${width}%"></div></div>
      <div class="attractiveness-row-detail">${escapeHtml(getAxisDetailText(axis.key, pair))}</div>
    </div>`;
  }).join('');
}

export function renderAttractivenessSection() {
  const section = document.getElementById('attractivenessSection');
  if (!section) return;
  const pair = app.pairs[app.selectedIdx];
  const attractiveness = pair && !pair.isAverage ? pair.attractiveness : null;
  if (!attractiveness) {
    section.hidden = true;
    return;
  }
  section.hidden = false;
  const titleLabel = renderPreferredInlineLabel(pair, pair.name);
  document.getElementById('attractivenessTitle').innerHTML = `${titleLabel} 투자매력도`;
  const totalEl = document.getElementById('attractivenessTotal');
  totalEl.innerHTML = `${formatScore(attractiveness.total)}<span class="attractiveness-total-max">/100</span>`;
  const radarCanvas = document.getElementById('radarChart');
  radarCanvas.setAttribute(
    'aria-label',
    `${formatPairName(pair.name)} 투자매력도 레이더 차트: 총점 ${formatScore(attractiveness.total)}/100`,
  );
  drawRadarChart(radarCanvas, attractiveness.scores);
  renderBreakdown(document.getElementById('attractivenessBreakdown'), pair);
}
