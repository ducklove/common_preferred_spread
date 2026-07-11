// js/heatmap.js — 전 종목 괴리율 히트맵(트리맵): squarified 레이아웃 + z-score 색상 + 접기 토글
// 색: 현재 괴리율의 역사적 위치(z = (spread - μ) / σ), 면적: 우선주 시총의 제곱근.
import { app, readStoredValue, writeStoredValue } from './state.js';
import { calculateLiveMarketCap } from './calc.js';
import { escapeHtml, formatMarketCap, formatPairName } from './format.js';
import { selectPair } from './views.js';

export const HEATMAP_COLLAPSED_STORAGE_KEY = 'heatmapCollapsed';
const HEATMAP_FALLBACK_WIDTH = 960; // clientWidth가 0인 환경(레이아웃 미계산) 폴백
const HEATMAP_DESKTOP_HEIGHT = 380;
const HEATMAP_MOBILE_HEIGHT = 300;
const HEATMAP_Z_FULL_SCALE = 2.5; // |z|가 이 값 이상이면 최대 색 강도
const HEATMAP_ALPHA_MIN = 0.12;
const HEATMAP_ALPHA_MAX = 0.85;
const HEATMAP_DEEP_INTENSITY = 0.5; // 이 강도 이상이면 흰 글자로 전환
const HEATMAP_COMPACT_MIN_WIDTH = 72; // 이보다 작은 타일은 텍스트 숨김
const HEATMAP_COMPACT_MIN_HEIGHT = 44;

let heatmapCollapsed = readStoredValue(HEATMAP_COLLAPSED_STORAGE_KEY) === '1';

export function computeHeatmapZScore(spread, spreadStats) {
  // spreadStats가 없거나(신규 종목) σ<=0이면 z를 계산하지 않는다 → 중립 타일.
  if (spread == null || Number.isNaN(spread) || !spreadStats) return null;
  const mean = Number(spreadStats.mean);
  const std = Number(spreadStats.std);
  if (!Number.isFinite(mean) || !Number.isFinite(std) || std <= 0) return null;
  return (spread - mean) / std;
}

export function getHeatmapIntensity(z) {
  if (z == null || Number.isNaN(z)) return 0;
  return Math.min(Math.abs(z) / HEATMAP_Z_FULL_SCALE, 1);
}

export function buildHeatmapItems() {
  const items = [];
  app.pairs.forEach((pair, idx) => {
    if (!pair || pair.isAverage || !pair.current) return;
    const current = pair.current;
    const marketCap = calculateLiveMarketCap(
      current.preferredPrice,
      current.preferredSharesOutstanding,
      current.preferredMarketCap,
    );
    const spread = current.spread != null && !Number.isNaN(current.spread) ? current.spread : null;
    items.push({
      pair,
      idx,
      marketCap,
      spread,
      z: computeHeatmapZScore(spread, pair.spreadStats),
      area: marketCap != null && marketCap > 0 ? Math.sqrt(marketCap) : null,
    });
  });

  // 시총 미상 종목도 항상 표시: 유효 면적 최솟값의 0.5배로 폴백, 전부 미상이면 동일 면적.
  const validAreas = items.filter(item => item.area != null && item.area > 0).map(item => item.area);
  const fallbackArea = validAreas.length ? Math.min(...validAreas) * 0.5 : 1;
  items.forEach(item => {
    if (item.area == null || !(item.area > 0)) item.area = fallbackArea;
  });
  items.sort((a, b) => b.area - a.area);
  return items;
}

function worstRowAspect(sum, min, max, side) {
  // 한 줄(row)에 담긴 면적들의 최악 종횡비 (Bruls et al., squarified treemap)
  if (!(sum > 0) || !(side > 0) || !(min > 0)) return Infinity;
  const sumSq = sum * sum;
  const sideSq = side * side;
  return Math.max((sideSq * max) / sumSq, sumSq / (sideSq * min));
}

export function squarify(values, width, height) {
  // 면적 내림차순 입력을 가정. width×height를 빈틈 없이 채우는 {x, y, w, h} 배열을 입력 순서로 반환.
  const sanitized = (Array.isArray(values) ? values : [])
    .map(value => (Number.isFinite(value) && value > 0 ? value : 0));
  const rects = sanitized.map(() => ({ x: 0, y: 0, w: 0, h: 0 }));
  const total = sanitized.reduce((sum, value) => sum + value, 0);
  if (!sanitized.length || total <= 0 || !(width > 0) || !(height > 0)) return rects;

  const scale = (width * height) / total;
  const scaled = sanitized.map(value => value * scale);

  let x = 0;
  let y = 0;
  let remainingWidth = width;
  let remainingHeight = height;
  let index = 0;

  while (index < scaled.length) {
    const side = Math.max(Math.min(remainingWidth, remainingHeight), 0);

    // 짧은 변을 따라 종횡비가 더는 개선되지 않을 때까지 줄에 타일을 추가
    let rowSum = scaled[index];
    let rowMin = scaled[index];
    let rowMax = scaled[index];
    let rowCount = 1;
    let rowWorst = worstRowAspect(rowSum, rowMin, rowMax, side);
    while (index + rowCount < scaled.length) {
      const value = scaled[index + rowCount];
      const nextSum = rowSum + value;
      const nextMin = Math.min(rowMin, value);
      const nextMax = Math.max(rowMax, value);
      const nextWorst = worstRowAspect(nextSum, nextMin, nextMax, side);
      if (nextWorst > rowWorst) break;
      rowSum = nextSum;
      rowMin = nextMin;
      rowMax = nextMax;
      rowWorst = nextWorst;
      rowCount += 1;
    }

    // 남은 영역이 가로로 길면 왼쪽 변에 세로 줄, 세로로 길면 위쪽 변에 가로 줄을 깐다.
    const vertical = remainingWidth >= remainingHeight;
    const thickness = side > 0 ? rowSum / side : 0;
    let offset = 0;
    for (let i = 0; i < rowCount; i++) {
      const length = thickness > 0 ? scaled[index + i] / thickness : 0;
      rects[index + i] = vertical
        ? { x, y: y + offset, w: thickness, h: length }
        : { x: x + offset, y, w: length, h: thickness };
      offset += length;
    }
    if (vertical) {
      x += thickness;
      remainingWidth = Math.max(remainingWidth - thickness, 0);
    } else {
      y += thickness;
      remainingHeight = Math.max(remainingHeight - thickness, 0);
    }
    index += rowCount;
  }
  return rects;
}

function buildHeatmapTileTitle(item) {
  const name = formatPairName(item.pair.name);
  const spreadText = item.spread != null ? `${item.spread.toFixed(1)}%` : '-';
  const mean = Number(item.pair.spreadStats?.mean);
  const zText = item.z != null && Number.isFinite(mean)
    ? `${item.z >= 0 ? '+' : ''}${item.z.toFixed(1)}σ (μ ${mean.toFixed(1)}%)`
    : '-';
  return `${name} · 괴리율 ${spreadText} · z ${zText} · 우선주 시총 ${formatMarketCap(item.marketCap)}`;
}

function renderHeatmapTileHtml(item, rect) {
  const intensity = getHeatmapIntensity(item.z);
  const classes = ['heatmap-tile'];
  if (item.z == null) classes.push('neutral');
  if (intensity >= HEATMAP_DEEP_INTENSITY) classes.push('deep');
  if (rect.w < HEATMAP_COMPACT_MIN_WIDTH || rect.h < HEATMAP_COMPACT_MIN_HEIGHT) classes.push('compact');
  if (item.idx === app.selectedIdx) classes.push('active');

  const styles = [
    `left:${rect.x.toFixed(2)}px`,
    `top:${rect.y.toFixed(2)}px`,
    `width:${rect.w.toFixed(2)}px`,
    `height:${rect.h.toFixed(2)}px`,
  ];
  if (item.z != null) {
    // 강도 0~1 → color-mix 12~85% (z>0 확대=빨강, z<0 축소=파랑, 테마 변수 기반)
    const mixPercent = Math.round((HEATMAP_ALPHA_MIN + intensity * (HEATMAP_ALPHA_MAX - HEATMAP_ALPHA_MIN)) * 100);
    styles.push(`background:color-mix(in srgb, var(${item.z >= 0 ? '--up' : '--down'}) ${mixPercent}%, var(--surface))`);
  }

  const spreadText = item.spread != null ? `${item.spread.toFixed(1)}%` : '-';
  const zText = item.z != null ? ` ${item.z >= 0 ? '+' : ''}${item.z.toFixed(1)}σ` : '';
  // title은 hover 전용이라 터치·스크린리더에서 접근 불가 → 같은 내용을 aria-label로 병행
  const tileTitle = escapeHtml(buildHeatmapTileTitle(item));
  return `<div class="${classes.join(' ')}" role="button" tabindex="0" data-idx="${item.idx}" title="${tileTitle}" aria-label="${tileTitle}" style="${styles.join(';')}">
    <div class="ht-name">${escapeHtml(formatPairName(item.pair.name))}</div>
    <div class="ht-meta">${spreadText}${zText}</div>
  </div>`;
}

export function renderHeatmap() {
  const treemap = document.getElementById('heatmapTreemap');
  if (!treemap) return;
  if (heatmapCollapsed) return; // 접힘 상태에서는 레이아웃 계산을 건너뛴다 (펼칠 때 재렌더)

  const items = buildHeatmapItems();
  if (!items.length) {
    treemap.innerHTML = '';
    return;
  }

  const width = treemap.clientWidth || HEATMAP_FALLBACK_WIDTH;
  const height = treemap.clientHeight
    || (window.innerWidth <= 768 ? HEATMAP_MOBILE_HEIGHT : HEATMAP_DESKTOP_HEIGHT);
  treemap.dataset.layoutWidth = String(width);
  treemap.dataset.layoutHeight = String(height);

  const rects = squarify(items.map(item => item.area), width, height);
  treemap.innerHTML = items.map((item, i) => renderHeatmapTileHtml(item, rects[i])).join('');
}

function applyHeatmapCollapsedState() {
  const body = document.getElementById('heatmapBody');
  const toggleBtn = document.getElementById('heatmapToggleBtn');
  if (body) body.style.display = heatmapCollapsed ? 'none' : '';
  if (toggleBtn) {
    toggleBtn.textContent = heatmapCollapsed ? '펼치기' : '접기';
    toggleBtn.setAttribute('aria-expanded', String(!heatmapCollapsed));
  }
}

export function bindHeatmapControls() {
  if (bindHeatmapControls._bound) return;
  const treemap = document.getElementById('heatmapTreemap');
  const toggleBtn = document.getElementById('heatmapToggleBtn');
  if (!treemap || !toggleBtn) return;

  const legend = document.getElementById('heatmapLegend');
  if (legend) {
    legend.innerHTML = `
      <span class="heatmap-legend-label">축소 ← -2σ</span>
      <span class="heatmap-legend-bar" aria-hidden="true"></span>
      <span class="heatmap-legend-label">+2σ → 확대</span>
    `;
  }

  const activateTile = tile => {
    const idx = parseInt(tile.dataset.idx, 10);
    if (Number.isNaN(idx)) return;
    selectPair(idx, { scrollToChart: true });
    // selectPair는 히트맵을 모르므로(순환 import 방지) active 표시는 여기서 직접 갱신한다.
    renderHeatmap();
  };
  treemap.addEventListener('click', event => {
    const tile = event.target.closest?.('.heatmap-tile');
    if (tile) activateTile(tile);
  });
  treemap.addEventListener('keydown', event => {
    if (event.key !== 'Enter' && event.key !== ' ') return;
    const tile = event.target.closest?.('.heatmap-tile');
    if (!tile) return;
    event.preventDefault();
    activateTile(tile);
  });

  toggleBtn.addEventListener('click', () => {
    heatmapCollapsed = !heatmapCollapsed;
    writeStoredValue(HEATMAP_COLLAPSED_STORAGE_KEY, heatmapCollapsed ? '1' : '0');
    applyHeatmapCollapsedState();
    if (!heatmapCollapsed) renderHeatmap(); // 접힌 동안 생략한 렌더 보충
  });

  applyHeatmapCollapsedState();
  bindHeatmapControls._bound = true;
}
