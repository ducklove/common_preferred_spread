// js/charts.js — 캔버스 차트 엔진 (스케일/세그먼트/축/라인/툴팁/밴드/배당마커/줌 패널/기간·줌 필터/테마 색)
import { app, ZOOM_MIN_WINDOW, ZOOM_RANGE_MAX } from './state.js';
import { escapeHtml, formatAxisPrice, formatKstTimestamp, formatPrice, getHistoryDateMs } from './format.js';
import {
  calculateEmaSeries,
  calculateLatestEma,
  calculateMeanStd,
  calculateSma,
  calculateSmaSeries,
} from './calc.js';
import { getDetailLabels, renderPreferredInlineLabel } from './labels.js';

export function getThemeColor(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

export function getChartTheme() {
  return {
    accent: getThemeColor('--accent'),
    up: getThemeColor('--up'),
    green: getThemeColor('--green'),
    textDim: getThemeColor('--text-dim'),
    grid: getThemeColor('--grid'),
    avgLine: getThemeColor('--avg-line'),
    avgLabel: getThemeColor('--avg-label'),
  };
}

// --- Chart ---
export function renderChart() {
  const p = app.pairs[app.selectedIdx];
  const theme = getChartTheme();
  const titleLabel = p.isAverage ? escapeHtml(p.name) : renderPreferredInlineLabel(p, p.name);
  document.getElementById('chartTitle').innerHTML = `${titleLabel} 괴리율 추이`;

  const canvas = document.getElementById('chart');
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

  const fullHist = p.history || [];
  const hist = getFilteredHistory(p);
  if (hist.length < 2) {
    ctx.clearRect(0, 0, W, H);
    return;
  }

  const spreads = hist.map(h => h.spread);
  const minS = Math.min(...spreads);
  const maxS = Math.max(...spreads);
  const rangeS = maxS - minS || 1;
  const fullSmaSeries = calculateSmaSeries(fullHist.map(entry => entry.spread), 250);
  const fullEmaSeries = calculateEmaSeries(fullHist.map(entry => entry.spread));
  const smaByDate = new Map(fullHist.map((entry, index) => [entry.date, fullSmaSeries[index]]));
  const emaByDate = new Map(fullHist.map((entry, index) => [entry.date, fullEmaSeries[index]]));
  const smaSeries = hist.map(entry => smaByDate.get(entry.date));
  const emaSeries = hist.map(entry => emaByDate.get(entry.date));
  const latestSma = smaSeries[smaSeries.length - 1] ?? calculateSma(spreads, 250);
  const latestEma = emaSeries[emaSeries.length - 1] ?? calculateLatestEma(spreads);

  const pad = { top: 20, right: 16, bottom: 36, left: 56 };
  const cW = W - pad.left - pad.right;
  const cH = H - pad.top - pad.bottom;
  const scale = buildChartScale(hist, pad, cW);
  const spreadSegments = getContinuousSegments(hist, scale);

  const drawTrendSeries = (series, color, dashPattern) => {
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.5;
    ctx.setLineDash(dashPattern);
    const segments = getContinuousSegments(hist, scale, series);
    segments.forEach(([start, end]) => {
      if (end <= start) return;
      ctx.beginPath();
      for (let i = start; i <= end; i++) {
        const value = series[i];
        if (value == null || Number.isNaN(value)) continue;
        const x = scale.xPositions[i];
        const y = pad.top + cH - ((value - minS) / rangeS * cH);
        if (i === start) {
          ctx.moveTo(x, y);
        } else {
          ctx.lineTo(x, y);
        }
      }
      ctx.stroke();
    });
    ctx.setLineDash([]);
  };

  // Clear
  ctx.clearRect(0, 0, W, H);

  // Y axis grid
  ctx.strokeStyle = theme.grid;
  ctx.lineWidth = 1;
  const yTicks = 5;
  ctx.font = '12px -apple-system, sans-serif';
  ctx.fillStyle = theme.textDim;
  ctx.textAlign = 'right';
  for (let i = 0; i <= yTicks; i++) {
    const v = minS + (rangeS * i / yTicks);
    const y = pad.top + cH - (cH * i / yTicks);
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(W - pad.right, y);
    ctx.stroke();
    ctx.fillText(v.toFixed(1) + '%', pad.left - 8, y + 4);
  }

  // X axis labels
  drawXAxis(ctx, pad, cH, cW, H, hist, scale);

  // 통계 밴드: 전체 히스토리 기준 평균(μ)과 ±1σ/±2σ 수평선 (보이는 범위만)
  const bandStats = calculateMeanStd(fullHist.map(entry => entry.spread));
  if (bandStats) {
    const drawStatBandLine = (value, label, isMean) => {
      if (value == null || Number.isNaN(value) || value < minS || value > maxS) return;
      const y = pad.top + cH - ((value - minS) / rangeS * cH);
      ctx.save();
      if (!isMean) ctx.globalAlpha = 0.5;
      ctx.strokeStyle = theme.avgLine;
      ctx.lineWidth = 1;
      ctx.setLineDash([2, 6]);
      ctx.beginPath();
      ctx.moveTo(pad.left, y);
      ctx.lineTo(W - pad.right, y);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.font = '10px -apple-system, sans-serif';
      ctx.fillStyle = theme.avgLabel;
      ctx.textAlign = 'left';
      ctx.fillText(label, pad.left + 4, y - 3);
      ctx.restore();
    };
    drawStatBandLine(bandStats.mean, `μ ${bandStats.mean.toFixed(1)}%`, true);
    if (bandStats.std > 0) {
      drawStatBandLine(bandStats.mean + bandStats.std, '±1σ', false);
      drawStatBandLine(bandStats.mean - bandStats.std, '±1σ', false);
      drawStatBandLine(bandStats.mean + 2 * bandStats.std, '±2σ', false);
      drawStatBandLine(bandStats.mean - 2 * bandStats.std, '±2σ', false);
    }
  }

  // Gradient fill
  const gradient = ctx.createLinearGradient(0, pad.top, 0, pad.top + cH);
  gradient.addColorStop(0, 'rgba(108,140,255,0.25)');
  gradient.addColorStop(1, 'rgba(108,140,255,0.0)');
  ctx.fillStyle = gradient;
  spreadSegments.forEach(([start, end]) => {
    if (end <= start) return;
    ctx.beginPath();
    for (let i = start; i <= end; i++) {
      const x = scale.xPositions[i];
      const y = pad.top + cH - ((hist[i].spread - minS) / rangeS * cH);
      if (i === start) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.lineTo(scale.xPositions[end], pad.top + cH);
    ctx.lineTo(scale.xPositions[start], pad.top + cH);
    ctx.closePath();
    ctx.fill();
  });

  // SMA / EMA trend lines
  drawTrendSeries(smaSeries, theme.avgLine, [6, 4]);
  drawTrendSeries(emaSeries, theme.green, [2, 4]);
  ctx.fillStyle = theme.avgLabel;
  ctx.textAlign = 'right';
  if (latestSma != null && !Number.isNaN(latestSma)) {
    ctx.fillText(`SMA 250 ${latestSma.toFixed(1)}%`, W - pad.right, pad.top + 12);
  }
  if (latestEma != null && !Number.isNaN(latestEma)) {
    ctx.fillStyle = theme.green;
    ctx.fillText(`EMA 0.1 ${latestEma.toFixed(1)}%`, W - pad.right, pad.top + 28);
  }

  // Line
  ctx.strokeStyle = theme.accent;
  ctx.lineWidth = 2;
  spreadSegments.forEach(([start, end]) => {
    if (end <= start) return;
    ctx.beginPath();
    for (let i = start; i <= end; i++) {
      const x = scale.xPositions[i];
      const y = pad.top + cH - ((hist[i].spread - minS) / rangeS * cH);
      if (i === start) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.stroke();
  });

  // Tooltip interaction
  const tooltip = document.getElementById('tooltip');
  const handleMove = (clientX, clientY) => {
    const r = canvas.getBoundingClientRect();
    const mx = clientX - r.left;
    const my = clientY - r.top;
    if (mx < pad.left || mx > W - pad.right || my < pad.top || my > pad.top + cH) {
      tooltip.classList.remove('visible');
      return;
    }
    const idx = findNearestIndexByX(scale.xPositions, mx);
    const d = hist[idx];
    if (!d) return;

    const dotX = scale.xPositions[idx];
    if (Math.abs(dotX - mx) > 24) {
      tooltip.classList.remove('visible');
      return;
    }
    const dotY = pad.top + cH - ((d.spread - minS) / rangeS * cH);

    tooltip.innerHTML = `
      <div class="tt-date">${d.date}</div>
      <div class="tt-row"><span>보통주</span><span>${formatPrice(d.commonPrice)}</span></div>
      <div class="tt-row"><span>우선주</span><span>${formatPrice(d.preferredPrice)}</span></div>
      <div class="tt-row"><span><strong>괴리율</strong></span><span><strong>${d.spread.toFixed(2)}%</strong></span></div>
    `;

    let tx = dotX + 12;
    let ty = dotY - 60;
    if (tx + 180 > W) tx = dotX - 180;
    if (ty < 0) ty = dotY + 12;
    tooltip.style.left = tx + 'px';
    tooltip.style.top = ty + 'px';
    tooltip.classList.add('visible');
  };
  canvas.onmousemove = e => handleMove(e.clientX, e.clientY);
  canvas.ontouchstart = canvas.ontouchmove = function(e) {
    const touch = e.touches[0];
    if (touch) handleMove(touch.clientX, touch.clientY);
  };
  canvas.ontouchend = function() {
    tooltip.classList.remove('visible');
  };
  canvas.onmouseleave = function() {
    tooltip.classList.remove('visible');
  };
}

// --- Price Chart ---
export function renderPriceChart() {
  const p = app.pairs[app.selectedIdx];
  const isAvg = !!p.isAverage;
  const hist = getFilteredHistory(p);
  const theme = getChartTheme();
  const detailLabels = isAvg ? null : getDetailLabels(p);
  if (hist.length < 2) return;

  const dividendEntries = isAvg ? null : app.dividendHistories?.[p.id]?.preferred;
  const hasDividendMarks = Array.isArray(dividendEntries) && dividendEntries.length > 0;

  // Title & legend
  const titleEl = document.getElementById('priceChartTitle');
  const legendEl = document.getElementById('priceLegend');
  if (isAvg) {
    titleEl.textContent = 'KOSPI 지수 추이';
    legendEl.innerHTML = '<span class="leg-kospi">KOSPI</span>';
  } else {
    titleEl.innerHTML = `${renderPreferredInlineLabel(p, p.name)} 시세 추이`;
    legendEl.innerHTML = `<span class="leg-common">${detailLabels.common}</span><span class="leg-preferred">${renderPreferredInlineLabel(p, detailLabels.preferred)}</span>${hasDividendMarks ? '<span class="leg-dividend">배당(우)</span>' : ''}`;
  }

  const canvas = document.getElementById('priceChart');
  const container = canvas.parentElement;
  const dpr = window.devicePixelRatio || 1;
  const rect = container.getBoundingClientRect();
  // Canvas height = container height minus title/legend area
  const legendBottom = legendEl.getBoundingClientRect().bottom;
  const canvasH = rect.bottom - legendBottom;
  canvas.width = rect.width * dpr;
  canvas.height = canvasH * dpr;
  canvas.style.width = rect.width + 'px';
  canvas.style.height = canvasH + 'px';

  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  const W = rect.width;
  const H = canvasH;

  const pad = { top: 16, right: 16, bottom: 32, left: 56 };
  const cW = W - pad.left - pad.right;
  const cH = H - pad.top - pad.bottom;

  ctx.clearRect(0, 0, W, H);

  if (isAvg) {
    // Single line: KOSPI
    const prices = hist.map(h => h.kospiPrice).filter(v => v != null);
    if (prices.length < 2) return;
    const kospiHist = hist.filter(h => h.kospiPrice != null);
    const minP = 0;
    const maxP = Math.max(...prices);
    const rangeP = maxP || 1;
    const scale = buildChartScale(kospiHist, pad, cW);

    drawYAxis(ctx, pad, cW, cH, W, minP, maxP, rangeP, '', 'left');
    drawXAxis(ctx, pad, cH, cW, H, kospiHist, scale);
    drawLine(ctx, pad, cW, cH, kospiHist, scale, d => d.kospiPrice, minP, rangeP, theme.green, true);

    // Tooltip
    setupPriceTooltip(canvas, pad, W, cW, cH, kospiHist, scale, (d) => {
      return `<div class="tt-date">${d.date}</div>
        <div class="tt-row"><span>KOSPI</span><span>${d.kospiPrice.toLocaleString('ko-KR')}</span></div>`;
    }, d => d.kospiPrice, minP, rangeP);
  } else {
    // Single shared axis: min=0, max from both series
    const allPrices = hist.flatMap(h => [h.commonPrice, h.preferredPrice]);
    const minP = 0;
    const maxP = Math.max(...allPrices);
    const rangeP = maxP || 1;
    const scale = buildChartScale(hist, pad, cW);

    drawYAxis(ctx, pad, cW, cH, W, minP, maxP, rangeP, '', 'left');
    drawXAxis(ctx, pad, cH, cW, H, hist, scale);
    drawLine(ctx, pad, cW, cH, hist, scale, d => d.commonPrice, minP, rangeP, theme.accent, false);
    drawLine(ctx, pad, cW, cH, hist, scale, d => d.preferredPrice, minP, rangeP, theme.up, false);

    // 배당(우선주) 마커: 표시 구간 내 배당 일자를 차트 하단에 점으로 표시
    if (hasDividendMarks) {
      const firstDate = hist[0]?.date || '';
      const lastDate = hist[hist.length - 1]?.date || '';
      ctx.fillStyle = theme.green;
      dividendEntries.forEach(entry => {
        if (!entry?.date || entry.date < firstDate || entry.date > lastDate) return;
        const time = getHistoryDateMs(entry.date);
        if (time == null) return;
        const x = pad.left + (((time - scale.minTime) / scale.rangeTime) * cW);
        ctx.beginPath();
        ctx.arc(x, pad.top + cH - 4, 2.5, 0, Math.PI * 2);
        ctx.fill();
      });
    }

    // Tooltip
    setupPriceTooltip(canvas, pad, W, cW, cH, hist, scale, (d) => {
      return `<div class="tt-date">${d.date}</div>
        <div class="tt-row"><span>${detailLabels.common}</span><span>${formatPrice(d.commonPrice)}</span></div>
        <div class="tt-row"><span>${detailLabels.preferred}</span><span>${formatPrice(d.preferredPrice)}</span></div>
        <div class="tt-row"><span><strong>괴리율</strong></span><span><strong>${d.spread.toFixed(2)}%</strong></span></div>`;
    }, d => d.commonPrice, minP, rangeP);
  }
}

export function drawYAxis(ctx, pad, cW, cH, W, minV, maxV, range, suffix, side, color) {
  const theme = getChartTheme();
  const yTicks = 4;
  ctx.font = '11px -apple-system, sans-serif';
  for (let i = 0; i <= yTicks; i++) {
    const v = minV + (range * i / yTicks);
    const y = pad.top + cH - (cH * i / yTicks);
    // Grid line (only draw once, from left axis)
    if (side === 'left') {
      ctx.strokeStyle = theme.grid;
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(pad.left, y);
      ctx.lineTo(W - pad.right, y);
      ctx.stroke();
    }
    ctx.fillStyle = color || theme.textDim;
    if (side === 'left') {
      ctx.textAlign = 'right';
      ctx.fillText(formatAxisPrice(v) + suffix, pad.left - 8, y + 4);
    } else {
      ctx.textAlign = 'left';
      ctx.fillText(formatAxisPrice(v) + suffix, W - pad.right + 8, y + 4);
    }
  }
}

export function drawXAxis(ctx, pad, cH, cW, H, hist, scale = buildChartScale(hist, pad, cW)) {
  ctx.fillStyle = getChartTheme().textDim;
  ctx.textAlign = 'center';
  ctx.font = '11px -apple-system, sans-serif';
  const xLabelCount = Math.min(6, hist.length);
  if (xLabelCount <= 0) return;
  if (xLabelCount === 1) {
    ctx.fillText(formatChartDateLabel(scale.minTime), pad.left, pad.top + cH + 20);
    return;
  }
  for (let i = 0; i < xLabelCount; i++) {
    const ratio = i / (xLabelCount - 1);
    const x = pad.left + (ratio * cW);
    const labelTime = scale.minTime + (scale.rangeTime * ratio);
    ctx.fillText(formatChartDateLabel(labelTime), x, pad.top + cH + 20);
  }
}

export function hexToRgba(hex, alpha) {
  const r = parseInt(hex.slice(1,3), 16);
  const g = parseInt(hex.slice(3,5), 16);
  const b = parseInt(hex.slice(5,7), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}

export function drawLine(ctx, pad, cW, cH, hist, scale, getValue, minV, range, color, fill) {
  const segments = getContinuousSegments(hist, scale);
  if (fill) {
    const gradient = ctx.createLinearGradient(0, pad.top, 0, pad.top + cH);
    gradient.addColorStop(0, hexToRgba(color, 0.18));
    gradient.addColorStop(1, hexToRgba(color, 0.0));
    ctx.fillStyle = gradient;
    segments.forEach(([start, end]) => {
      if (end <= start) return;
      ctx.beginPath();
      for (let i = start; i <= end; i++) {
        const x = scale.xPositions[i];
        const y = pad.top + cH - ((getValue(hist[i]) - minV) / range * cH);
        if (i === start) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      }
      ctx.lineTo(scale.xPositions[end], pad.top + cH);
      ctx.lineTo(scale.xPositions[start], pad.top + cH);
      ctx.closePath();
      ctx.fill();
    });
  }
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.5;
  segments.forEach(([start, end]) => {
    if (end <= start) return;
    ctx.beginPath();
    for (let i = start; i <= end; i++) {
      const x = scale.xPositions[i];
      const y = pad.top + cH - ((getValue(hist[i]) - minV) / range * cH);
      if (i === start) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.stroke();
  });
}

export function setupPriceTooltip(canvas, pad, W, cW, cH, hist, scale, htmlFn, getValue, minV, range) {
  const tooltip = document.getElementById('priceTooltip');
  const handleMove = (clientX, clientY) => {
    const r = canvas.getBoundingClientRect();
    const mx = clientX - r.left;
    const my = clientY - r.top;
    if (mx < pad.left || mx > W - pad.right || my < pad.top || my > pad.top + cH) {
      tooltip.classList.remove('visible');
      return;
    }
    const idx = findNearestIndexByX(scale.xPositions, mx);
    const d = hist[idx];
    if (!d) return;

    tooltip.innerHTML = htmlFn(d);

    const dotX = scale.xPositions[idx];
    if (Math.abs(dotX - mx) > 24) {
      tooltip.classList.remove('visible');
      return;
    }
    const dotY = pad.top + cH - ((getValue(d) - minV) / range * cH);
    let tx = dotX + 12;
    let ty = dotY - 60;
    if (tx + 180 > W) tx = dotX - 180;
    if (ty < 0) ty = dotY + 12;
    tooltip.style.left = tx + 'px';
    tooltip.style.top = ty + 'px';
    tooltip.classList.add('visible');
  };
  canvas.onmousemove = e => handleMove(e.clientX, e.clientY);
  canvas.ontouchstart = canvas.ontouchmove = function(e) {
    const touch = e.touches[0];
    if (touch) handleMove(touch.clientX, touch.clientY);
  };
  canvas.ontouchend = function() {
    tooltip.classList.remove('visible');
  };
  canvas.onmouseleave = function() {
    tooltip.classList.remove('visible');
  };
}

const DAY_MS = 24 * 60 * 60 * 1000;
const MIN_CHART_GAP_DAYS = 10;
const CHART_DATE_LABEL_FORMATTER = new Intl.DateTimeFormat('sv-SE', {
  timeZone: 'Asia/Seoul',
  year: '2-digit',
  month: '2-digit',
  day: '2-digit',
});

export function buildChartScale(hist, pad, cW) {
  const times = hist.map(entry => getHistoryDateMs(entry.date));
  const validTimes = times.filter(time => time != null);
  const minTime = validTimes[0] ?? 0;
  const maxTime = validTimes[validTimes.length - 1] ?? (minTime + DAY_MS);
  const rangeTime = Math.max(maxTime - minTime, DAY_MS);
  const diffs = [];

  for (let i = 1; i < times.length; i++) {
    if (times[i] == null || times[i - 1] == null) continue;
    const diff = times[i] - times[i - 1];
    if (diff > 0) diffs.push(diff);
  }

  diffs.sort((a, b) => a - b);
  const medianDiff = diffs.length ? diffs[Math.floor(diffs.length / 2)] : DAY_MS;
  const gapThresholdMs = Math.max(MIN_CHART_GAP_DAYS * DAY_MS, medianDiff * 4);
  const xPositions = times.map(time => (
    time == null ? pad.left : pad.left + (((time - minTime) / rangeTime) * cW)
  ));

  return { times, xPositions, minTime, maxTime, rangeTime, gapThresholdMs };
}

export function getContinuousSegments(hist, scale, series = null) {
  const segments = [];
  let start = null;

  for (let i = 0; i < hist.length; i++) {
    const time = scale.times[i];
    const value = series ? series[i] : 0;
    const hasValue = series == null || (value != null && !Number.isNaN(value));
    const hasGap = (
      i > 0
      && scale.times[i] != null
      && scale.times[i - 1] != null
      && (scale.times[i] - scale.times[i - 1] > scale.gapThresholdMs)
    );

    if (time == null || !hasValue) {
      if (start != null && i - 1 >= start) segments.push([start, i - 1]);
      start = null;
      continue;
    }

    if (hasGap) {
      if (start != null && i - 1 >= start) segments.push([start, i - 1]);
      start = i;
      continue;
    }

    if (start == null) start = i;
  }

  if (start != null && hist.length - 1 >= start) {
    segments.push([start, hist.length - 1]);
  }

  return segments;
}

export function formatChartDateLabel(timestampMs) {
  return CHART_DATE_LABEL_FORMATTER.format(new Date(timestampMs));
}

export function findNearestIndexByX(xPositions, targetX) {
  if (!xPositions.length) return -1;

  let low = 0;
  let high = xPositions.length - 1;

  while (low < high) {
    const mid = Math.floor((low + high) / 2);
    if (xPositions[mid] < targetX) low = mid + 1;
    else high = mid;
  }

  if (low === 0) return 0;
  const prev = low - 1;
  return Math.abs(xPositions[low] - targetX) < Math.abs(xPositions[prev] - targetX)
    ? low
    : prev;
}

export function bindZoomControls() {
  const startInput = document.getElementById('zoomStart');
  const endInput = document.getElementById('zoomEnd');
  const resetBtn = document.getElementById('zoomResetBtn');
  if (!startInput || !endInput || !resetBtn || bindZoomControls._bound) return;

  const handleInput = (source) => {
    const nextStart = source === 'start' ? Number(startInput.value) : app.zoomWindow.start;
    const nextEnd = source === 'end' ? Number(endInput.value) : app.zoomWindow.end;
    setZoomWindow(nextStart, nextEnd);
  };

  startInput.addEventListener('input', () => handleInput('start'));
  endInput.addEventListener('input', () => handleInput('end'));
  resetBtn.addEventListener('click', () => resetZoomWindow());
  bindZoomControls._bound = true;
}

// --- Helpers ---
export function getPeriodFilteredHistory(pair) {
  const hist = Array.isArray(pair?.history) ? pair.history : [];
  if (app.periodDays === 0) return hist;
  const cutStr = formatKstTimestamp(new Date(Date.now() - app.periodDays * 24 * 60 * 60 * 1000)).slice(0, 10);
  return hist.filter(h => h.date >= cutStr);
}

export function clampZoomWindow(start, end) {
  let nextStart = Math.max(0, Math.min(Number.isFinite(start) ? start : 0, ZOOM_RANGE_MAX));
  let nextEnd = Math.max(0, Math.min(Number.isFinite(end) ? end : ZOOM_RANGE_MAX, ZOOM_RANGE_MAX));

  if (nextEnd - nextStart < ZOOM_MIN_WINDOW) {
    if (nextStart !== app.zoomWindow.start) {
      nextStart = Math.max(0, nextEnd - ZOOM_MIN_WINDOW);
    } else {
      nextEnd = Math.min(ZOOM_RANGE_MAX, nextStart + ZOOM_MIN_WINDOW);
    }
  }

  if (nextEnd - nextStart < ZOOM_MIN_WINDOW) {
    nextStart = Math.max(0, Math.min(nextStart, ZOOM_RANGE_MAX - ZOOM_MIN_WINDOW));
    nextEnd = Math.min(ZOOM_RANGE_MAX, nextStart + ZOOM_MIN_WINDOW);
  }

  return { start: nextStart, end: nextEnd };
}

export function syncZoomInputs() {
  const startInput = document.getElementById('zoomStart');
  const endInput = document.getElementById('zoomEnd');
  if (startInput) startInput.value = String(app.zoomWindow.start);
  if (endInput) endInput.value = String(app.zoomWindow.end);
}

export function setZoomWindow(start, end, { render = true } = {}) {
  app.zoomWindow = clampZoomWindow(start, end);
  syncZoomInputs();
  if (render) {
    renderZoomPanel();
    renderChart();
    renderPriceChart();
  }
}

export function resetZoomWindow({ render = true } = {}) {
  app.zoomWindow = { start: 0, end: ZOOM_RANGE_MAX };
  syncZoomInputs();
  if (render) {
    renderZoomPanel();
    renderChart();
    renderPriceChart();
  }
}

export function getFilteredHistory(pair) {
  const hist = getPeriodFilteredHistory(pair);
  if (!Array.isArray(hist) || hist.length <= 2) return hist;
  if (app.zoomWindow.start <= 0 && app.zoomWindow.end >= ZOOM_RANGE_MAX) return hist;

  const scale = buildChartScale(hist, { left: 0 }, ZOOM_RANGE_MAX);
  const startTime = scale.minTime + (scale.rangeTime * (app.zoomWindow.start / ZOOM_RANGE_MAX));
  const endTime = scale.minTime + (scale.rangeTime * (app.zoomWindow.end / ZOOM_RANGE_MAX));
  const filtered = hist.filter((entry, idx) => {
    const time = scale.times[idx];
    return time != null && time >= startTime && time <= endTime;
  });

  if (filtered.length >= 2) return filtered;

  const startIdx = Math.max(0, Math.floor((hist.length - 1) * (app.zoomWindow.start / ZOOM_RANGE_MAX)));
  const endIdx = Math.min(hist.length - 1, Math.ceil((hist.length - 1) * (app.zoomWindow.end / ZOOM_RANGE_MAX)));
  return hist.slice(startIdx, Math.max(startIdx + 2, endIdx + 1));
}

export function renderZoomPanel() {
  const panel = document.getElementById('zoomPanel');
  const canvas = document.getElementById('zoomChart');
  const selection = document.getElementById('zoomSelection');
  const rangeLabel = document.getElementById('zoomRangeLabel');
  if (!panel || !canvas || !selection || !rangeLabel) return;

  const hist = getPeriodFilteredHistory(app.pairs[app.selectedIdx]);
  if (!Array.isArray(hist) || hist.length < 2) {
    panel.style.display = 'none';
    return;
  }

  panel.style.display = '';
  syncZoomInputs();

  const zoomedHist = getFilteredHistory(app.pairs[app.selectedIdx]);
  const startDate = zoomedHist[0]?.date || hist[0]?.date || '-';
  const endDate = zoomedHist[zoomedHist.length - 1]?.date || hist[hist.length - 1]?.date || '-';
  rangeLabel.textContent = `${startDate} ~ ${endDate} · ${zoomedHist.length}개`;

  const leftPct = (app.zoomWindow.start / ZOOM_RANGE_MAX) * 100;
  const widthPct = ((app.zoomWindow.end - app.zoomWindow.start) / ZOOM_RANGE_MAX) * 100;
  selection.style.left = `${leftPct}%`;
  selection.style.width = `${Math.max(widthPct, (ZOOM_MIN_WINDOW / ZOOM_RANGE_MAX) * 100)}%`;

  const dpr = window.devicePixelRatio || 1;
  const rect = panel.querySelector('.zoom-overview').getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  canvas.style.width = rect.width + 'px';
  canvas.style.height = rect.height + 'px';

  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, rect.width, rect.height);

  const theme = getChartTheme();
  const pad = { top: 8, right: 8, bottom: 8, left: 8 };
  const cW = rect.width - pad.left - pad.right;
  const cH = rect.height - pad.top - pad.bottom;
  const spreads = hist.map(item => item.spread).filter(value => value != null && !Number.isNaN(value));
  if (!spreads.length) return;

  const minS = Math.min(...spreads);
  const maxS = Math.max(...spreads);
  const rangeS = maxS - minS || 1;
  const scale = buildChartScale(hist, pad, cW);
  const segments = getContinuousSegments(hist, scale);

  const gradient = ctx.createLinearGradient(0, pad.top, 0, pad.top + cH);
  gradient.addColorStop(0, hexToRgba(theme.accent, 0.24));
  gradient.addColorStop(1, hexToRgba(theme.accent, 0.02));
  ctx.fillStyle = gradient;
  segments.forEach(([start, end]) => {
    if (end <= start) return;
    ctx.beginPath();
    for (let i = start; i <= end; i++) {
      const x = scale.xPositions[i];
      const y = pad.top + cH - ((hist[i].spread - minS) / rangeS * cH);
      if (i === start) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.lineTo(scale.xPositions[end], pad.top + cH);
    ctx.lineTo(scale.xPositions[start], pad.top + cH);
    ctx.closePath();
    ctx.fill();
  });

  ctx.strokeStyle = theme.accent;
  ctx.lineWidth = 1.25;
  segments.forEach(([start, end]) => {
    if (end <= start) return;
    ctx.beginPath();
    for (let i = start; i <= end; i++) {
      const x = scale.xPositions[i];
      const y = pad.top + cH - ((hist[i].spread - minS) / rangeS * cH);
      if (i === start) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.stroke();
  });
}
