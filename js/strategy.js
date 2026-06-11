// js/strategy.js — 전략 분석(백테스트) 섹션: analysis/outputs CSV 지연 로드 → 요약 stat / 로그 스케일 에쿼티 커브 / 연도별·이벤트 스터디 테이블 / 접기 토글
import { readStoredValue, writeStoredValue } from './state.js';
import { escapeHtml, getHistoryDateMs, toFiniteNumber } from './format.js';
import {
  buildChartScale,
  drawXAxis,
  findNearestIndexByX,
  getChartTheme,
  getContinuousSegments,
} from './charts.js';

export const STRATEGY_COLLAPSED_STORAGE_KEY = 'strategyCollapsed';
const STRATEGY_DAILY_CSV_URL = 'analysis/outputs/top3_spread_strategy_daily.csv';
const STRATEGY_ANNUAL_CSV_URL = 'analysis/outputs/top3_spread_strategy_annual.csv';
const STRATEGY_POOLED_CSV_URL = 'analysis/outputs/pooled_summary.csv';
const YEAR_MS = 365.25 * 24 * 60 * 60 * 1000;
const LOAD_ERROR_HTML = '<div class="strategy-data-error">데이터를 불러올 수 없습니다</div>';
// 이벤트 스터디 1차 스펙(5일 신호·20일 성과)과 가설별 측정 지표 (H1/H2=보통주, H3=우선주 수익률)
const EVENT_STUDY_LOOKBACK = '5';
const EVENT_STUDY_HORIZON = '20';
const EVENT_STUDY_HYPOTHESES = [
  { id: 'H1', desc: '보통주 급등 + 괴리 확대 → 보통주 수익률', metric: 'fwd_common_return' },
  { id: 'H2', desc: '보통주 급등 + 괴리 유지 → 보통주 수익률', metric: 'fwd_common_return' },
  { id: 'H3', desc: '우선주 급등 + 괴리 축소 → 우선주 수익률', metric: 'fwd_preferred_return' },
];

let strategyCollapsed = readStoredValue(STRATEGY_COLLAPSED_STORAGE_KEY) === '1';
let strategyLoadPromise = null; // CSV 3종 병렬 fetch 1회 캐시
let strategyData = null; // { daily, annual, pooled } — 각 항목은 실패 시 null
let strategyContentRendered = false;
let dailySeriesCache = null;

// --- CSV 파서 (따옴표 필드·이중 따옴표 이스케이프·CRLF·BOM 처리) ---
export function parseCsv(text) {
  const src = String(text ?? '').replace(/^\ufeff/, ''); // UTF-8 BOM 제거
  const rows = [];
  let row = [];
  let field = '';
  let quoted = false;
  for (let i = 0; i < src.length; i++) {
    const ch = src[i];
    if (quoted) {
      if (ch === '"' && src[i + 1] === '"') { field += '"'; i += 1; }
      else if (ch === '"') quoted = false;
      else field += ch;
    } else if (ch === '"') {
      quoted = true;
    } else if (ch === ',') {
      row.push(field); field = '';
    } else if (ch === '\n' || ch === '\r') {
      if (ch === '\r' && src[i + 1] === '\n') i += 1;
      row.push(field); rows.push(row); field = ''; row = [];
    } else {
      field += ch;
    }
  }
  if (field !== '' || row.length) { row.push(field); rows.push(row); }
  return rows;
}

export function csvRowsToObjects(rows) {
  if (!Array.isArray(rows) || rows.length < 2) return [];
  const header = rows[0];
  return rows.slice(1)
    .filter(cells => cells.length > 1 || (cells[0] ?? '') !== '') // 말미 빈 줄 제거
    .map(cells => Object.fromEntries(header.map((name, idx) => [name, cells[idx] ?? ''])));
}

async function fetchStrategyCsv(url) {
  try {
    const res = await fetch(url);
    if (!res.ok) return null;
    return csvRowsToObjects(parseCsv(await res.text()));
  } catch (e) {
    return null; // 실패한 파일만 비우고 예외는 전파하지 않는다
  }
}

function ensureStrategyLoad() {
  if (!strategyLoadPromise) {
    strategyLoadPromise = Promise.all([
      fetchStrategyCsv(STRATEGY_DAILY_CSV_URL),
      fetchStrategyCsv(STRATEGY_ANNUAL_CSV_URL),
      fetchStrategyCsv(STRATEGY_POOLED_CSV_URL),
    ]).then(([daily, annual, pooled]) => {
      strategyData = { daily, annual, pooled };
      return strategyData;
    });
  }
  return strategyLoadPromise;
}

// --- 일별 시계열·요약 통계 ---
function getDailySeries() {
  if (dailySeriesCache) return dailySeriesCache;
  const points = [];
  (strategyData?.daily || []).forEach(row => {
    const equity = toFiniteNumber(row.equity);
    if (!row.date || equity == null || equity <= 0) return;
    const kospi = toFiniteNumber(row.kospiValue);
    points.push({
      date: row.date,
      equity,
      kospi: kospi != null && kospi > 0 ? kospi : null,
      cumulativeDividends: toFiniteNumber(row.cumulativeDividends),
    });
  });
  dailySeriesCache = points;
  return points;
}

function getDailySpanYears(points) {
  const firstMs = getHistoryDateMs(points[0]?.date);
  const lastMs = getHistoryDateMs(points[points.length - 1]?.date);
  if (firstMs == null || lastMs == null) return null;
  return (lastMs - firstMs) / YEAR_MS;
}

function buildSeriesSummary(points, getValue, spanYears) {
  const values = [];
  points.forEach(point => {
    const value = getValue(point);
    if (value == null || getHistoryDateMs(point.date) == null) return;
    values.push({ value });
  });
  if (values.length < 2) return null;

  const first = values[0];
  const last = values[values.length - 1];
  // 연수는 백테스트 리포트와 동일하게 전체 일별 구간(신호일~마지막일) 기준 — KOSPI가 하루 늦게 시작해도 같은 분모 사용
  const cagr = spanYears > 0 && first.value > 0
    ? Math.pow(last.value / first.value, 1 / spanYears) - 1
    : null;
  let peak = -Infinity;
  let mdd = 0; // 일별 running max 대비 최저 낙폭 (0 이하 fraction)
  values.forEach(({ value }) => {
    if (value > peak) peak = value;
    else if (peak > 0) mdd = Math.min(mdd, value / peak - 1);
  });
  return { cagr, mdd, finalValue: last.value };
}

// --- 포맷 헬퍼 (YoY 색상은 views 소유 헬퍼 대신 로컬 구현) ---
function getSignColorClass(value) {
  if (value == null || Number.isNaN(value)) return 'flat-color';
  if (value > 0) return 'up-color';
  if (value < 0) return 'down-color';
  return 'flat-color';
}

function formatFractionPercent(fraction, { signed = false } = {}) {
  if (fraction == null || Number.isNaN(fraction)) return '-';
  const pct = fraction * 100;
  return `${signed && pct > 0 ? '+' : ''}${pct.toFixed(2)}%`;
}

function formatWon(value) {
  if (value == null || Number.isNaN(value)) return '-';
  return `${Math.round(value).toLocaleString('ko-KR')}원`;
}

function formatKrwAxisLabel(value) {
  if (value >= 1e8) {
    const eok = value / 1e8;
    return `${Number.isInteger(eok) ? eok.toLocaleString('ko-KR') : eok.toFixed(1)}억`;
  }
  if (value >= 1e7) {
    const cheonman = value / 1e7;
    return `${Number.isInteger(cheonman) ? String(cheonman) : cheonman.toFixed(1)}천만`;
  }
  if (value >= 1e4) return `${Math.round(value / 1e4).toLocaleString('ko-KR')}만`;
  return Math.round(value).toLocaleString('ko-KR');
}

function formatEventP(p) {
  if (p == null || Number.isNaN(p)) return '-';
  return p < 0.0005 ? '<0.001' : p.toFixed(3);
}

// --- 요약 stat 박스 ---
function renderStrategyStats() {
  const statsEl = document.getElementById('strategyStats');
  if (!statsEl) return;
  const points = getDailySeries();
  if (points.length < 2) {
    statsEl.innerHTML = LOAD_ERROR_HTML;
    return;
  }

  const spanYears = getDailySpanYears(points);
  const strategySummary = buildSeriesSummary(points, point => point.equity, spanYears);
  const kospiSummary = buildSeriesSummary(points, point => point.kospi, spanYears);
  let cumulativeDividends = null;
  for (let i = points.length - 1; i >= 0; i--) {
    if (points[i].cumulativeDividends != null) { cumulativeDividends = points[i].cumulativeDividends; break; }
  }

  const percentStat = (fraction, colored = true) => fraction == null
    ? '-'
    : `<span class="${colored ? getSignColorClass(fraction) : ''}">${formatFractionPercent(fraction)}</span>`;
  const stats = [
    { label: '전략 CAGR', value: percentStat(strategySummary?.cagr) },
    { label: 'KOSPI CAGR', value: percentStat(kospiSummary?.cagr) },
    { label: '전략 MDD', value: percentStat(strategySummary?.mdd) },
    { label: 'KOSPI MDD', value: percentStat(kospiSummary?.mdd) },
    {
      label: '최종 평가액 <span class="stat-label-normal">(초기 1,000만원)</span>',
      value: `<div class="stat-combo">
        <div class="stat-combo-row"><span class="stat-combo-label">전략</span><span class="stat-combo-value">${formatWon(strategySummary?.finalValue)}</span></div>
        <div class="stat-combo-row"><span class="stat-combo-label">KOSPI</span><span class="stat-combo-value">${formatWon(kospiSummary?.finalValue)}</span></div>
      </div>`,
    },
  ];
  if (cumulativeDividends != null) {
    stats.push({ label: '누적 배당 수취', value: formatWon(cumulativeDividends) });
  }
  statsEl.innerHTML = stats.map(item => `<div class="stat-box"><div class="label">${item.label}</div><div class="value">${item.value}</div></div>`).join('');
}

// --- 에쿼티 커브 차트 (y축 로그 스케일: 값 범위가 수백만~십억 원이라 선형으로는 초반 구간이 보이지 않음) ---
function buildLogAxisTicks(minValue, maxValue) {
  const ticks = [];
  const minExp = Math.floor(Math.log10(minValue));
  const maxExp = Math.ceil(Math.log10(maxValue));
  // 기본은 10^n 눈금(1천만/1억/10억), 값 범위가 좁으면 2×/5× 보조 눈금 추가
  const multipliers = Math.log10(maxValue / minValue) >= 2.5 ? [1] : [1, 2, 5];
  for (let exp = minExp; exp <= maxExp; exp++) {
    multipliers.forEach(multiplier => {
      const value = multiplier * Math.pow(10, exp);
      if (value >= minValue && value <= maxValue) ticks.push(value);
    });
  }
  return ticks.sort((a, b) => a - b);
}

function drawLogSeries(ctx, points, scale, series, yFor, color) {
  const segments = getContinuousSegments(points, scale, series);
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.5;
  segments.forEach(([start, end]) => {
    if (end <= start) return;
    ctx.beginPath();
    let started = false;
    for (let i = start; i <= end; i++) {
      const value = series[i];
      if (value == null || Number.isNaN(value)) continue;
      const x = scale.xPositions[i];
      const y = yFor(value);
      if (!started) { ctx.moveTo(x, y); started = true; }
      else ctx.lineTo(x, y);
    }
    ctx.stroke();
  });
}

function renderStrategyChart() {
  const canvas = document.getElementById('strategyChart');
  if (!canvas) return;
  const container = canvas.parentElement;
  const points = getDailySeries();
  if (points.length < 2) {
    container.innerHTML = LOAD_ERROR_HTML;
    return;
  }

  const theme = getChartTheme();
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
  const pad = { top: 18, right: 16, bottom: 32, left: 56 };
  const cW = W - pad.left - pad.right;
  const cH = H - pad.top - pad.bottom;
  ctx.clearRect(0, 0, W, H);

  let minV = Infinity;
  let maxV = -Infinity;
  points.forEach(point => {
    minV = Math.min(minV, point.equity, point.kospi ?? Infinity);
    maxV = Math.max(maxV, point.equity, point.kospi ?? -Infinity);
  });
  if (!(minV > 0) || !(maxV > minV)) return;

  const logMin = Math.log10(minV);
  const logRange = (Math.log10(maxV) - logMin) || 1;
  const yFor = value => pad.top + cH - ((Math.log10(value) - logMin) / logRange * cH);
  const scale = buildChartScale(points, pad, cW);

  // Y축: 로그 눈금 + 천만/억 단위 한국어 라벨
  ctx.lineWidth = 1;
  ctx.font = '11px -apple-system, sans-serif';
  buildLogAxisTicks(minV, maxV).forEach(tick => {
    const y = yFor(tick);
    ctx.strokeStyle = theme.grid;
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(W - pad.right, y);
    ctx.stroke();
    ctx.fillStyle = theme.textDim;
    ctx.textAlign = 'right';
    ctx.fillText(formatKrwAxisLabel(tick), pad.left - 8, y + 4);
  });

  drawXAxis(ctx, pad, cH, cW, H, points, scale);

  drawLogSeries(ctx, points, scale, points.map(point => point.kospi), yFor, theme.green);
  drawLogSeries(ctx, points, scale, points.map(point => point.equity), yFor, theme.accent);

  // 범례 (초반 값이 차트 하단에 깔리므로 좌상단이 안전)
  ctx.font = '11px -apple-system, sans-serif';
  ctx.textAlign = 'left';
  ctx.fillStyle = theme.accent;
  ctx.fillText('● 전략', pad.left + 6, pad.top + 12);
  ctx.fillStyle = theme.green;
  ctx.fillText('● KOSPI', pad.left + 52, pad.top + 12);

  // 툴팁 (renderChart 패턴과 동일한 마우스/터치 핸들링)
  const tooltip = document.getElementById('strategyTooltip');
  const handleMove = (clientX, clientY) => {
    const r = canvas.getBoundingClientRect();
    const mx = clientX - r.left;
    const my = clientY - r.top;
    if (mx < pad.left || mx > W - pad.right || my < pad.top || my > pad.top + cH) {
      tooltip.classList.remove('visible');
      return;
    }
    const idx = findNearestIndexByX(scale.xPositions, mx);
    const d = points[idx];
    if (!d) return;
    const dotX = scale.xPositions[idx];
    if (Math.abs(dotX - mx) > 24) {
      tooltip.classList.remove('visible');
      return;
    }

    tooltip.innerHTML = `
      <div class="tt-date">${escapeHtml(d.date)}</div>
      <div class="tt-row"><span>전략</span><span>${formatWon(d.equity)}</span></div>
      <div class="tt-row"><span>KOSPI</span><span>${d.kospi != null ? formatWon(d.kospi) : '-'}</span></div>
    `;
    const dotY = yFor(d.equity);
    let tx = dotX + 12;
    let ty = dotY - 60;
    if (tx + 200 > W) tx = dotX - 200;
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

// --- 연도별 성과 테이블 ---
function buildAnnualTableHtml() {
  const rows = strategyData?.annual;
  if (!rows || !rows.length) return LOAD_ERROR_HTML;
  const body = rows.map(row => {
    const strategyYoY = toFiniteNumber(row.strategyYoY);
    const kospiYoY = toFiniteNumber(row.kospiYoY);
    const excessYoY = toFiniteNumber(row.excessYoY);
    return `<tr>
      <td>${escapeHtml(row.year)}</td>
      <td>${Math.round(toFiniteNumber(row.strategyValue) ?? 0).toLocaleString('ko-KR')}</td>
      <td class="${getSignColorClass(strategyYoY)}">${formatFractionPercent(strategyYoY, { signed: true })}</td>
      <td class="${getSignColorClass(kospiYoY)}">${formatFractionPercent(kospiYoY, { signed: true })}</td>
      <td class="${getSignColorClass(excessYoY)}">${formatFractionPercent(excessYoY, { signed: true })}</td>
    </tr>`;
  }).join('');
  return `<div class="index-weight-table-wrap strategy-table-wrap">
    <table class="index-weight-table strategy-table" id="strategyAnnualTable">
      <thead>
        <tr><th>연도</th><th>전략 평가액(원)</th><th>전략 YoY</th><th>KOSPI YoY</th><th>초과수익</th></tr>
      </thead>
      <tbody>${body}</tbody>
    </table>
  </div>`;
}

// --- 이벤트 스터디 풀드 요약 테이블 ---
function buildEventStudyTableHtml() {
  const rows = strategyData?.pooled;
  if (!rows || !rows.length) return LOAD_ERROR_HTML;
  const body = EVENT_STUDY_HYPOTHESES.map(meta => {
    const row = rows.find(item => (
      item.hypothesis === meta.id
      && item.lookback === EVENT_STUDY_LOOKBACK
      && item.horizon === EVENT_STUDY_HORIZON
    ));
    if (!row) return '';
    const events = toFiniteNumber(row.events);
    const mean = toFiniteNumber(row[`${meta.metric}Mean`]);
    const ciLow = toFiniteNumber(row[`${meta.metric}CiLow`]);
    const ciHigh = toFiniteNumber(row[`${meta.metric}CiHigh`]);
    const p = toFiniteNumber(row[`${meta.metric}P`]);
    const ciText = ciLow != null && ciHigh != null
      ? `${(ciLow * 100).toFixed(2)}% ~ ${(ciHigh * 100).toFixed(2)}%`
      : '-';
    return `<tr>
      <td>
        <div class="index-weight-name">${meta.id}</div>
        <div class="index-weight-sub">${meta.desc}</div>
      </td>
      <td>${events != null ? events.toLocaleString('ko-KR') : '-'}</td>
      <td class="${getSignColorClass(mean)}">${formatFractionPercent(mean, { signed: true })}</td>
      <td>${ciText}</td>
      <td>${formatEventP(p)}</td>
    </tr>`;
  }).join('');
  if (!body) return LOAD_ERROR_HTML;
  return `<div class="index-weight-table-wrap strategy-table-wrap">
    <table class="index-weight-table strategy-table" id="strategyEventTable">
      <thead>
        <tr><th>가설</th><th>이벤트 수</th><th>20일 평균 수익률</th><th>95% CI</th><th>p (단측)</th></tr>
      </thead>
      <tbody>${body}</tbody>
    </table>
  </div>`;
}

function renderStrategyTables() {
  const tablesEl = document.getElementById('strategyTables');
  if (!tablesEl) return;
  tablesEl.innerHTML = `
    <div class="strategy-table-block">
      <div class="strategy-table-title">연도별 성과</div>
      ${buildAnnualTableHtml()}
    </div>
    <div class="strategy-table-block">
      <div class="strategy-table-title">이벤트 스터디 풀드 요약 (5일 신호 · 20일 성과)</div>
      ${buildEventStudyTableHtml()}
      <div class="strategy-table-note">단측검정·탐색적 분석이며 인과 추정이 아님 (상세: <a href="analysis/hypothesis_event_study_report.md" target="_blank" rel="noopener">analysis/ 리포트</a>)</div>
    </div>
  `;
}

function renderStrategyNote() {
  const noteEl = document.getElementById('strategyNote');
  if (!noteEl) return;
  noteEl.innerHTML = '백테스트 가정: 신호 다음 거래일 종가 체결 · 매수/매도 수수료 각 1% · 정수 주식 수량 · 우선주 현금배당은 예수금 적립 후 재투자 · 5거래일 연속 괴리율 상위 3위 진입 시 교체 매매 · KOSPI 비교는 동일 시점 1천만원 일시투자(price-only). '
    + '과거 데이터 기반 시뮬레이션 결과이며 미래 수익을 보장하지 않습니다. '
    + '<a href="analysis/top3_spread_strategy_report.md" target="_blank" rel="noopener">상세 리포트</a>';
}

function renderStrategyContent() {
  strategyContentRendered = true;
  renderStrategyStats();
  renderStrategyChart();
  renderStrategyTables();
}

// --- 진입점 ---
export function renderStrategySection() {
  if (strategyCollapsed) return; // 접힘 상태에서는 CSV 로드/렌더 모두 생략 (펼칠 때 보충)
  const statsEl = document.getElementById('strategyStats');
  if (!statsEl) return;

  if (strategyData) {
    if (strategyContentRendered) renderStrategyChart(); // resize 등 재호출: 차트만 다시 그림
    else renderStrategyContent();
    return;
  }
  if (!strategyLoadPromise) {
    statsEl.innerHTML = '<div class="strategy-loading">백테스트 데이터를 불러오는 중...</div>';
    ensureStrategyLoad()
      .then(() => {
        if (!strategyCollapsed) renderStrategyContent();
      })
      .catch(e => console.error('전략 섹션 렌더 실패', e)); // 예외 전파 금지
  }
  // 로드 중이면 완료 콜백이 렌더한다.
}

function applyStrategyCollapsedState() {
  const body = document.getElementById('strategyBody');
  const toggleBtn = document.getElementById('strategyToggleBtn');
  if (body) body.style.display = strategyCollapsed ? 'none' : '';
  if (toggleBtn) {
    toggleBtn.textContent = strategyCollapsed ? '펼치기' : '접기';
    toggleBtn.setAttribute('aria-expanded', String(!strategyCollapsed));
  }
}

export function bindStrategyControls() {
  if (bindStrategyControls._bound) return;
  const toggleBtn = document.getElementById('strategyToggleBtn');
  if (!toggleBtn) return;

  toggleBtn.addEventListener('click', () => {
    strategyCollapsed = !strategyCollapsed;
    writeStoredValue(STRATEGY_COLLAPSED_STORAGE_KEY, strategyCollapsed ? '1' : '0');
    applyStrategyCollapsedState();
    if (!strategyCollapsed) renderStrategySection(); // 접힌 동안 생략한 로드/렌더 보충
  });

  renderStrategyNote();
  applyStrategyCollapsedState();
  bindStrategyControls._bound = true;
}
