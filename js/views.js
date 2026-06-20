// js/views.js — 오버뷰/카드/테이블/통계/모달 렌더 + selectPair + 각종 bind + 테마 적용 + CSV 내보내기
import {
  app,
  CARD_SORT_CONFIG,
  CARD_SORT_STORAGE_KEY,
  PERIOD_DAYS_OPTIONS,
  PERIOD_DAYS_STORAGE_KEY,
  SELECTED_CODE_QUERY_KEY,
  TABLE_HEADER_CONFIG,
  TABLE_SORT_DEFAULT_DIRECTION,
  THEME_STORAGE_KEY,
  ensureDividends,
  ensureHistory,
  getAveragePair,
  getCardGroups,
  getGroupItemsByPairId,
  getRepresentativePairs,
  isGroupPinned,
  isHistoryLoaded,
  readStoredValue,
  savePinnedPairIds,
  writeStoredValue,
} from './state.js';
import {
  buildHistoryCsv,
  escapeHtml,
  fmtChange,
  formatCompactMetricValue,
  formatIndexSpread,
  formatIndexWeightPercent,
  formatKstTimestamp,
  formatMarketCap,
  formatMarketPrice,
  formatPairName,
  formatPercentChange,
  formatPointChange,
  formatPrice,
  formatRatioPercent,
  formatSignedPoints,
  formatStatPrice,
  formatTradedValue,
  formatYield,
  getDirectionClass,
  getHistoryDateMs,
  getTextColorClass,
  getTickerCode,
  normalizeTickerCode,
  parseDirectionalNumber,
  toFiniteNumber,
} from './format.js';
import {
  calculateLatestEma,
  calculateLiveMarketCap,
  calculateMeanStd,
  calculatePairBeta,
  calculatePairCorrelation,
  calculatePercentileRank,
  calculatePreferredRatio,
  calculateSimpleAverageMetrics,
  calculateSma,
  calculateSqrtPreferredMarketCapSpreadIndex,
} from './calc.js';
import {
  buildTodaySummaryFromPairs,
  getFutureSessionKind,
  getRenderableMarketSummary,
  mergeMarketExtras,
  mergeNightFutureMetric,
} from './market.js';
import {
  getDetailLabels,
  getPreferredShortLabel,
  renderPreferredInlineLabel,
  renderPreferredTermLabel,
  renderPreferredTermSummary,
  renderPreferredYieldLabel,
} from './labels.js';
import {
  getFilteredHistory,
  renderChart,
  renderPriceChart,
  renderZoomPanel,
  resetZoomWindow,
} from './charts.js';

export function queueDividendRender() {
  // 배당 데이터 최초 로드가 끝나는 시점에만 1회 재렌더한다 (이미 로드됐으면 일반 렌더 경로에 포함).
  if (app.dividendHistories || app.dividendRenderQueued) return;
  app.dividendRenderQueued = true;
  ensureDividends().then(() => {
    renderPriceChart();
    renderStats();
  });
}

export function togglePinnedGroup(idx) {
  const pair = app.pairs[idx];
  if (!pair || pair.isAverage) return;
  const items = getGroupItemsByPairId(pair.id) || [{ pair, idx }];
  if (isGroupPinned(items)) {
    items.forEach(item => app.pinnedPairIds.delete(item.pair.id));
  } else {
    app.pinnedPairIds.add(pair.id);
  }
  savePinnedPairIds();
  renderCards();
}

export function restoreStoredSettings() {
  const storedSortMode = readStoredValue(CARD_SORT_STORAGE_KEY);
  if (storedSortMode && CARD_SORT_CONFIG[storedSortMode]) {
    app.cardSortMode = storedSortMode;
    document.querySelectorAll('#cardSortOptions button').forEach(button => {
      button.classList.toggle('active', button.dataset.cardSort === app.cardSortMode);
    });
  }
  const storedPeriodText = readStoredValue(PERIOD_DAYS_STORAGE_KEY);
  const storedPeriodDays = storedPeriodText == null ? null : Number(storedPeriodText);
  if (storedPeriodDays != null && PERIOD_DAYS_OPTIONS.includes(storedPeriodDays)) {
    app.periodDays = storedPeriodDays;
    document.querySelectorAll('#periodBtns button').forEach(button => {
      button.classList.toggle('active', parseInt(button.dataset.days, 10) === app.periodDays);
    });
  }
}

export function fitTextToWidth(el, { maxFontSize, minFontSize }) {
  if (!el) return;

  el.style.fontSize = `${maxFontSize}px`;
  el.classList.remove('autofit-wrap');

  if (!el.clientWidth) return;
  while (parseFloat(el.style.fontSize) > minFontSize && el.scrollWidth > el.clientWidth + 1) {
    el.style.fontSize = `${(parseFloat(el.style.fontSize) - 0.5).toFixed(1)}px`;
  }

  if (el.scrollWidth > el.clientWidth + 1) {
    el.classList.add('autofit-wrap');
  }
}

export function applyAutoFitText(root = document) {
  const configs = [
    { selector: '.overview-card.leader-card .name', maxFontSize: 16, minFontSize: 12 },
    { selector: '.card .name, .overview-card:not(.leader-card) .name', maxFontSize: 14, minFontSize: 11 },
    { selector: '.overview-rank-name', maxFontSize: 11.5, minFontSize: 9.5 },
    { selector: '.price-label', maxFontSize: 12, minFontSize: 9.5 },
  ];

  configs.forEach(config => {
    root.querySelectorAll(config.selector).forEach(el => fitTextToWidth(el, config));
  });
}

export function updateThemeButtonLabel() {
  const themeBtn = document.getElementById('themeToggle');
  if (!themeBtn) return;
  themeBtn.setAttribute('aria-label', app.currentTheme === 'dark' ? '일반 모드로 전환' : '다크 모드로 전환');
  themeBtn.title = '테마 전환';
}

export function applyTheme(theme, { persist = true, rerender = true } = {}) {
  app.currentTheme = theme === 'light' ? 'light' : 'dark';
  document.documentElement.dataset.theme = app.currentTheme;
  updateThemeButtonLabel();

  if (persist) {
    try {
      localStorage.setItem(THEME_STORAGE_KEY, app.currentTheme);
    } catch (e) {
      // 저장 실패 시 현재 세션 테마만 적용한다.
    }
  }

  if (rerender) {
    renderZoomPanel();
    renderChart();
    renderPriceChart();
  }
}

export function bindThemeButton() {
  const themeBtn = document.getElementById('themeToggle');
  if (!themeBtn) return;
  themeBtn.addEventListener('click', toggleTheme);
  updateThemeButtonLabel();
}

export function toggleTheme() {
  applyTheme(app.currentTheme === 'dark' ? 'light' : 'dark');
}

export function getPreferredShareCodeByPairId(pairId) {
  const config = app.pairConfigMap.get(pairId);
  if (!config) return '';
  return normalizeTickerCode(getTickerCode(config.preferredTicker || config.commonTicker));
}

export function updateSelectedPairQueryParam() {
  const pair = app.pairs[app.selectedIdx];
  if (!pair || pair.isAverage) return;
  const code = getPreferredShareCodeByPairId(pair.id);
  if (!code) return;

  const url = new URL(window.location.href);
  if (url.searchParams.get(SELECTED_CODE_QUERY_KEY) === code) return;
  url.searchParams.set(SELECTED_CODE_QUERY_KEY, code);
  window.history.replaceState(null, '', url.toString());
}

export function resolveSelectedPairIndexFromQuery() {
  const url = new URL(window.location.href);
  const code = normalizeTickerCode(url.searchParams.get(SELECTED_CODE_QUERY_KEY));
  if (!code) return { idx: 0, requested: false, matched: false };

  let commonMatchIdx = -1;
  for (let idx = 0; idx < app.pairs.length; idx++) {
    const pair = app.pairs[idx];
    if (!pair || pair.isAverage) continue;
    const config = app.pairConfigMap.get(pair.id);
    if (!config) continue;

    const preferredCode = normalizeTickerCode(getTickerCode(config.preferredTicker));
    if (preferredCode && preferredCode === code) {
      return { idx, requested: true, matched: true };
    }

    const commonCode = normalizeTickerCode(getTickerCode(config.commonTicker));
    if (commonMatchIdx < 0 && commonCode && commonCode === code) {
      commonMatchIdx = idx;
    }
  }

  return {
    idx: commonMatchIdx >= 0 ? commonMatchIdx : 0,
    requested: true,
    matched: commonMatchIdx >= 0,
  };
}

export function scrollToSelectedSpreadSection() {
  const section = document.getElementById('spreadChartSection');
  if (!section) return;
  requestAnimationFrame(() => {
    section.scrollIntoView({ behavior: 'smooth', block: 'start' });
  });
}

export function selectPair(idx, { updateUrl = true, scrollToChart = false } = {}) {
  if (Number.isNaN(idx) || idx < 0) return;
  app.selectedIdx = idx;
  resetZoomWindow({ render: false });
  if (updateUrl) {
    updateSelectedPairQueryParam();
  }
  renderTodayOverview();
  renderCards();
  queueDividendRender();
  const renderDetail = () => {
    if (app.selectedIdx !== idx) return; // 로드 중 다른 종목이 선택된 경우
    renderZoomPanel();
    renderChart();
    renderPriceChart();
    renderStats();
    if (scrollToChart) {
      scrollToSelectedSpreadSection();
    }
  };
  const pair = app.pairs[idx];
  if (isHistoryLoaded(pair)) {
    renderDetail();
  } else {
    // 미로드 상태에서도 current 기반 항목은 먼저 표시하고, 로드 후 차트를 채운다.
    renderZoomPanel();
    renderChart();
    renderPriceChart();
    renderStats();
    ensureHistory(pair).then(renderDetail);
  }
}

export function renderTodayOverview() {
  const summary = app.todayOverviewData || buildTodaySummaryFromPairs();
  const overviewEl = document.getElementById('todayOverview');
  if (!summary || summary.averageSpread == null) {
    overviewEl.innerHTML = '';
    return;
  }
  const representativeCount = summary.representativeCount || getRepresentativePairs().length;

  overviewEl.innerHTML = `
    <div class="today-overview-header">
      <div>
        <div class="today-overview-kicker">오늘의 우선주 현황</div>
        <div class="today-overview-title">실시간 괴리율 스냅샷</div>
      </div>
    </div>
    <div class="today-overview-grid">
      ${renderAverageOverviewCard(summary, representativeCount)}
      ${renderMarketOverviewCard(summary.market)}
      ${renderLeaderOverviewCard('최고 괴리율 확대', summary.topWidening, summary.topWideningRunners || [], '전 종목이 괴리율 축소 또는 보합입니다.')}
      ${renderLeaderOverviewCard('최고 괴리율 축소', summary.topNarrowing, summary.topNarrowingRunners || [], '전 종목이 괴리율 확대 또는 보합입니다.')}
    </div>
  `;

  overviewEl.querySelectorAll('.overview-card.clickable[data-idx]').forEach(card => {
    card.addEventListener('click', () => {
      selectPair(parseInt(card.dataset.idx, 10));
    });
  });
  overviewEl.querySelectorAll('[data-index-card="1"]').forEach(card => {
    card.addEventListener('click', openIndexWeightModal);
    card.addEventListener('keydown', event => {
      if (event.key !== 'Enter' && event.key !== ' ') return;
      event.preventDefault();
      openIndexWeightModal();
    });
  });
  overviewEl.querySelectorAll('.overview-rank-item[data-idx]').forEach(item => {
    item.addEventListener('click', event => {
      event.stopPropagation();
      selectPair(parseInt(item.dataset.idx, 10));
    });
  });
  applyAutoFitText(overviewEl);
}

export function renderMarketOverviewCard(market) {
  const mergedMarket = getRenderableMarketSummary(market);
  const extras = mergeMarketExtras(mergedMarket?.extras || [], app.latestMarketExtras);
  const futureMetric = mergeNightFutureMetric(
    mergedMarket?.nightFuture || null,
    app.latestNightFutureMetric,
  );
  const kosdaqMetric = extras.find(metric => metric.id === 'KOSDAQ') || null;
  const sideMetrics = extras.filter(metric => metric.id !== 'KOSDAQ');
  const secondaryMetric = kosdaqMetric || futureMetric;
  if (futureMetric && secondaryMetric !== futureMetric) {
    sideMetrics.unshift(futureMetric);
  }

  return `
    <div class="overview-card primary">
      <div class="overview-split market">
        <div class="overview-main-copy">
          <div class="overview-label">KOSPI 지수</div>
          <div class="overview-value overview-market-line">${formatMarketPrice(mergedMarket?.price)}</div>
          <div class="overview-change ${getDirectionClass(mergedMarket?.changePct)}">${formatPercentChange(mergedMarket?.changePct)}</div>
          <div class="overview-points">${formatSignedPoints(mergedMarket?.change, 'pt')}</div>
          ${renderMarketSecondaryMetric(secondaryMetric)}
        </div>
        <div class="overview-side-list">
          ${sideMetrics.map(renderMarketExtraMetric).join('')}
        </div>
      </div>
    </div>
  `;
}

export function renderAverageOverviewCard(summary, representativeCount) {
  const liveSimpleAverage = calculateSimpleAverageMetrics();
  const simpleAverageSpread = toFiniteNumber(summary.simpleAverageSpread) ?? liveSimpleAverage.spread;
  const averageSpreadVsSimple = toFiniteNumber(summary.averageSpreadVsSimple)
    ?? (simpleAverageSpread == null ? null : summary.averageSpread - simpleAverageSpread);

  return `
    <div class="overview-card average-card" data-index-card="1" role="button" tabindex="0" aria-haspopup="dialog" aria-controls="indexWeightModal">
      <div class="overview-label">괴리율 지수</div>
      <div class="average-card-body">
        <div class="average-main-metric">
          <div class="overview-value">${summary.averageSpread.toFixed(2)}%</div>
          <div class="overview-change ${getDirectionClass(summary.averageSpreadChange)}">전일비 ${formatPointChange(summary.averageSpreadChange)}</div>
        </div>
        ${renderAverageSpreadSparkline(summary.averageSpread)}
      </div>
      <div class="overview-summary-grid">
        ${renderOverviewStat('우선주 평균 상승률', formatPercentChange(summary.averagePreferredChange))}
        ${renderOverviewStat('보통주 평균 상승률', formatPercentChange(summary.averageCommonChange))}
        ${renderOverviewStat('단순 평균', simpleAverageSpread == null ? '-' : `${simpleAverageSpread.toFixed(2)}%`, { directional: false })}
        ${renderOverviewStat('지수-단순', formatPointChange(averageSpreadVsSimple))}
      </div>
      <div class="overview-detail average-detail-row">
        <span>${summary.averageMethod || '제곱근 총액가중'} · ${representativeCount}개 그룹</span>
        <span class="overview-detail-action">비중</span>
      </div>
    </div>
  `;
}

export function getAverageSpreadTrend(currentAverageSpread = null) {
  const averagePair = getAveragePair();
  const entries = (averagePair?.history || [])
    .filter(entry => entry?.date && entry.spread != null && !Number.isNaN(Number(entry.spread)))
    .map(entry => ({ date: entry.date, spread: Number(entry.spread) }));

  if (!entries.length) return [];

  const latestMs = getHistoryDateMs(entries[entries.length - 1].date);
  const cutoffMs = latestMs == null ? null : latestMs - (30 * 24 * 60 * 60 * 1000);
  let recent = cutoffMs == null
    ? entries.slice(-30)
    : entries.filter(entry => {
        const entryMs = getHistoryDateMs(entry.date);
        return entryMs != null && entryMs >= cutoffMs;
      });
  if (recent.length < 2) recent = entries.slice(-30);

  if (currentAverageSpread != null && !Number.isNaN(Number(currentAverageSpread)) && recent.length) {
    recent = recent.slice();
    recent[recent.length - 1] = {
      ...recent[recent.length - 1],
      spread: Number(currentAverageSpread),
    };
  }

  return recent;
}

export function renderAverageSpreadSparkline(currentAverageSpread) {
  const trend = getAverageSpreadTrend(currentAverageSpread);
  if (trend.length < 2) {
    return `
      <div class="average-sparkline empty">
        <div class="average-sparkline-label">최근 1개월</div>
        <div>추이 없음</div>
      </div>
    `;
  }

  const width = 128;
  const height = 54;
  const pad = 4;
  const values = trend.map(point => point.spread);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const points = trend.map((point, index) => {
    const x = pad + (index / (trend.length - 1)) * (width - pad * 2);
    const y = height - pad - ((point.spread - min) / range) * (height - pad * 2);
    return [x.toFixed(1), y.toFixed(1)];
  });
  const linePath = points.map((point, index) => `${index === 0 ? 'M' : 'L'} ${point[0]} ${point[1]}`).join(' ');
  const fillPath = `${linePath} L ${points[points.length - 1][0]} ${height - pad} L ${points[0][0]} ${height - pad} Z`;
  const directionClass = getDirectionClass(values[values.length - 1] - values[0]);

  return `
    <div class="average-sparkline ${directionClass}">
      <div class="average-sparkline-label">최근 1개월</div>
      <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="최근 한달간 평균 괴리율 추이">
        <path class="average-sparkline-fill" d="${fillPath}"></path>
        <path class="average-sparkline-line" d="${linePath}"></path>
      </svg>
    </div>
  `;
}

export function renderOverviewStat(label, value, { directional = true } = {}) {
  const valueClass = directional ? getTextColorClass(parseDirectionalNumber(value)) : 'neutral-color';
  return `
    <div class="overview-stat">
      <div class="overview-stat-label">${label}</div>
      <div class="overview-stat-value ${valueClass}">${value}</div>
    </div>
  `;
}

export function renderIndexModalMetric(label, value, colorClass = '') {
  return `
    <div class="index-modal-metric">
      <div class="index-modal-metric-label">${label}</div>
      <div class="index-modal-metric-value ${colorClass}">${value}</div>
    </div>
  `;
}

export function getIndexWeightPartLabel(parts) {
  if (!Array.isArray(parts) || !parts.length) return '';
  return parts
    .map(part => {
      const name = escapeHtml(part.preferredName || part.name || '');
      const share = parts.length > 1 ? ` ${formatIndexWeightPercent(part.issuerShare)}` : '';
      return `${name}${share}`;
    })
    .join(' · ');
}

export function renderIndexWeightModalContent() {
  const index = calculateSqrtPreferredMarketCapSpreadIndex();
  const simpleAverage = calculateSimpleAverageMetrics();
  const summaryEl = document.getElementById('indexWeightSummary');
  const noteEl = document.getElementById('indexWeightNote');
  const tableBody = document.getElementById('indexWeightTableBody');
  if (!summaryEl || !noteEl || !tableBody) return;

  if (!index || !Array.isArray(index.constituents) || !index.constituents.length) {
    summaryEl.innerHTML = '';
    noteEl.textContent = '';
    tableBody.innerHTML = '<tr><td colspan="5" class="index-weight-empty">비중을 계산할 수 없습니다.</td></tr>';
    return;
  }

  const indexMinusSimple = simpleAverage.spread == null ? null : index.spread - simpleAverage.spread;
  summaryEl.innerHTML = [
    renderIndexModalMetric('괴리율 지수', formatIndexSpread(index.spread)),
    renderIndexModalMetric('단순 평균', formatIndexSpread(simpleAverage.spread), 'neutral-color'),
    renderIndexModalMetric('지수-단순', formatPointChange(indexMinusSimple), getTextColorClass(indexMinusSimple)),
    renderIndexModalMetric('구성 그룹', `${index.issuerCount}개`, 'neutral-color'),
  ].join('');
  noteEl.textContent = '비중은 각 보통주 그룹의 우선주 시가총액 합계에 제곱근을 적용한 뒤 전체 합계로 나눈 값입니다.';

  const maxWeight = Math.max(...index.constituents.map(item => item.weight || 0), 1);
  tableBody.innerHTML = index.constituents.map(item => {
    const barWidth = Math.max(2, Math.min(100, (item.weight || 0) / maxWeight * 100));
    return `
      <tr>
        <td>
          <div class="index-weight-name">${escapeHtml(item.name)}</div>
          <div class="index-weight-sub">${getIndexWeightPartLabel(item.parts)}</div>
        </td>
        <td class="index-weight-share">
          <div>${formatIndexWeightPercent(item.weight)}</div>
          <div class="index-weight-bar"><span style="width: ${barWidth.toFixed(1)}%"></span></div>
        </td>
        <td>${formatMarketCap(item.totalMarketCap)}</td>
        <td>${formatIndexSpread(item.spread)}</td>
        <td class="${getTextColorClass(item.spreadChange)}">${formatPointChange(item.spreadChange)}</td>
      </tr>
    `;
  }).join('');
}

export function openIndexWeightModal(event) {
  if (event) event.stopPropagation();
  const modal = document.getElementById('indexWeightModal');
  const closeButton = document.getElementById('indexWeightModalClose');
  if (!modal) return;

  renderIndexWeightModalContent();
  app.indexWeightModalLastFocus = document.activeElement;
  modal.hidden = false;
  document.body.classList.add('modal-open');
  requestAnimationFrame(() => closeButton?.focus());
}

export function closeIndexWeightModal() {
  const modal = document.getElementById('indexWeightModal');
  if (!modal || modal.hidden) return;
  modal.hidden = true;
  document.body.classList.remove('modal-open');
  if (app.indexWeightModalLastFocus && typeof app.indexWeightModalLastFocus.focus === 'function') {
    app.indexWeightModalLastFocus.focus();
  }
  app.indexWeightModalLastFocus = null;
}

export function bindIndexWeightModal() {
  if (bindIndexWeightModal._bound) return;
  const modal = document.getElementById('indexWeightModal');
  const closeButton = document.getElementById('indexWeightModalClose');
  if (!modal || !closeButton) return;

  closeButton.addEventListener('click', closeIndexWeightModal);
  modal.addEventListener('click', event => {
    if (event.target === modal) closeIndexWeightModal();
  });
  document.addEventListener('keydown', event => {
    if (event.key === 'Escape') closeIndexWeightModal();
  });
  bindIndexWeightModal._bound = true;
}

export function getFutureSessionLabel(metric) {
  const sessionKind = getFutureSessionKind(metric);
  if (sessionKind === 'night') return '야간';
  if (sessionKind === 'day') return '주간';
  return null;
}

export function renderSessionBadge(label) {
  const sessionClass = label === '야간' ? 'night' : 'day';
  return `<span class="session-badge ${sessionClass}">${label}</span>`;
}

export function renderFutureTimeText(metric) {
  const timeText = String(metric?.time || '').trim();
  if (!timeText) return '';
  if (timeText === String(metric?.marketStatus || '').trim()) return '';
  return `<span class="session-time">${timeText}</span>`;
}

export function renderMarketExtraMetric(metric) {
  const sessionLabel = metric.id === 'KOSPI200_FUTURES' ? getFutureSessionLabel(metric) : null;
  const timeText = metric.id === 'KOSPI200_FUTURES' ? renderFutureTimeText(metric) : '';
  return `
    <div class="overview-mini-item">
      <div class="overview-mini-label">${metric.name}${sessionLabel ? renderSessionBadge(sessionLabel) : ''}${timeText}</div>
      <div class="overview-mini-data">
        <div class="overview-mini-value">${formatCompactMetricValue(metric)}</div>
        <div class="overview-mini-change ${getDirectionClass(metric.changePct)}">${formatPercentChange(metric.changePct)}</div>
      </div>
    </div>
  `;
}

export function renderMarketSecondaryMetric(metric) {
  if (!metric) return '';
  const label = metric.name || 'KOSPI200 선물';
  const sessionLabel = metric.id === 'KOSPI200_FUTURES' ? getFutureSessionLabel(metric) : null;
  const timeText = metric.id === 'KOSPI200_FUTURES' ? renderFutureTimeText(metric) : '';
  return `
    <div class="overview-market-secondary">
      <span class="overview-market-secondary-name">${label}${sessionLabel ? renderSessionBadge(sessionLabel) : ''}${timeText}</span>
      <span class="overview-market-secondary-value ${getDirectionClass(metric.changePct)}">${formatMarketPrice(metric.price)} ${formatPercentChange(metric.changePct)}</span>
    </div>
  `;
}

export function renderLeaderOverviewCard(title, leader, runners, emptyMessage) {
  if (!leader) {
    return `
      <div class="overview-card leader-card">
        <div class="overview-label">${title}</div>
        <div class="overview-value">없음</div>
        <div class="overview-detail">${emptyMessage}</div>
      </div>
    `;
  }

  const items = getGroupItemsByPairId(leader.id);
  if (!items || !items.length) {
    return `
      <div class="overview-card leader-card">
        <div class="overview-label">${title}</div>
        <div class="overview-value">${leader.name}</div>
        <div class="overview-change ${getDirectionClass(leader.spreadChange)}">${formatPointChange(leader.spreadChange)}</div>
        <div class="overview-detail">현재 괴리율 ${leader.spread.toFixed(2)}%</div>
      </div>
    `;
  }

  const primaryIdx = items[0].idx;
  const isActive = items.some(item => item.idx === app.selectedIdx);
  const hasRunners = Array.isArray(runners) && runners.length > 0;

  return `
    <div class="overview-card leader-card clickable${isActive ? ' active' : ''}" data-idx="${primaryIdx}">
      <div class="overview-label">${title}</div>
      <div class="overview-split runners${hasRunners ? '' : ' no-side'}">
        <div class="overview-main-copy">
          ${renderGroupSnapshot(items, { emphasizeChange: true })}
        </div>
        ${hasRunners ? renderLeaderRunnerPanel(runners) : ''}
      </div>
    </div>
  `;
}

export function renderLeaderRunnerPanel(runners = []) {
  if (!runners.length) {
    return '';
  }

  return `
    <div class="overview-rank-list">
      ${runners.slice(0, 4).map(runner => renderLeaderRunnerItem(runner)).join('')}
    </div>
  `;
}

export function renderLeaderRunnerItem(runner) {
  const items = getGroupItemsByPairId(runner.id);
  const primaryIdx = items?.[0]?.idx;
  const isActive = items ? items.some(item => item.idx === app.selectedIdx) : false;
  const idxAttr = primaryIdx == null ? '' : primaryIdx;
  const spreadChangeClass = getDirectionClass(runner.spreadChange);

  return `
    <button type="button" class="overview-rank-item${isActive ? ' active' : ''}" data-idx="${idxAttr}">
      <span class="overview-rank-name">${formatPairName(getOverviewPairName(runner.id, runner.name))}</span>
      <span class="overview-rank-meta">
        <span>${runner.spread.toFixed(1)}%</span>
        <span>/</span>
        <span class="spread-change ${spreadChangeClass} emphasis">${formatPointChange(runner.spreadChange)}</span>
      </span>
    </button>
  `;
}

export function getOverviewPairName(pairId, fallbackName) {
  const items = getGroupItemsByPairId(pairId);
  if (!items || !items.length) return fallbackName;
  const pair = items[0].pair;
  return items.length > 1 ? pair.commonName : pair.name;
}

export function renderGroupSnapshot(items, { emphasizeChange = false } = {}) {
  const top = items[0];
  const p = top.pair;
  const c = p.current;
  const dir = getDirectionClass(c.spreadChange);
  const displayName = items.length > 1 ? p.commonName : p.name;
  const displayNameHtml = items.length > 1
    ? escapeHtml(displayName)
    : renderPreferredInlineLabel(p, displayName);
  const emphasisClass = emphasizeChange ? ' emphasis' : '';

  return `
    <div class="name">${displayNameHtml}</div>
    <div class="spread-line">
      <div class="spread-val">${c.spread.toFixed(1)}%</div>
      <div class="spread-change ${dir}${emphasisClass}">${formatPointChange(c.spreadChange)}</div>
    </div>
    ${renderGroupPriceDetails(items)}
  `;
}

export function renderPriceLine(label, valueHtml) {
  return `<span class="price-line">
    <span class="price-label">${label}</span>
    <span class="price-value">${valueHtml}</span>
  </span>`;
}

export function renderGroupPriceDetails(items) {
  const p = items[0].pair;
  const c = p.current;
  const labels = getDetailLabels(p, items);

  if (items.length === 1) {
    return `<div class="prices">
      ${renderPriceLine(labels.common, `${formatPrice(c.commonPrice)} ${fmtChange(c.commonChange)}`)}
      ${renderPriceLine(renderPreferredInlineLabel(p, labels.preferred), `${formatPrice(c.preferredPrice)} ${fmtChange(c.preferredChange)}`)}
      <span class="div-info">배당 ${c.commonDivYield.toFixed(1)}% / ${c.preferredDivYield.toFixed(1)}%</span>
    </div>`;
  }

  const prefLines = items.map(item => {
    const ip = item.pair;
    const ic = ip.current;
    return renderPriceLine(renderPreferredInlineLabel(ip, getPreferredShortLabel(ip)), `${formatPrice(ic.preferredPrice)} ${fmtChange(ic.preferredChange)}`);
  }).join('');

  return `<div class="prices">
    ${renderPriceLine(labels.common, `${formatPrice(c.commonPrice)} ${fmtChange(c.commonChange)}`)}
    ${prefLines}
    <span class="div-info">배당 ${c.commonDivYield.toFixed(1)}% / ${c.preferredDivYield.toFixed(1)}%</span>
  </div>`;
}

// --- Cards ---
export function renderCards() {
  const cardGroups = getCardGroups();
  const el = document.getElementById('cards');
  el.innerHTML = cardGroups.map(items => {
    const top = items[0];
    const p = top.pair;
    const c = p.current;
    const primaryIdx = top.idx;
    const isActive = items.some(item => item.idx === app.selectedIdx);

    if (p.isAverage) {
      const dir = getDirectionClass(c.spreadChange);
      return `<div class="card avg-card${isActive ? ' active' : ''}" data-idx="${primaryIdx}" role="button" tabindex="0">
        <div class="name">${p.name}</div>
        <div class="spread-line">
          <div class="spread-val">${c.spread.toFixed(1)}%</div>
          <div class="spread-change ${dir}">${formatPointChange(c.spreadChange)}</div>
        </div>
      </div>`;
    }

    const pinned = isGroupPinned(items);
    return `<div class="card${isActive ? ' active' : ''}" data-idx="${primaryIdx}" role="button" tabindex="0">
      <button type="button" class="card-pin${pinned ? ' pinned' : ''}" data-pin-idx="${primaryIdx}" aria-label="관심종목 ${pinned ? '해제' : '등록'}" aria-pressed="${pinned}">${pinned ? '★' : '☆'}</button>
      ${renderGroupSnapshot(items)}
    </div>`;
  }).join('');
  el.querySelectorAll('.card').forEach(card => {
    const handleCardAction = () => {
      selectPair(parseInt(card.dataset.idx, 10));
    };
    card.addEventListener('click', handleCardAction);
    card.addEventListener('keydown', event => {
      if (event.target.closest?.('.card-pin')) return; // 핀 버튼은 자체 기본 동작(클릭) 사용
      if (event.key !== 'Enter' && event.key !== ' ') return;
      event.preventDefault();
      handleCardAction();
    });
  });
  el.querySelectorAll('.card-pin').forEach(button => {
    button.addEventListener('click', event => {
      event.stopPropagation();
      togglePinnedGroup(parseInt(button.dataset.pinIdx, 10));
    });
  });
  applyAutoFitText(el);
}

export function renderTableHeaders() {
  // index.html의 정적 th 수와 무관하게 TABLE_HEADER_CONFIG로부터 헤더 행을 완전 생성한다.
  const headRow = document.querySelector('.table-section thead tr');
  if (!headRow) return;
  // th가 재생성되므로 정렬 헤더에 있던 키보드 포커스는 동일 키 th로 복원한다.
  const activeSortKey = document.activeElement && headRow.contains(document.activeElement)
    ? document.activeElement.dataset?.sortKey || ''
    : '';
  headRow.innerHTML = TABLE_HEADER_CONFIG.map(config => {
    const isSorted = !!config.sortable && app.tableSortState.key === config.key;
    const classes = [
      config.sortable ? 'sortable' : '',
      config.numeric ? 'numeric' : '',
      isSorted && app.tableSortState.direction === 'asc' ? 'sorted-asc' : '',
      isSorted && app.tableSortState.direction === 'desc' ? 'sorted-desc' : '',
    ].filter(Boolean).join(' ');
    const ariaSort = isSorted
      ? (app.tableSortState.direction === 'asc' ? 'ascending' : 'descending')
      : 'none';
    // 괴리율 컬럼은 정적 마크업에 있던 min-width(바 셀 폭 확보)를 유지한다.
    const style = config.key === 'spread' ? ' style="min-width:120px"' : '';
    return `<th${classes ? ` class="${classes}"` : ''} data-sort-key="${config.sortable ? config.key : ''}"`
      + ` tabindex="${config.sortable ? 0 : -1}" aria-sort="${ariaSort}"${style}>${escapeHtml(config.label)}</th>`;
  }).join('');
  if (activeSortKey) {
    headRow.querySelector(`th[data-sort-key="${activeSortKey}"]`)?.focus();
  }
}

export function bindTableSortHeaders() {
  // th가 매 렌더마다 재생성되므로 th 개별 바인딩 대신 thead 위임 리스너로 처리한다.
  const tableHead = document.querySelector('.table-section thead');
  if (!tableHead || bindTableSortHeaders._bound) return;
  bindTableSortHeaders._bound = true;
  const findSortHeader = event => {
    const th = event.target.closest?.('th[data-sort-key]');
    return th && tableHead.contains(th) ? th : null;
  };
  const applySort = th => {
    const config = TABLE_HEADER_CONFIG.find(item => item.key === th.dataset.sortKey);
    if (!config?.sortable) return;
    if (app.tableSortState.key === config.key) {
      app.tableSortState.direction = app.tableSortState.direction === 'asc' ? 'desc' : 'asc';
    } else {
      app.tableSortState = {
        key: config.key,
        direction: TABLE_SORT_DEFAULT_DIRECTION[config.key] || 'desc',
      };
    }
    renderTable();
  };
  tableHead.addEventListener('click', event => {
    const th = findSortHeader(event);
    if (th) applySort(th);
  });
  tableHead.addEventListener('keydown', event => {
    if (event.key !== 'Enter' && event.key !== ' ') return;
    const th = findSortHeader(event);
    if (!th) return;
    event.preventDefault();
    applySort(th);
  });
}

export function getPairTableName(pair) {
  return pair?.preferredName || pair?.name || pair?.commonName || '';
}

// 배당차(실효 괴리 보정) = 우선주 배당수익률 − 보통주 배당수익률 (%p)
export function getDivYieldGap(current) {
  const preferredDivYield = current?.preferredDivYield;
  const commonDivYield = current?.commonDivYield;
  return Number.isFinite(preferredDivYield) && Number.isFinite(commonDivYield)
    ? preferredDivYield - commonDivYield
    : null;
}

export function formatDivYieldGap(value) {
  if (value == null || Number.isNaN(value)) return '-';
  return `${value > 0 ? '+' : ''}${value.toFixed(2)}%p`;
}

// 괴리 회수 추정: 괴리율이 그대로여도 추가 배당수익률 차이만으로 현재 괴리만큼 회수하는 데 걸리는 연수.
export function formatSpreadRecoveryYears(spread, divYieldGap) {
  if (spread == null || Number.isNaN(spread)) return '-';
  if (divYieldGap == null || Number.isNaN(divYieldGap) || divYieldGap <= 0.05) return '-';
  const years = spread / divYieldGap;
  if (!Number.isFinite(years) || years < 0) return '-';
  if (years > 99) return '99+년';
  return `${years.toFixed(1)}년`;
}

export function getTableRowMetrics(pair) {
  const current = pair.current || {};
  const commonMarketCap = calculateLiveMarketCap(
    current.commonPrice,
    current.commonSharesOutstanding,
    current.commonMarketCap,
  );
  const preferredMarketCap = calculateLiveMarketCap(
    current.preferredPrice,
    current.preferredSharesOutstanding,
    current.preferredMarketCap,
  );
  return {
    pair,
    name: getPairTableName(pair),
    commonMarketCap,
    preferredMarketCap,
    preferredRatio: calculatePreferredRatio(commonMarketCap, preferredMarketCap),
    divYieldGap: getDivYieldGap(current),
    spread: pair.current.spread,
    spreadChange: pair.current.spreadChange,
  };
}

export function compareTableMetric(a, b, direction) {
  const aMissing = a == null || Number.isNaN(a);
  const bMissing = b == null || Number.isNaN(b);
  if (aMissing && bMissing) return 0;
  if (aMissing) return 1;
  if (bMissing) return -1;
  if (a === b) return 0;
  return direction === 'asc' ? a - b : b - a;
}

export function compareTableRows(a, b) {
  if (app.tableSortState.key === 'name') {
    const base = a.name.localeCompare(b.name, 'ko');
    return app.tableSortState.direction === 'asc' ? base : -base;
  }

  const metricCompare = compareTableMetric(a[app.tableSortState.key], b[app.tableSortState.key], app.tableSortState.direction);
  if (metricCompare !== 0) return metricCompare;
  return a.name.localeCompare(b.name, 'ko');
}

// --- Table ---
export function renderTable() {
  renderTableHeaders();

  const stockRows = app.pairs
    .filter(p => !p.isAverage)
    .map(getTableRowMetrics)
    .sort(compareTableRows);
  const maxSpread = Math.max(1, ...stockRows.map(row => row.spread || 0));
  const tablePairs = stockRows.map(row => row.pair);
  const stockMetricsById = new Map(stockRows.map(row => [row.pair.id, row]));

  document.getElementById('tableBody').innerHTML = tablePairs.map(p => {
    const c = p.current;
    const textColorClass = getTextColorClass(c.spreadChange);
    const barW = (c.spread / maxSpread * 100).toFixed(1);
    const row = stockMetricsById.get(p.id);
    const displayName = row?.name || getPairTableName(p);
    const commonMarketCap = row?.commonMarketCap ?? null;
    const preferredMarketCap = row?.preferredMarketCap ?? null;
    const preferredRatio = row?.preferredRatio ?? null;
    const divYieldGap = row?.divYieldGap ?? null;
    return `<tr>
      <td><strong>${renderPreferredInlineLabel(p, displayName)}</strong></td>
      <td class="numeric">${formatPrice(c.commonPrice)}</td>
      <td class="numeric">${formatPrice(c.preferredPrice)}</td>
      <td class="numeric">${formatMarketCap(commonMarketCap)}</td>
      <td class="numeric">${formatMarketCap(preferredMarketCap)}</td>
      <td class="numeric">${formatRatioPercent(preferredRatio)}</td>
      <td class="numeric${divYieldGap > 0 ? ' div-yield' : ''}">${formatDivYieldGap(divYieldGap)}</td>
      <td class="numeric ${textColorClass}">${formatPointChange(c.spreadChange)}</td>
      <td class="numeric"><div class="bar-cell"><div class="bar" style="width:${barW}%"></div>${c.spread.toFixed(1)}%</div></td>
    </tr>`;
  }).join('');
}

// --- Stats ---
export function renderStats() {
  const p = app.pairs[app.selectedIdx];
  const hist = p.history || [];
  const spreads = hist.map(h => h.spread).filter(value => value != null && !Number.isNaN(value));
  const current = p.current.spread;
  const beta = calculatePairBeta(hist);
  const correlation = calculatePairCorrelation(hist);
  const sma250 = calculateSma(spreads, 250);
  const ema = calculateLatestEma(spreads) ?? p.current.spread;
  const spreadStats = calculateMeanStd(spreads);
  const oneYearCutoff = formatKstTimestamp(new Date(Date.now() - 365 * 24 * 60 * 60 * 1000)).slice(0, 10);
  const spreads1y = hist.filter(h => h.date >= oneYearCutoff).map(h => h.spread);
  const percentileAll = calculatePercentileRank(spreads, current);
  const percentile1y = calculatePercentileRank(spreads1y, current);
  const zScore = spreadStats && spreadStats.std > 0 && current != null && !Number.isNaN(current)
    ? (current - spreadStats.mean) / spreadStats.std
    : null;
  const preferredLabel = formatPairName(p.preferredName || "우선주");
  const commonLabel = formatPairName(p.commonName || "보통주");
  const commonMarketCap = calculateLiveMarketCap(
    p.current.commonPrice,
    p.current.commonSharesOutstanding,
    p.current.commonMarketCap,
  );
  const preferredMarketCap = calculateLiveMarketCap(
    p.current.preferredPrice,
    p.current.preferredSharesOutstanding,
    p.current.preferredMarketCap,
  );

  const renderChangeHtml = value => {
    if (value == null || Number.isNaN(value)) return "-";
    return `<span class="${getTextColorClass(value)}">${formatPercentChange(value)}</span>`;
  };
  const renderComboRows = rows => `<div class="stat-combo">${rows.map(row => `
    <div class="stat-combo-row">
      <span class="stat-combo-label">${row.label}</span>
      <span class="stat-combo-value">${row.value}</span>
    </div>
  `).join('')}</div>`;
  const formatDividendAmount = amount => {
    const value = Number(amount);
    if (amount == null || Number.isNaN(value)) return '-';
    return `${value.toLocaleString('ko-KR')}원`;
  };
  const buildRecentDividendRows = entries => {
    const commonByDate = new Map(
      (Array.isArray(entries?.common) ? entries.common : [])
        .filter(entry => entry?.date && entry.amount != null && !Number.isNaN(Number(entry.amount)))
        .map(entry => [entry.date, Number(entry.amount)]),
    );
    const preferredByDate = new Map(
      (Array.isArray(entries?.preferred) ? entries.preferred : [])
        .filter(entry => entry?.date && entry.amount != null && !Number.isNaN(Number(entry.amount)))
        .map(entry => [entry.date, Number(entry.amount)]),
    );
    return [...new Set([...commonByDate.keys(), ...preferredByDate.keys()])]
      .sort()
      .slice(-3)
      .reverse()
      .map(date => ({
        label: date,
        value: `<span class="dividend-pair-values"><span>우 ${formatDividendAmount(preferredByDate.get(date))}</span><span>보 ${formatDividendAmount(commonByDate.get(date))}</span></span>`,
      }));
  };

  const dividendYieldRows = [
    { label: renderPreferredYieldLabel(p), value: formatYield(p.current.preferredDivYield) },
    { label: "보통주", value: formatYield(p.current.commonDivYield) },
  ];
  if (!p.isAverage) {
    // 실효 괴리율(배당 보정): 배당수익률 차이(우선주 − 보통주)와, 괴리율이 그대로여도
    // 그 차이만으로 현재 괴리만큼 회수하는 데 걸리는 연수 추정.
    const divYieldGap = getDivYieldGap(p.current);
    const divYieldGapText = formatDivYieldGap(divYieldGap);
    dividendYieldRows.push(
      {
        label: "차이",
        value: divYieldGap > 0 ? `<span class="div-yield">${divYieldGapText}</span>` : divYieldGapText,
      },
      { label: "괴리 회수 추정", value: formatSpreadRecoveryYears(current, divYieldGap) },
    );
  }

  const stats = [
    {
      label: "괴리율",
      value: renderComboRows([
        { label: "현재", value: current == null ? "-" : `${current.toFixed(2)}%` },
        { label: "250일 평균", value: sma250 == null ? "-" : `${sma250.toFixed(2)}%` },
        { label: "EMA (K=0.1)", value: ema == null ? "-" : `${ema.toFixed(2)}%` },
      ]),
    },
    {
      label: "괴리율 위치",
      value: renderComboRows([
        { label: "전체 백분위", value: percentileAll == null ? "-" : `${Math.round(percentileAll)}%` },
        { label: "1Y 백분위", value: percentile1y == null ? "-" : `${Math.round(percentile1y)}%` },
        { label: "z-score", value: zScore == null ? "-" : `${zScore >= 0 ? '+' : ''}${zScore.toFixed(2)}σ` },
      ]),
    },
    {
      label: renderPreferredInlineLabel(p, preferredLabel),
      value: renderComboRows([
        { label: "현재가격", value: formatStatPrice(p.current.preferredPrice) },
        { label: "전일비", value: renderChangeHtml(p.current.preferredChange) },
      ]),
    },
    {
      label: commonLabel,
      value: renderComboRows([
        { label: "현재가격", value: formatStatPrice(p.current.commonPrice) },
        { label: "전일비", value: renderChangeHtml(p.current.commonChange) },
      ]),
    },
    {
      label: '베타 / 상관계수 <span class="stat-label-normal">r</span>',
      value: renderComboRows([
        { label: "베타", value: beta == null ? "-" : beta.toFixed(2) },
        { label: "r", value: correlation == null ? "-" : correlation.toFixed(2) },
      ]),
    },
    {
      label: "시가총액",
      value: renderComboRows([
        { label: "우선주", value: formatMarketCap(preferredMarketCap) },
        { label: "보통주", value: formatMarketCap(commonMarketCap) },
      ]),
    },
    {
      label: "거래액 (최근 1개월 평균)",
      value: renderComboRows([
        { label: "우선주", value: formatTradedValue(p.current.preferredAvgTradedValue20) },
        { label: "보통주", value: formatTradedValue(p.current.commonAvgTradedValue20) },
      ]),
    },
    {
      label: "배당수익률",
      value: renderComboRows(dividendYieldRows),
    },
  ];

  if (!p.isAverage) {
    stats.push({
      label: renderPreferredTermLabel(p),
      value: renderPreferredTermSummary(p),
      wide: true,
    });

    const recentDividendRows = buildRecentDividendRows(app.dividendHistories?.[p.id]);
    stats.push({
      label: "최근 배당",
      value: recentDividendRows.length ? renderComboRows(recentDividendRows) : "-",
    });
  }

  const statsEl = document.getElementById("statsRow");
  statsEl.innerHTML = stats.map(item => `<div class="stat-box${item.wide ? ' wide' : ''}"><div class="label">${item.label}</div><div class="value">${item.value}</div></div>`).join("");
}

export function bindCardSortControls() {
  const controls = document.getElementById('cardSortOptions');
  if (!controls || bindCardSortControls._bound) return;
  controls.addEventListener('click', event => {
    const button = event.target.closest('button[data-card-sort]');
    if (!button || !controls.contains(button)) return;
    app.cardSortMode = button.dataset.cardSort || 'spread';
    writeStoredValue(CARD_SORT_STORAGE_KEY, app.cardSortMode);
    controls.querySelectorAll('button').forEach(item => {
      item.classList.toggle('active', item === button);
    });
    renderCards();
  });
  bindCardSortControls._bound = true;
}

// --- Period buttons ---
export function bindPeriodBtns() {
  document.getElementById('periodBtns').addEventListener('click', function(e) {
    if (e.target.tagName !== 'BUTTON') return;
    this.querySelectorAll('button').forEach(b => b.classList.remove('active'));
    e.target.classList.add('active');
    app.periodDays = parseInt(e.target.dataset.days);
    writeStoredValue(PERIOD_DAYS_STORAGE_KEY, String(app.periodDays));
    resetZoomWindow({ render: false });
    renderZoomPanel();
    renderTable();
    renderChart();
    renderPriceChart();
    renderStats();
  });
}

export function exportSelectedPairCsv() {
  const pair = app.pairs[app.selectedIdx];
  if (!pair) return;
  const hist = getFilteredHistory(pair) || [];
  if (!hist.length) return;
  const csv = buildHistoryCsv(hist, { includeKospi: !!pair.isAverage });
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = `${pair.id}_${hist[0].date}_${hist[hist.length - 1].date}.csv`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

export function bindCsvExportButton() {
  const button = document.getElementById('csvExportBtn');
  if (!button || bindCsvExportButton._bound) return;
  button.addEventListener('click', exportSelectedPairCsv);
  bindCsvExportButton._bound = true;
}
