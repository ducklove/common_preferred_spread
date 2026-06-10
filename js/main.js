// js/main.js — initializeDashboard + resize 리스너 + 부트 호출
import { app, configLoadPromise, ensureHistory, getAveragePair, loadSummaryData } from './state.js';
import { parseSnapshotTimestamp } from './format.js';
import { buildTodaySummaryFromPairs, normalizeMarketExtras, stampMarketNightFutureMetric } from './market.js';
import {
  applyAutoFitText,
  bindCardSortControls,
  bindCsvExportButton,
  bindIndexWeightModal,
  bindPeriodBtns,
  bindTableSortHeaders,
  bindThemeButton,
  queueDividendRender,
  renderCards,
  renderStats,
  renderTable,
  renderTodayOverview,
  resolveSelectedPairIndexFromQuery,
  restoreStoredSettings,
  scrollToSelectedSpreadSection,
  updateSelectedPairQueryParam,
} from './views.js';
import { bindZoomControls, renderChart, renderPriceChart, renderZoomPanel } from './charts.js';
import { bindAutoRefresh, bindRefreshButton, fetchCurrentPrices } from './live.js';

// --- Init ---
export async function initializeDashboard() {
  document.getElementById('lastUpdated').textContent = '데이터 불러오는 중...';
  bindThemeButton();
  bindTableSortHeaders();
  restoreStoredSettings();
  await loadSummaryData();
  app.todayOverviewData = buildTodaySummaryFromPairs();
  app.latestAppliedSnapshotMs = parseSnapshotTimestamp(app.STOCK_DATA.lastUpdated)?.getTime() || 0;
  if (app.todayOverviewData?.market) {
    app.todayOverviewData = {
      ...app.todayOverviewData,
      market: stampMarketNightFutureMetric(app.todayOverviewData.market, app.latestAppliedSnapshotMs),
    };
  }
  app.latestMarketExtras = normalizeMarketExtras(app.todayOverviewData?.market?.extras || []);
  app.latestNightFutureMetric = app.todayOverviewData?.market?.nightFuture || app.todayOverviewData?.market?.future || null;
  document.getElementById('lastUpdated').textContent = '최종 업데이트: ' + app.STOCK_DATA.lastUpdated;
  await configLoadPromise;
  const initialSelection = resolveSelectedPairIndexFromQuery();
  app.selectedIdx = initialSelection.idx;
  // 차트에 필요한 초기 히스토리: 지수(_average, 스파크라인용)와 선택 종목
  await Promise.all([ensureHistory(getAveragePair()), ensureHistory(app.pairs[app.selectedIdx])].filter(Boolean));
  renderTodayOverview();
  renderCards();
  renderTable();
  bindIndexWeightModal();
  bindCardSortControls();
  bindZoomControls();
  renderZoomPanel();
  renderChart();
  renderPriceChart();
  renderStats();
  queueDividendRender();
  bindPeriodBtns();
  bindCsvExportButton();
  if (initialSelection.matched) {
    updateSelectedPairQueryParam();
    scrollToSelectedSpreadSection();
  }
  bindRefreshButton();
  bindAutoRefresh();
  fetchCurrentPrices();
}

initializeDashboard().catch(e => {
  console.error(e);
  document.getElementById('lastUpdated').textContent = '데이터 로드 실패: 새로고침 해주세요';
});

// Resize
window.addEventListener('resize', () => {
  applyAutoFitText();
  renderZoomPanel();
  renderChart();
  renderPriceChart();
});
