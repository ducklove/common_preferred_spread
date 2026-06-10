// js/market.js — 시장 메트릭 병합/야간선물 세션 로직 + buildTodaySummaryFromPairs
import { app, MARKET_EXTRA_SPECS, NIGHT_FUTURE_FRESH_AGE_MS, getAveragePair, getRepresentativePairs } from './state.js';
import {
  getCurrentKstDateString,
  getCurrentKstNightSessionDateString,
  isCurrentKstNightSession,
  toFiniteNumber,
} from './format.js';
import { calculateSimpleAverageMetrics, calculateSqrtPreferredMarketCapSpreadIndex } from './calc.js';

export function getAverageHistoryMarketFallback() {
  const averagePair = getAveragePair();
  if (!averagePair) return null;

  const kospiHistory = averagePair.history.filter(entry => entry.kospiPrice != null);
  const latestKospi = kospiHistory[kospiHistory.length - 1];
  const previousKospi = kospiHistory[kospiHistory.length - 2];
  if (!latestKospi) return null;

  return {
    id: 'KOSPI',
    name: '코스피',
    price: +latestKospi.kospiPrice.toFixed(2),
    change: previousKospi ? +(latestKospi.kospiPrice - previousKospi.kospiPrice).toFixed(2) : null,
    changePct: previousKospi ? +(((latestKospi.kospiPrice - previousKospi.kospiPrice) / previousKospi.kospiPrice) * 100).toFixed(2) : null,
  };
}

export function getRenderableMarketSummary(market) {
  return mergeMarketSummary(
    market,
    mergeMarketSummary(getAverageHistoryMarketFallback(), { extras: app.latestMarketExtras }),
  );
}

export function normalizeNightFutureMetric(metric) {
  if (!metric) return null;
  return {
    ...metric,
    price: metric.price != null ? Number(metric.price) : null,
    change: metric.change != null ? Number(metric.change) : null,
    changePct: metric.changePct != null ? Number(metric.changePct) : null,
    _snapshotMs: metric._snapshotMs != null ? Number(metric._snapshotMs) : null,
  };
}

export function stampNightFutureMetric(metric, snapshotMs = null) {
  if (!metric) return null;
  return {
    ...metric,
    _snapshotMs: snapshotMs != null ? snapshotMs : (metric._snapshotMs != null ? Number(metric._snapshotMs) : null),
  };
}

export function stampMarketNightFutureMetric(marketSummary, snapshotMs = null) {
  if (!marketSummary) return marketSummary;
  const stamped = { ...marketSummary };
  if (marketSummary.nightFuture) {
    stamped.nightFuture = stampNightFutureMetric(marketSummary.nightFuture, snapshotMs);
  }
  if (marketSummary.future) {
    stamped.future = stampNightFutureMetric(marketSummary.future, snapshotMs);
  }
  return stamped;
}

export function hasNightFutureMetricValue(metric) {
  if (!metric) return false;
  return [metric.price, metric.change, metric.changePct].some(
    value => value != null && !Number.isNaN(value)
  );
}

export function getFutureSessionKind(metric, now = new Date()) {
  if (!metric) return null;
  const source = String(metric.source || '').toLowerCase();
  const marketStatus = String(metric.marketStatus || '');
  const timeText = String(metric.time || '');

  if (['esignal_socket', 'investing_html', 'kis_websocket_trade'].includes(source)) {
    return 'night';
  }
  if (['hankyung_html', 'kis_future_quote'].includes(source)) {
    return 'day';
  }
  if (marketStatus.includes('야간') || timeText.includes('야간')) {
    return 'night';
  }
  if (marketStatus.includes('장중') || marketStatus.includes('장마감')) {
    return 'day';
  }
  return isCurrentKstNightSession(now) ? 'night' : 'day';
}

export function isNightFutureMetricCurrent(metric, now = new Date()) {
  if (!metric || !hasNightFutureMetricValue(metric)) return false;
  const sessionKind = getFutureSessionKind(metric, now);
  if (!metric.sessionTradeDate) {
    return isCurrentKstNightSession(now) ? sessionKind === 'night' : sessionKind !== 'night';
  }
  return isCurrentKstNightSession(now)
    ? (sessionKind === 'night' && metric.sessionTradeDate === getCurrentKstNightSessionDateString(now))
    : (sessionKind !== 'night' && metric.sessionTradeDate === getCurrentKstDateString(now));
}

export function isNightFutureMetricFresh(metric, now = new Date()) {
  if (!isNightFutureMetricCurrent(metric, now)) return false;
  if (metric?._snapshotMs == null || Number.isNaN(metric._snapshotMs)) return true;
  return Date.now() - metric._snapshotMs <= NIGHT_FUTURE_FRESH_AGE_MS;
}

export function mergeNightFutureMetric(primaryMetric, fallbackMetric, preservedMetric = app.latestNightFutureMetric) {
  const primary = normalizeNightFutureMetric(primaryMetric);
  const fallback = normalizeNightFutureMetric(fallbackMetric);
  const preserved = normalizeNightFutureMetric(preservedMetric);
  const candidates = [primary, fallback, preserved];
  return candidates.find(metric => isNightFutureMetricFresh(metric))
    || candidates.find(metric => isNightFutureMetricCurrent(metric))
    || candidates.find(metric => metric && !metric.sessionTradeDate && hasNightFutureMetricValue(metric))
    || null;
}

export function normalizeMarketExtras(extras = []) {
  const byId = new Map();
  extras.forEach(metric => {
    if (!metric || !metric.id) return;
    const spec = MARKET_EXTRA_SPECS.find(item => item.id === metric.id) || {};
    byId.set(metric.id, {
      ...spec,
      ...metric,
      name: metric.name || spec.name || metric.id,
      unit: metric.unit != null ? metric.unit : (spec.unit || ''),
    });
  });
  return MARKET_EXTRA_SPECS.map(spec => byId.get(spec.id) || {
    ...spec,
    price: null,
    change: null,
    changePct: null,
    marketStatus: null,
  });
}

export function hasMarketMetricValue(metric) {
  return metric && metric.price != null && !Number.isNaN(metric.price);
}

export function mergeMarketExtras(primaryExtras = [], fallbackExtras = [], preservedExtras = []) {
  const primary = normalizeMarketExtras(primaryExtras);
  const fallback = normalizeMarketExtras(fallbackExtras);
  const preserved = normalizeMarketExtras(preservedExtras);

  return MARKET_EXTRA_SPECS.map(spec => {
    const candidates = [
      primary.find(metric => metric.id === spec.id),
      fallback.find(metric => metric.id === spec.id),
      preserved.find(metric => metric.id === spec.id),
    ];
    return candidates.find(hasMarketMetricValue)
      || candidates.find(Boolean)
      || {
        ...spec,
        price: null,
        change: null,
        changePct: null,
        marketStatus: null,
      };
  });
}

export function mergeMarketSummary(primaryMarket, fallbackMarket) {
  if (!primaryMarket && !fallbackMarket) return null;
  const primary = primaryMarket || {};
  const fallback = fallbackMarket || {};

  return {
    id: primary.id || fallback.id || 'KOSPI',
    name: primary.name || fallback.name || '코스피',
    price: primary.price != null ? primary.price : fallback.price ?? null,
    change: primary.change != null ? primary.change : fallback.change ?? null,
    changePct: primary.changePct != null ? primary.changePct : fallback.changePct ?? null,
    marketStatus: primary.marketStatus || fallback.marketStatus || null,
    unit: primary.unit != null ? primary.unit : fallback.unit ?? null,
    extras: mergeMarketExtras(primary.extras || [], fallback.extras || [], app.latestMarketExtras),
    nightFuture: mergeNightFutureMetric(
      primary.nightFuture || primary.future || null,
      fallback.nightFuture || fallback.future || null,
    ),
  };
}

export function serializePairSummary(pair) {
  return {
    id: pair.id,
    name: pair.name,
    spread: pair.current.spread,
    spreadChange: pair.current.spreadChange,
  };
}

export function buildTodaySummaryFromPairs(marketOverride = null) {
  const representatives = getRepresentativePairs().filter(pair => pair.current.spread != null);
  if (!representatives.length) return null;

  const market = getRenderableMarketSummary(marketOverride);
  const spreadIndex = calculateSqrtPreferredMarketCapSpreadIndex();
  const simpleAverage = calculateSimpleAverageMetrics(representatives);
  const fallbackAverageSpread = simpleAverage.spread;
  const averageSpreadValue = spreadIndex?.spread ?? fallbackAverageSpread;
  const averagePair = getAveragePair();
  const averagePairSpread = toFiniteNumber(averagePair?.current?.spread);
  const averagePairSpreadChange = toFiniteNumber(averagePair?.current?.spreadChange);
  const indexSpreadChange = averagePairSpread != null
    && Math.abs(averagePairSpread - averageSpreadValue) < 0.005
    ? averagePairSpreadChange
    : null;
  const fallbackAverageSpreadChange = simpleAverage.spreadChange;

  const topWideningRanked = representatives
    .filter(pair => pair.current.spreadChange > 0)
    .sort((a, b) => b.current.spreadChange - a.current.spreadChange);
  const topNarrowingRanked = representatives
    .filter(pair => pair.current.spreadChange < 0)
    .sort((a, b) => a.current.spreadChange - b.current.spreadChange);
  const rankedByWidening = representatives
    .filter(pair => pair.current.spreadChange != null && !Number.isNaN(pair.current.spreadChange))
    .sort((a, b) => b.current.spreadChange - a.current.spreadChange);
  const rankedByNarrowing = representatives
    .filter(pair => pair.current.spreadChange != null && !Number.isNaN(pair.current.spreadChange))
    .sort((a, b) => a.current.spreadChange - b.current.spreadChange);
  const topWidening = topWideningRanked[0] || rankedByWidening[0] || null;
  const topNarrowing = topNarrowingRanked[0] || rankedByNarrowing[0] || null;

  return {
    source: '기본 데이터 기준',
    market,
    representativeCount: spreadIndex?.issuerCount || representatives.length,
    averageMethod: spreadIndex?.methodLabel || '동일비중',
    averageSpread: +averageSpreadValue.toFixed(2),
    averageSpreadChange: (indexSpreadChange ?? (spreadIndex ? null : fallbackAverageSpreadChange)) == null
      ? null
      : +(indexSpreadChange ?? (spreadIndex ? null : fallbackAverageSpreadChange)).toFixed(2),
    simpleAverageSpread: simpleAverage.spread == null ? null : +simpleAverage.spread.toFixed(2),
    simpleAverageSpreadChange: simpleAverage.spreadChange == null ? null : +simpleAverage.spreadChange.toFixed(2),
    averageSpreadVsSimple: simpleAverage.spread == null ? null : +(averageSpreadValue - simpleAverage.spread).toFixed(2),
    simpleAverageCount: simpleAverage.count,
    averageCommonChange: spreadIndex?.commonChange != null
      ? +spreadIndex.commonChange.toFixed(2)
      : simpleAverage.commonChange != null
      ? +simpleAverage.commonChange.toFixed(2)
      : null,
    averagePreferredChange: spreadIndex?.preferredChange != null
      ? +spreadIndex.preferredChange.toFixed(2)
      : simpleAverage.preferredChange != null
      ? +simpleAverage.preferredChange.toFixed(2)
      : null,
    topWidening: topWidening ? serializePairSummary(topWidening) : null,
    topWideningRunners: rankedByWidening
      .filter(pair => !topWidening || pair.id !== topWidening.id)
      .slice(0, 4)
      .map(serializePairSummary),
    topNarrowing: topNarrowing ? serializePairSummary(topNarrowing) : null,
    topNarrowingRunners: rankedByNarrowing
      .filter(pair => !topNarrowing || pair.id !== topNarrowing.id)
      .slice(0, 4)
      .map(serializePairSummary),
  };
}
