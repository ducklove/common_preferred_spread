// js/live.js — current.json 폴링 + 실시간 스냅샷 수집/적용 + 자동갱신 + 새로고침 버튼
// --- Live price updates from current.json ---
import {
  app,
  AUTO_REFRESH_INTERVAL_MS,
  LIVE_ESIGNAL_NIGHT_SOCKET_URL,
  LIVE_FETCH_TIMEOUT_MS,
  LIVE_INTERNAL_PROXY_BASE_URL,
  LIVE_INTERNAL_STOCK_BATCH_CONCURRENCY,
  LIVE_INTERNAL_STOCK_BATCH_SIZE,
  LIVE_INTERNAL_STOCK_CONCURRENCY,
  LIVE_INTERNAL_STOCK_MARKET,
  LIVE_REFRESH_FORCE_ATTEMPTS,
  LIVE_REFRESH_MAX_AGE_MS,
  LIVE_REFRESH_RETRY_ATTEMPTS,
  LIVE_REFRESH_RETRY_DELAY_MS,
  LIVE_REFRESH_TIMEOUT_MS,
  configLoadPromise,
  getAveragePair,
  getPrevDaySpread,
  isHistoryLoaded,
  upsertHistoryPoint,
} from './state.js';
import {
  formatKstTimestamp,
  getCurrentKstDateString,
  getCurrentKstNightSessionDateString,
  getCurrentKstNightSessionDayMonth,
  getTickerCode,
  isWeekendDateText,
  normalizeDateText,
  parseSnapshotTimestamp,
} from './format.js';
import {
  buildTodaySummaryFromPairs,
  mergeMarketExtras,
  mergeMarketSummary,
  mergeNightFutureMetric,
  stampMarketNightFutureMetric,
} from './market.js';
import { renderCards, renderStats, renderTable, renderTodayOverview } from './views.js';
import { renderChart, renderPriceChart, renderZoomPanel } from './charts.js';
import { renderHeatmap } from './heatmap.js';

export function getSnapshotPriceDate(snapshotPrice, fallbackDate) {
  const explicitDate = normalizeDateText(snapshotPrice?.date || snapshotPrice?.tradeDate || snapshotPrice?.priceDate);
  if (explicitDate) return explicitDate;

  const commonDate = normalizeDateText(snapshotPrice?.commonTradeDate);
  const preferredDate = normalizeDateText(snapshotPrice?.preferredTradeDate);
  if (commonDate && preferredDate && commonDate === preferredDate) return commonDate;
  if (commonDate && !preferredDate) return commonDate;
  if (preferredDate && !commonDate) return preferredDate;

  const normalizedFallback = normalizeDateText(fallbackDate);
  return normalizedFallback && !isWeekendDateText(normalizedFallback) ? normalizedFallback : null;
}

export function extractQuoteTradeDate(quote) {
  if (!quote) return null;
  const candidates = [
    quote.date,
    quote.tradeDate,
    quote.priceDate,
    quote.localTradedAt,
    quote.overMarketPriceInfo?.localTradedAt,
  ];
  for (const candidate of candidates) {
    const normalized = normalizeDateText(candidate);
    if (normalized) return normalized;
  }
  return null;
}

export function isSnapshotMsStale(timestampMs, maxAgeMs = LIVE_REFRESH_MAX_AGE_MS) {
  if (!timestampMs) return true;
  return Date.now() - timestampMs > maxAgeMs;
}

export function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export function withCacheBustingParam(url) {
  const separator = url.includes('?') ? '&' : '?';
  return `${url}${separator}_ts=${Date.now()}`;
}

export function encodePathSegment(value) {
  return encodeURIComponent(String(value || ''));
}

export function buildInternalProxyUrl(path, params = {}) {
  const base = LIVE_INTERNAL_PROXY_BASE_URL.endsWith('/')
    ? LIVE_INTERNAL_PROXY_BASE_URL
    : `${LIVE_INTERNAL_PROXY_BASE_URL}/`;
  const url = new URL(String(path || '').replace(/^\/+/, ''), base);
  Object.entries(params).forEach(([key, value]) => {
    if (value !== null && value !== undefined && value !== '') {
      url.searchParams.set(key, String(value));
    }
  });
  return url.toString();
}

export async function fetchWithTimeout(url, responseType = 'json', timeoutMs = LIVE_FETCH_TIMEOUT_MS) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const resp = await fetch(withCacheBustingParam(url), {
      signal: controller.signal,
      cache: 'no-store',
    });
    if (!resp.ok) {
      const error = new Error(`실시간 조회 실패 (${resp.status})`);
      error.status = resp.status;
      throw error;
    }
    return responseType === 'text' ? resp.text() : resp.json();
  } catch (e) {
    if (e && e.name === 'AbortError') {
      throw new Error('실시간 조회 시간 초과');
    }
    throw e;
  } finally {
    clearTimeout(timeoutId);
  }
}

export async function fetchInternalProxyJson(path, params = {}, timeoutMs = LIVE_FETCH_TIMEOUT_MS) {
  return fetchWithTimeout(buildInternalProxyUrl(path, params), 'json', timeoutMs);
}

export function isRateLimitError(error) {
  return error?.status === 429;
}

export async function runWithTimeout(task, timeoutMs, errorMessage) {
  let timeoutId = null;
  try {
    return await Promise.race([
      task,
      new Promise((_, reject) => {
        timeoutId = setTimeout(() => reject(new Error(errorMessage)), timeoutMs);
      }),
    ]);
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
  }
}

export function setRefreshButtonState(isBusy) {
  app.refreshButtonBusy = isBusy;
  const refreshBtn = document.getElementById('refreshBtn');
  if (!refreshBtn) return;
  refreshBtn.disabled = isBusy;
  refreshBtn.textContent = isBusy ? '갱신 중...' : '새로고침';
}

export function bindRefreshButton() {
  const refreshBtn = document.getElementById('refreshBtn');
  if (!refreshBtn) return;
  refreshBtn.addEventListener('click', async () => {
    if (app.refreshButtonBusy) return;
    setRefreshButtonState(true);
    try {
      await fetchCurrentPrices({ forceLiveRefresh: true });
    } finally {
      setRefreshButtonState(false);
    }
  });
}

export function scheduleAutoRefresh(delayMs = AUTO_REFRESH_INTERVAL_MS) {
  if (app.autoRefreshTimer) {
    clearTimeout(app.autoRefreshTimer);
  }
  app.autoRefreshTimer = setTimeout(async () => {
    if (document.hidden) {
      scheduleAutoRefresh(AUTO_REFRESH_INTERVAL_MS);
      return;
    }
    try {
      await fetchCurrentPrices();
    } finally {
      scheduleAutoRefresh(AUTO_REFRESH_INTERVAL_MS);
    }
  }, delayMs);
}

export function bindAutoRefresh() {
  scheduleAutoRefresh();
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) return;
    const elapsed = Date.now() - app.lastCurrentPricesFetchStartedAt;
    if (!app.lastCurrentPricesFetchStartedAt || elapsed >= AUTO_REFRESH_INTERVAL_MS) {
      fetchCurrentPrices();
    } else {
      scheduleAutoRefresh(Math.max(1000, AUTO_REFRESH_INTERVAL_MS - elapsed));
    }
  });
}

export function parseRawNumber(value) {
  if (value == null || value === '') return null;
  const parsed = parseFloat(String(value).replace(/,/g, ''));
  return Number.isNaN(parsed) ? null : parsed;
}

export function pickDefined(...values) {
  for (const value of values) {
    if (value !== null && value !== undefined) return value;
  }
  return null;
}

export function applySignedDirection(value, sign, referencePct = null) {
  const parsed = parseRawNumber(value);
  if (parsed == null) return null;
  const signText = String(sign || '').toUpperCase();
  if (['4', '5', 'FALLING', 'LOWER_LIMIT'].includes(signText)) return -Math.abs(parsed);
  if (['1', '2', 'RISING', 'UPPER_LIMIT'].includes(signText)) return Math.abs(parsed);
  const pct = parseRawNumber(referencePct);
  if (pct < 0) return -Math.abs(parsed);
  if (pct > 0) return Math.abs(parsed);
  if (pct === 0) return 0;
  return parsed;
}

export function getProxyTimestamp(payload) {
  const raw = payload?.raw || {};
  const tradedAt = raw.nxtOverMarketPriceInfo?.localTradedAt || raw.overMarketPriceInfo?.localTradedAt;
  if (tradedAt) return tradedAt;
  const polledAt = parseRawNumber(payload?.meta?.polled_at);
  if (polledAt != null) return formatKstTimestamp(new Date(polledAt));
  return null;
}

export function buildInternalStockQuote(payload, code) {
  if (!payload) return null;
  const summary = payload.summary || {};
  const raw = payload.raw || {};
  const sign = raw.prdy_vrss_sign || raw.rf || raw.compareToPreviousPrice?.code;
  const rawChangePct = pickDefined(summary.change_rate, summary.change_pct, raw.prdy_ctrt, raw.cr);
  const changePct = applySignedDirection(rawChangePct, sign, rawChangePct);
  const rawChange = pickDefined(summary.change, raw.prdy_vrss, raw.cv);
  const change = applySignedDirection(rawChange, sign, changePct);
  const price = parseRawNumber(pickDefined(summary.current_price, raw.stck_prpr, raw.nv));
  const tradedAt = getProxyTimestamp(payload);
  const tradeDate = normalizeDateText(tradedAt) || getCurrentKstDateString();

  if (price == null) return null;
  return {
    itemCode: code || payload.symbol || summary.symbol || raw.cd,
    stockName: summary.name || raw.nm || code,
    closePriceRaw: price,
    closePrice: price,
    compareToPreviousClosePriceRaw: change,
    compareToPreviousClosePrice: change,
    fluctuationsRatioRaw: changePct,
    fluctuationsRatio: changePct,
    marketStatus: summary.market_state || raw.ms || null,
    date: tradeDate,
    tradeDate,
    localTradedAt: tradedAt,
  };
}

export async function fetchInternalStockQuote(code) {
  try {
    const payload = await fetchInternalProxyJson(
      `/v1/stocks/${encodePathSegment(code)}/quote`,
      { market: LIVE_INTERNAL_STOCK_MARKET },
      6000,
    );
    const quote = buildInternalStockQuote(payload, code);
    if (quote) return quote;
  } catch (e) {
    if (isRateLimitError(e)) throw e;
    // KIS 프록시 실패 시 네이버 금융 프록시로 폴백한다.
  }

  const payload = await fetchInternalProxyJson(`/v1/naverfinance/stocks/${encodePathSegment(code)}/quote`, {}, 6000);
  return buildInternalStockQuote(payload, code);
}

export async function fetchInternalNaverStockQuotes(codes) {
  if (!codes.length) return [];
  const payload = await fetchInternalProxyJson(
    '/v1/naverfinance/stocks/quotes',
    { symbols: codes.join(',') },
    8000,
  );
  const items = Array.isArray(payload?.items) ? payload.items : [];
  return items
    .map(item => buildInternalStockQuote(item, item?.symbol))
    .filter(Boolean);
}

export async function fetchLiveJson(url, timeoutMs = LIVE_FETCH_TIMEOUT_MS) {
  return fetchWithTimeout(url, 'json', timeoutMs);
}

export async function fetchLiveText(url, timeoutMs = LIVE_FETCH_TIMEOUT_MS) {
  return fetchWithTimeout(url, 'text', timeoutMs);
}

export async function mapWithConcurrency(items, limit, mapper) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (true) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      if (currentIndex >= items.length) return;
      results[currentIndex] = await mapper(items[currentIndex], currentIndex);
    }
  }

  const workerCount = Math.min(limit, items.length);
  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  return results;
}

export function chunkArray(items, size) {
  const chunks = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

export async function fetchLiveStockQuoteMap(codes) {
  if (!codes.length) return new Map();

  const quoteMap = new Map();
  const batchResults = await mapWithConcurrency(
    chunkArray(codes, LIVE_INTERNAL_STOCK_BATCH_SIZE),
    LIVE_INTERNAL_STOCK_BATCH_CONCURRENCY,
    async batchCodes => {
      try {
        return await fetchInternalNaverStockQuotes(batchCodes);
      } catch (e) {
        if (isRateLimitError(e)) throw e;
        return [];
      }
    },
  );
  batchResults.flat().forEach(quote => {
    quoteMap.set(quote.itemCode, quote);
  });

  const missingCodes = codes.filter(code => !quoteMap.has(code));
  if (!missingCodes.length) return quoteMap;

  const fallbackQuotes = await mapWithConcurrency(
    missingCodes,
    LIVE_INTERNAL_STOCK_CONCURRENCY,
    async code => {
      try {
        return await fetchInternalStockQuote(code);
      } catch (e) {
        return null;
      }
    },
  );
  fallbackQuotes
    .filter(quote => quote?.itemCode)
    .forEach(quote => {
      quoteMap.set(quote.itemCode, quote);
    });

  return quoteMap;
}

export function buildLiveMarketMetric(quote, defaults = {}) {
  if (!quote) return null;
  const summary = quote.summary || {};
  const raw = quote.raw || {};
  const sign = raw.prdy_vrss_sign || raw.rf || raw.compareToPreviousPrice?.code;
  const rawChangePct = pickDefined(
    quote.fluctuationsRatioRaw,
    quote.fluctuationsRatio,
    summary.change_rate,
    summary.change_pct,
    raw.bstp_nmix_prdy_ctrt,
    raw.prdy_ctrt,
    raw.cr,
  );
  const rawChange = pickDefined(
    quote.compareToPreviousClosePriceRaw,
    quote.compareToPreviousClosePrice,
    summary.change,
    raw.bstp_nmix_prdy_vrss,
    raw.prdy_vrss,
    raw.cv,
  );
  const price = parseRawNumber(pickDefined(
    quote.closePriceRaw,
    quote.closePrice,
    summary.current_price,
    summary.regular_market_price,
    raw.bstp_nmix_prpr,
    raw.stck_prpr,
    raw.nv,
  ));
  const changePct = applySignedDirection(rawChangePct, sign, rawChangePct);
  const change = applySignedDirection(rawChange, sign, changePct);
  if (price == null && change == null && changePct == null) return null;
  return {
    id: defaults.id || quote.itemCode || quote.symbolCode || quote.reutersCode || quote.symbol || summary.symbol,
    name: defaults.name || quote.stockName || quote.indexName || summary.name || summary.symbol || defaults.id,
    price,
    change,
    changePct,
    marketStatus: quote.marketStatus || summary.market_state || raw.ms || null,
    unit: defaults.unit || '',
    priceDecimals: defaults.priceDecimals == null ? 2 : defaults.priceDecimals,
  };
}

export function extractMarketIndexMetricFromHtml(html, headClass, defaults = {}) {
  if (!html) return null;
  const doc = new DOMParser().parseFromString(html, 'text/html');
  const link = doc.querySelector(`a.head.${headClass}`);
  const item = link ? link.closest('li') : null;
  if (!item) return null;

  const headInfo = item.querySelector('.head_info');
  const price = parseRawNumber(headInfo?.querySelector('.value')?.textContent);
  const rawChange = parseRawNumber(headInfo?.querySelector('.change')?.textContent);
  if (price == null || rawChange == null) return null;

  let signedChange = rawChange;
  if (headInfo?.classList.contains('point_up')) {
    signedChange = Math.abs(rawChange);
  } else if (headInfo?.classList.contains('point_dn')) {
    signedChange = -Math.abs(rawChange);
  } else if (headInfo?.classList.contains('point_eq')) {
    signedChange = 0;
  }

  const previousPrice = price - signedChange;
  const changePct = previousPrice ? +((signedChange / previousPrice) * 100).toFixed(2) : null;

  return {
    id: defaults.id,
    name: defaults.name,
    price: +price.toFixed(2),
    change: +signedChange.toFixed(2),
    changePct,
    marketStatus: null,
    unit: defaults.unit || '',
    priceDecimals: defaults.priceDecimals == null ? 2 : defaults.priceDecimals,
  };
}

export function extractNightFutureMetricFromHtml(html) {
  if (!html) return null;
  const patterns = {
    price: /data-test="instrument-price-last"[^>]*>([^<]+)</i,
    change: /data-test="instrument-price-change"[^>]*>([^<]+)</i,
    changePct: /data-test="instrument-price-change-percent"[^>]*>([^<]+)</i,
    time: /data-test="trading-time-label"[^>]*>([^<]+)</i,
  };
  const values = Object.fromEntries(
    Object.entries(patterns).map(([key, pattern]) => {
      const match = html.match(pattern);
      return [key, match ? match[1].trim() : null];
    }),
  );

  const price = parseRawNumber(values.price);
  const change = parseRawNumber(values.change);
  const changePct = parseRawNumber((values.changePct || '').replace(/[()%]/g, ''));
  const timeText = (values.time || '').trim();
  if (price == null || change == null || changePct == null) return null;
  if (timeText && /^\d{2}\/\d{2}$/.test(timeText) && timeText !== getCurrentKstNightSessionDayMonth()) {
    return null;
  }

  return {
    id: 'KOSPI200_FUTURES',
    name: 'KOSPI200 선물',
    price: +price.toFixed(2),
    change: +change.toFixed(2),
    changePct: +changePct.toFixed(2),
    marketStatus: null,
    unit: '',
    time: timeText || null,
    source: 'investing_html',
    sessionTradeDate: getCurrentKstNightSessionDateString(),
  };
}

export function buildEsignalNightFutureMetric(payload) {
  if (!payload) return null;
  const price = parseRawNumber(payload.value);
  const change = parseRawNumber(payload.value_diff);
  const basePrice = parseRawNumber(payload.value_day);
  if (price == null || change == null || basePrice == null || basePrice === 0) return null;

  const timestamp = payload.tstamp ? new Date(payload.tstamp) : null;
  const parsedTimestamp = timestamp && !Number.isNaN(timestamp.getTime()) ? timestamp : null;
  const sessionTradeDate = parsedTimestamp
    ? getCurrentKstNightSessionDateString(parsedTimestamp)
    : getCurrentKstNightSessionDateString();
  if (sessionTradeDate !== getCurrentKstNightSessionDateString()) return null;

  const timeText = payload.ttime != null
    ? String(payload.ttime).padStart(6, '0').replace(/(\d{2})(\d{2})(\d{2})/, '$1:$2:$3')
    : (parsedTimestamp ? formatKstTimestamp(parsedTimestamp).slice(11, 19) : null);

  return {
    id: 'KOSPI200_FUTURES',
    name: 'KOSPI200 선물',
    price: +price.toFixed(2),
    change: +change.toFixed(2),
    changePct: +(((price - basePrice) / basePrice) * 100).toFixed(2),
    marketStatus: '야간',
    unit: '',
    code: payload.symbol || null,
    time: timeText || null,
    source: 'esignal_socket',
    sessionTradeDate,
  };
}

export async function fetchLiveEsignalNightFutureMetric(timeoutMs = 7000) {
  if (typeof WebSocket !== 'function') return null;

  return await new Promise(resolve => {
    let settled = false;
    let ws = null;
    const finish = value => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try {
        if (ws && ws.readyState === WebSocket.OPEN) ws.close();
      } catch (e) {
        // ignore close errors
      }
      resolve(value || null);
    };
    const timer = setTimeout(() => finish(null), timeoutMs);

    try {
      ws = new WebSocket(LIVE_ESIGNAL_NIGHT_SOCKET_URL);
    } catch (e) {
      finish(null);
      return;
    }

    ws.addEventListener('message', event => {
      const raw = String(event.data || '');
      if (!raw) return;
      if (raw.startsWith('0')) {
        try {
          ws.send('40');
        } catch (e) {
          finish(null);
        }
        return;
      }
      if (raw === '2') {
        try {
          ws.send('3');
        } catch (e) {
          finish(null);
        }
        return;
      }
      if (!raw.startsWith('42')) return;

      try {
        const packet = JSON.parse(raw.slice(2));
        if (!Array.isArray(packet) || packet[0] !== 'populate') return;
        const payload = typeof packet[1] === 'string' ? JSON.parse(packet[1]) : packet[1];
        finish(buildEsignalNightFutureMetric(payload));
      } catch (e) {
        finish(null);
      }
    });
    ws.addEventListener('error', () => finish(null));
    ws.addEventListener('close', () => finish(null));
  });
}

export function extractDayFutureMetricFromHankyungHtml(html) {
  if (!html) return null;
  const match = html.match(
    /<div class="stock-data(?:\s+(\w+))?">[\s\S]*?<p class="price">\s*([\d,]+\.\d+)\s*<\/p>[\s\S]*?<span class="stock-point">\s*([\d,]+\.\d+)\s*<\/span>[\s\S]*?<span class="rate">\s*([+-]?[\d,]+\.\d+%)\s*<\/span>[\s\S]*?<p class="txt-info txt-rt"[^>]*>\s*(\d{4}\.\d{2}\.\d{2})\s*([^<]+?)\s*<\/p>/i,
  );
  if (!match) return null;

  const [, direction = '', priceText, changeText, changePctText, tradeDateText, statusText] = match;
  const price = parseRawNumber(priceText);
  let change = parseRawNumber(changeText);
  const changePct = parseRawNumber(String(changePctText).replace('%', ''));
  const tradeDate = String(tradeDateText).replace(/\./g, '-');
  const status = String(statusText || '').replace(/\s+/g, ' ').trim();
  if (price == null || change == null || changePct == null) return null;
  if (tradeDate !== getCurrentKstDateString()) return null;

  if (changePct < 0 || direction.toLowerCase() === 'down') {
    change = -Math.abs(change);
  } else if (changePct > 0 || direction.toLowerCase() === 'up') {
    change = Math.abs(change);
  }

  return {
    id: 'KOSPI200_FUTURES',
    name: 'KOSPI200 선물',
    price: +price.toFixed(2),
    change: +change.toFixed(2),
    changePct: +changePct.toFixed(2),
    marketStatus: status || null,
    unit: '',
    time: status || null,
    source: 'hankyung_html',
    sessionTradeDate: tradeDate,
  };
}

export function buildLiveMarketSummary(marketQuote, extras = [], nightFuture = null) {
  if (!marketQuote && !extras.length) return null;
  const summary = buildLiveMarketMetric(marketQuote, { id: 'KOSPI', name: '코스피' }) || {
    id: 'KOSPI',
    name: '코스피',
    price: null,
    change: null,
    changePct: null,
    marketStatus: null,
  };
  if (extras.length) {
    summary.extras = extras;
  }
  if (nightFuture) {
    summary.nightFuture = nightFuture;
  }
  return summary;
}

export async function buildLiveSnapshot() {
  await configLoadPromise;
  if (!app.pairConfigMap.size) {
    throw new Error('config.json을 불러오지 못했습니다.');
  }

  const uniqueCodes = [...new Set(
    [...app.pairConfigMap.values()].flatMap(item => [
      getTickerCode(item.commonTicker),
      getTickerCode(item.preferredTicker),
    ]).filter(Boolean)
  )];

  const quoteMap = await fetchLiveStockQuoteMap(uniqueCodes);

  const [marketPayload, kosdaqPayload, usdKrwPayload, goldPayload, sp500Payload] = await Promise.all([
    fetchInternalProxyJson('/v1/indexes/kospi/quote', {}, 6000).catch(() => null),
    fetchInternalProxyJson('/v1/indexes/kosdaq/quote', {}, 6000).catch(() => null),
    fetchInternalProxyJson(`/v1/yfinance/stocks/${encodePathSegment('USDKRW=X')}/quote`, {}, 6000).catch(() => null),
    fetchInternalProxyJson(`/v1/yfinance/stocks/${encodePathSegment('GC=F')}/quote`, {}, 6000).catch(() => null),
    fetchInternalProxyJson(`/v1/yfinance/stocks/${encodePathSegment('^GSPC')}/quote`, {}, 6000).catch(() => null),
  ]);
  const marketExtras = [
    buildLiveMarketMetric(kosdaqPayload, { id: 'KOSDAQ', name: 'KOSDAQ', priceDecimals: 2 }),
    buildLiveMarketMetric(usdKrwPayload, { id: 'USDKRW', name: '환율', unit: '원', priceDecimals: 2 }),
    buildLiveMarketMetric(goldPayload, { id: 'GOLD', name: '금가격 (COMEX)', unit: '', priceDecimals: 2 }),
    buildLiveMarketMetric(sp500Payload, { id: 'SP500', name: 'S&P500', priceDecimals: 2 }),
  ].filter(Boolean);

  const prices = {};
  for (const pair of app.pairs.filter(item => !item.isAverage)) {
    const config = app.pairConfigMap.get(pair.id);
    if (!config) continue;

    const commonQuote = quoteMap.get(getTickerCode(config.commonTicker));
    const preferredQuote = quoteMap.get(getTickerCode(config.preferredTicker));
    if (!commonQuote || !preferredQuote) continue;

    const commonPrice = parseRawNumber(pickDefined(commonQuote.closePriceRaw, commonQuote.closePrice));
    const preferredPrice = parseRawNumber(pickDefined(preferredQuote.closePriceRaw, preferredQuote.closePrice));
    const commonDelta = parseRawNumber(pickDefined(commonQuote.compareToPreviousClosePriceRaw, commonQuote.compareToPreviousClosePrice));
    const preferredDelta = parseRawNumber(pickDefined(preferredQuote.compareToPreviousClosePriceRaw, preferredQuote.compareToPreviousClosePrice));
    const previousCommonPrice = commonPrice != null && commonDelta != null ? commonPrice - commonDelta : null;
    const previousPreferredPrice = preferredPrice != null && preferredDelta != null ? preferredPrice - preferredDelta : null;
    const spread = commonPrice != null && commonPrice !== 0 && preferredPrice != null
      ? +(((commonPrice - preferredPrice) / commonPrice) * 100).toFixed(2)
      : null;
    const previousSpread = previousCommonPrice != null && previousCommonPrice !== 0 && previousPreferredPrice != null
      ? +(((previousCommonPrice - previousPreferredPrice) / previousCommonPrice) * 100).toFixed(2)
      : null;
    const commonTradeDate = extractQuoteTradeDate(commonQuote);
    const preferredTradeDate = extractQuoteTradeDate(preferredQuote);
    const priceDate = commonTradeDate && preferredTradeDate && commonTradeDate === preferredTradeDate
      ? commonTradeDate
      : null;

    prices[pair.id] = {
      date: priceDate,
      commonTradeDate,
      preferredTradeDate,
      commonPrice,
      preferredPrice,
      spread,
      spreadChange: spread != null && previousSpread != null ? +(spread - previousSpread).toFixed(2) : null,
      commonChange: parseRawNumber(pickDefined(commonQuote.fluctuationsRatioRaw, commonQuote.fluctuationsRatio)),
      preferredChange: parseRawNumber(pickDefined(preferredQuote.fluctuationsRatioRaw, preferredQuote.fluctuationsRatio)),
    };
  }

  const market = buildLiveMarketSummary(marketPayload, marketExtras);
  const latestTime = new Date();

  if (!Object.keys(prices).length && !market) {
    throw new Error('실시간 시세를 가져오지 못했습니다.');
  }

  return {
    source: '내부 프록시 실시간',
    lastUpdated: formatKstTimestamp(latestTime),
    prices,
    market,
  };
}

export function applyCurrentSnapshot(cur, fallbackSource = '네이버 증권') {
  const incomingSnapshotMs = parseSnapshotTimestamp(cur.lastUpdated)?.getTime() || 0;
  if (incomingSnapshotMs && app.latestAppliedSnapshotMs && incomingSnapshotMs < app.latestAppliedSnapshotMs) {
    return false;
  }

  const appliedSnapshotMs = incomingSnapshotMs || Date.now();
  const previousMarketSummary = app.todayOverviewData?.market || null;
  const incomingMarketSummary = mergeMarketSummary(cur.market || null, cur.summary?.market || null);
  const marketSummary = stampMarketNightFutureMetric(
    mergeMarketSummary(incomingMarketSummary, previousMarketSummary),
    appliedSnapshotMs,
  );
  const effectiveLastUpdated = cur.lastUpdated || formatKstTimestamp(new Date());
  const snapshotDate = effectiveLastUpdated.slice(0, 10);
  const appliedHistoryDates = [];

  app.pairs.forEach(p => {
    if (p.isAverage) return;
    const cp = cur.prices?.[p.id];
    if (!cp) return;
    const priceDate = getSnapshotPriceDate(cp, snapshotDate);

    p.current.commonPrice = cp.commonPrice;
    p.current.preferredPrice = cp.preferredPrice;
    p.current.spread = cp.spread;
    if (cp.commonChange != null) p.current.commonChange = cp.commonChange;
    if (cp.preferredChange != null) p.current.preferredChange = cp.preferredChange;
    if (cp.spreadChange != null) {
      p.current.spreadChange = cp.spreadChange;
    }

    const prev = priceDate ? getPrevDaySpread(p, priceDate) : null;
    if (cp.spreadChange == null && prev !== null) {
      p.current.spreadChange = +(cp.spread - prev).toFixed(2);
    }

    if (priceDate) {
      const point = {
        commonPrice: cp.commonPrice,
        preferredPrice: cp.preferredPrice,
        spread: cp.spread,
      };
      app.lastAppliedPriceByPairId.set(p.id, { priceDate, point });
      if (isHistoryLoaded(p) && upsertHistoryPoint(p, priceDate, point)) {
        appliedHistoryDates.push(priceDate);
      }
    }
  });

  const nextSummary = buildTodaySummaryFromPairs(marketSummary);

  let resolvedIndexSpreadChange = null;
  const avgPair = getAveragePair();
  if (avgPair && (cur.averageSpread != null || cur.indexSpread != null || nextSummary?.averageSpread != null)) {
    const uniqueHistoryDates = [...new Set(appliedHistoryDates)].sort();
    const averageHistoryDate = (
      normalizeDateText(cur.date || cur.tradeDate || cur.priceDate)
      || uniqueHistoryDates[uniqueHistoryDates.length - 1]
      || (!isWeekendDateText(snapshotDate) ? snapshotDate : null)
    );
    const prev = averageHistoryDate ? getPrevDaySpread(avgPair, averageHistoryDate) : null;
    // current.json의 averageSpread는 대표종목 단순평균이므로, sqrt 시총가중 지수(프런트 nextSummary → 서버 indexSpread)를 우선한다.
    const averageSpread = nextSummary?.averageSpread ?? cur.indexSpread ?? cur.averageSpread;
    // 전일비도 같은 방식 우선: sqrt 지수 → 전일 지수 대비 직접 계산 → 서버 indexSpreadChange → 단순평균 변화 순.
    let averageSpreadChange = nextSummary?.averageSpreadChange;
    if (averageSpreadChange == null && prev !== null && averageSpread != null) {
      averageSpreadChange = +(averageSpread - prev).toFixed(2);
    }
    if (averageSpreadChange == null) {
      averageSpreadChange = cur.indexSpreadChange ?? cur.averageSpreadChange;
    }

    avgPair.current.spread = averageSpread;
    if (averageSpreadChange != null) {
      avgPair.current.spreadChange = averageSpreadChange;
      resolvedIndexSpreadChange = averageSpreadChange;
    }

    if (averageHistoryDate && isHistoryLoaded(avgPair)) {
      const nextEntry = { commonPrice: 0, preferredPrice: 0, spread: averageSpread };
      if (marketSummary?.price != null) nextEntry.kospiPrice = marketSummary.price;
      upsertHistoryPoint(avgPair, averageHistoryDate, nextEntry);
    }
  }

  app.todayOverviewData = cur.summary
    ? {
        ...(nextSummary || {}),
        ...cur.summary,
        // 지수 관련 값은 sqrt 시총가중 방식(nextSummary)을 우선한다. (topWidening 등 리더 정보는 cur.summary 유지)
        ...(nextSummary?.averageSpread != null ? {
          averageSpread: nextSummary.averageSpread,
          averageSpreadChange: nextSummary.averageSpreadChange ?? resolvedIndexSpreadChange,
          averageMethod: nextSummary.averageMethod,
          simpleAverageSpread: nextSummary.simpleAverageSpread,
          averageSpreadVsSimple: nextSummary.averageSpreadVsSimple,
          representativeCount: nextSummary.representativeCount,
        } : {}),
        market: stampMarketNightFutureMetric(
          mergeMarketSummary(cur.summary.market || nextSummary?.market || null, marketSummary),
          appliedSnapshotMs,
        ),
        source: cur.source || fallbackSource,
      }
    : {
        ...(nextSummary || {}),
        market: stampMarketNightFutureMetric(
          mergeMarketSummary(nextSummary?.market || null, marketSummary),
          appliedSnapshotMs,
        ),
        source: cur.source || fallbackSource,
      };

  app.latestMarketExtras = mergeMarketExtras(
    app.todayOverviewData?.market?.extras || [],
    app.latestMarketExtras,
  );
  app.latestNightFutureMetric = mergeNightFutureMetric(
    app.todayOverviewData?.market?.nightFuture || app.todayOverviewData?.market?.future || null,
    app.latestNightFutureMetric,
  );

  app.latestAppliedSnapshotMs = appliedSnapshotMs;
  document.getElementById('lastUpdated').textContent = '최종 업데이트: ' + effectiveLastUpdated;
  renderTodayOverview();
  renderCards();
  renderTable();
  renderZoomPanel();
  renderChart();
  renderPriceChart();
  renderStats();
  renderHeatmap();
  return true;
}

export async function refreshLivePricesIfNeeded(baseSnapshot, { forceRefresh = false } = {}) {
  const baseSnapshotMs = parseSnapshotTimestamp(baseSnapshot?.lastUpdated)?.getTime() || 0;
  const referenceSnapshotMs = Math.max(baseSnapshotMs, app.latestAppliedSnapshotMs || 0);
  if (!forceRefresh && !isSnapshotMsStale(referenceSnapshotMs)) return false;
  if (app.liveRefreshPromise) {
    return app.liveRefreshPromise;
  }

  app.liveRefreshPromise = (async () => {
    try {
      const liveSnapshot = await runWithTimeout(
        buildLiveSnapshot(),
        LIVE_REFRESH_TIMEOUT_MS,
        '실시간 갱신 시간이 초과되었습니다.',
      );
      return applyCurrentSnapshot(liveSnapshot, '내부 프록시 실시간');
    } catch (e) {
      // 실시간 프록시 갱신 실패 시 기존 스냅샷을 유지한다.
      return false;
    } finally {
      app.liveRefreshPromise = null;
    }
  })();

  return app.liveRefreshPromise;
}

export async function refreshLivePricesWithRetry(
  baseSnapshot,
  attempts = LIVE_REFRESH_RETRY_ATTEMPTS,
  options = {},
) {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    const refreshed = await refreshLivePricesIfNeeded(baseSnapshot, options);
    const hasFreshSnapshot = !isSnapshotMsStale(app.latestAppliedSnapshotMs || 0);
    if (refreshed || (!options.forceRefresh && hasFreshSnapshot)) {
      return refreshed;
    }
    if (attempt < attempts - 1) {
      await delay(LIVE_REFRESH_RETRY_DELAY_MS);
    }
  }
  return false;
}

export async function fetchCurrentPrices({ forceLiveRefresh = false } = {}) {
  if (app.currentPricesFetchPromise) {
    return app.currentPricesFetchPromise;
  }

  app.lastCurrentPricesFetchStartedAt = Date.now();
  app.currentPricesFetchPromise = (async () => {
    let baseSnapshot = {
      source: 'data.js',
      lastUpdated: app.STOCK_DATA.lastUpdated,
    };

    try {
      const resp = await fetch('current.json?t=' + Date.now());
      if (resp.ok) {
        const cur = await resp.json();
        applyCurrentSnapshot(cur, cur.source || '네이버 증권');
        baseSnapshot = cur;
      }
    } catch (e) {
      // current.json 로드 실패 시 data.js 기준으로 실시간 조회만 시도한다.
    }

    await refreshLivePricesWithRetry(
      baseSnapshot,
      forceLiveRefresh ? LIVE_REFRESH_FORCE_ATTEMPTS : LIVE_REFRESH_RETRY_ATTEMPTS,
      {
      forceRefresh: forceLiveRefresh,
      },
    );
  })();

  try {
    return await app.currentPricesFetchPromise;
  } finally {
    app.currentPricesFetchPromise = null;
  }
}
