// js/state.js — 공유 상태 객체(app) + 상수 + 스토리지 헬퍼 + 데이터 로더 + 그룹핑 + 히스토리 upsert
import { normalizeDateText } from './format.js';

export const DATA_SUMMARY_URL = 'data/summary.json';
export const LEGACY_DATA_JS_URL = 'data.js';
export const DATA_DIVIDENDS_URL = 'data/dividends.json';
export const THEME_STORAGE_KEY = 'theme';
export const PINNED_PAIRS_STORAGE_KEY = 'pinnedPairIds';
export const CARD_SORT_STORAGE_KEY = 'cardSortMode';
export const PERIOD_DAYS_STORAGE_KEY = 'periodDays';
export const PERIOD_DAYS_OPTIONS = [0, 30, 90, 180, 365];
export const LIVE_REFRESH_MAX_AGE_MS = 60 * 1000;
export const LIVE_REFRESH_RETRY_DELAY_MS = 1500;
export const LIVE_REFRESH_RETRY_ATTEMPTS = 3;
export const LIVE_REFRESH_FORCE_ATTEMPTS = 1;
export const LIVE_FETCH_TIMEOUT_MS = 2500;
export const LIVE_REFRESH_TIMEOUT_MS = 15000;
export const AUTO_REFRESH_INTERVAL_MS = 300 * 1000;
export const NIGHT_FUTURE_FRESH_AGE_MS = 10 * 60 * 1000;
export const LIVE_INTERNAL_PROXY_BASE_URL = 'https://cantabile.tplinkdns.com:3298';
export const LIVE_INTERNAL_STOCK_MARKET = 'UN';
export const LIVE_INTERNAL_STOCK_BATCH_SIZE = 20;
export const LIVE_INTERNAL_STOCK_BATCH_CONCURRENCY = 1;
export const LIVE_INTERNAL_STOCK_CONCURRENCY = 8;
export const LIVE_ESIGNAL_NIGHT_SOCKET_URL = 'wss://esignal.co.kr/proxy/8888/socket.io/?EIO=4&transport=websocket';
export const EMA_ALPHA = 0.1;
export const SELECTED_CODE_QUERY_KEY = 'code';
export const PREFERRED_ADDITIONAL_DIVIDEND_OVERRIDES = {
  cj_4pref: 50,
};
export const TABLE_SORT_DEFAULT_DIRECTION = {
  name: 'asc',
  commonMarketCap: 'desc',
  preferredMarketCap: 'desc',
  preferredRatio: 'desc',
  divYieldGap: 'desc',
  spreadChange: 'desc',
  spread: 'desc',
};
export const TABLE_HEADER_CONFIG = [
  { key: 'name', label: '\uC885\uBAA9', sortable: true, numeric: false },
  { key: 'commonPrice', label: '\uBCF4\uD1B5\uC8FC', sortable: false, numeric: true },
  { key: 'preferredPrice', label: '\uC6B0\uC120\uC8FC', sortable: false, numeric: true },
  { key: 'commonMarketCap', label: '\uBCF4\uD1B5\uC8FC \uC2DC\uCD1D', sortable: true, numeric: true },
  { key: 'preferredMarketCap', label: '\uC6B0\uC120\uC8FC \uC2DC\uCD1D', sortable: true, numeric: true },
  { key: 'preferredRatio', label: '\uC6B0\uC120\uC8FC \uBE44\uC728', sortable: true, numeric: true },
  { key: 'divYieldGap', label: '\uBC30\uB2F9\uCC28', sortable: true, numeric: true },
  { key: 'spreadChange', label: '\uBCC0\uB3D9', sortable: true, numeric: true },
  { key: 'spread', label: '\uAD34\uB9AC\uC728', sortable: true, numeric: true },
];
export const MARKET_EXTRA_SPECS = [
  { id: 'KOSDAQ', name: 'KOSDAQ', unit: '', priceDecimals: 2 },
  { id: 'USDKRW', name: '환율', unit: '원', priceDecimals: 2 },
  { id: 'GOLD', name: '금가격 (COMEX)', unit: '', priceDecimals: 2 },
  { id: 'SP500', name: 'S&P500', unit: '', priceDecimals: 2 },
];
export const ZOOM_RANGE_MAX = 1000;
export const ZOOM_MIN_WINDOW = 30;
export const CARD_SORT_CONFIG = {
  spread: { direction: 'desc' },
  preferredYield: { direction: 'desc' },
  spreadWidening: { direction: 'desc' },
  spreadNarrowing: { direction: 'asc' },
};

// 모듈 간 공유 가변 상태. ES 모듈의 export let은 외부 모듈에서 재할당할 수 없으므로
// 단일 객체(app)의 속성으로 공유한다 (기존 IIFE 최상위 let들의 1:1 대응).
export const app = {
  STOCK_DATA: { lastUpdated: '', pairs: [] },
  pairs: [],
  currentTheme: document.documentElement.dataset.theme === 'light' ? 'light' : 'dark',
  selectedIdx: 0,
  periodDays: 0, // 0 = all
  zoomWindow: { start: 0, end: ZOOM_RANGE_MAX },
  latestMarketExtras: [],
  latestNightFutureMetric: null,
  cardSortMode: 'spread',
  pinnedPairIds: loadPinnedPairIds(),
  todayOverviewData: null,
  latestAppliedSnapshotMs: 0,
  liveRefreshPromise: null,
  currentPricesFetchPromise: null,
  autoRefreshTimer: null,
  lastCurrentPricesFetchStartedAt: 0,
  refreshButtonBusy: false,
  pairConfigMap: new Map(),
  indexWeightModalLastFocus: null,
  tableSortState: {
    key: 'name',
    direction: TABLE_SORT_DEFAULT_DIRECTION.name,
  },
  historyLoadedIds: new Set(),
  historyLoadPromises: new Map(),
  lastAppliedPriceByPairId: new Map(), // pair별 마지막 실시간 가격 {priceDate, point}
  dividendHistories: null,
  dividendLoadPromise: null,
  dividendRenderQueued: false, // 배당 최초 로드 완료 시 1회 재렌더용
};
app.pairs = app.STOCK_DATA.pairs;

export const configLoadPromise = loadPairConfig();

export async function loadSummaryData() {
  // 1순위: summary.json (소형이므로 no-store + 타임스탬프)
  try {
    const resp = await fetch(DATA_SUMMARY_URL + '?t=' + Date.now(), { cache: 'no-store' });
    if (resp.ok) {
      const summary = await resp.json();
      if (Array.isArray(summary?.pairs) && summary.pairs.length) {
        app.STOCK_DATA = {
          lastUpdated: summary.lastUpdated || '',
          pairs: summary.pairs.map(p => ({ ...p, history: [] })),
        };
        app.pairs = app.STOCK_DATA.pairs;
        return 'summary';
      }
    }
  } catch (e) {
    // summary.json 로드 실패 시 data.js 폴백으로 진행
  }
  await loadLegacyDataJs();
  return 'legacy';
}

export async function loadLegacyDataJs() {
  // data.js는 top-level const라 window 속성이 생기지 않으므로, 텍스트로 받아 직접 파싱한다.
  const resp = await fetch(LEGACY_DATA_JS_URL + '?t=' + Date.now(), { cache: 'no-store' });
  if (!resp.ok) {
    throw new Error('data.js 로드 실패 (' + resp.status + ')');
  }
  const text = await resp.text();
  const prefix = 'const STOCK_DATA = ';
  if (!text.startsWith(prefix)) {
    throw new Error('data.js 형식 오류');
  }
  let jsonText = text.slice(prefix.length).trimEnd();
  if (jsonText.endsWith(';')) jsonText = jsonText.slice(0, -1);
  const parsed = JSON.parse(jsonText);
  if (!Array.isArray(parsed?.pairs) || !parsed.pairs.length) {
    throw new Error('data.js 형식 오류');
  }
  app.STOCK_DATA = parsed;
  app.pairs = app.STOCK_DATA.pairs;
  app.pairs.forEach(p => app.historyLoadedIds.add(p.id)); // 레거시는 히스토리 내장
}

export function isHistoryLoaded(pair) {
  return !!pair && app.historyLoadedIds.has(pair.id);
}

export function buildHistoryRecords(payload) {
  const dates = Array.isArray(payload?.dates) ? payload.dates : [];
  const common = Array.isArray(payload?.common) ? payload.common : [];
  const preferred = Array.isArray(payload?.preferred) ? payload.preferred : [];
  const spread = Array.isArray(payload?.spread) ? payload.spread : [];
  const kospi = Array.isArray(payload?.kospi) ? payload.kospi : null;
  const length = Math.min(dates.length, common.length, preferred.length, spread.length);
  const records = [];
  for (let i = 0; i < length; i++) {
    const record = {
      date: dates[i],
      commonPrice: common[i],
      preferredPrice: preferred[i],
      spread: spread[i],
    };
    if (kospi && kospi[i] != null) record.kospiPrice = kospi[i];
    records.push(record);
  }
  return records;
}

export function ensureHistory(pair) {
  if (!pair || isHistoryLoaded(pair)) return Promise.resolve(pair?.history || []);
  if (app.historyLoadPromises.has(pair.id)) return app.historyLoadPromises.get(pair.id);
  const version = encodeURIComponent(app.STOCK_DATA.lastUpdated || '');
  const promise = fetch(`data/history/${encodeURIComponent(pair.id)}.json?v=${version}`)
    .then(resp => {
      if (!resp.ok) throw new Error(`history ${resp.status}`);
      return resp.json();
    })
    .then(payload => {
      pair.history = buildHistoryRecords(payload);
      app.historyLoadedIds.add(pair.id);
      const lastApplied = app.lastAppliedPriceByPairId.get(pair.id);
      if (lastApplied) upsertHistoryPoint(pair, lastApplied.priceDate, lastApplied.point);
      return pair.history;
    })
    .catch(e => {
      console.warn('히스토리 로드 실패:', pair.id, e);
      return pair.history || [];
    })
    .finally(() => app.historyLoadPromises.delete(pair.id));
  app.historyLoadPromises.set(pair.id, promise);
  return promise;
}

export function ensureDividends() {
  if (app.dividendHistories) return Promise.resolve(app.dividendHistories);
  if (app.dividendLoadPromise) return app.dividendLoadPromise;
  const version = encodeURIComponent(app.STOCK_DATA.lastUpdated || '');
  app.dividendLoadPromise = fetch(`${DATA_DIVIDENDS_URL}?v=${version}`)
    .then(resp => {
      if (!resp.ok) throw new Error(`dividends ${resp.status}`);
      return resp.json();
    })
    .then(payload => {
      app.dividendHistories = payload?.dividendHistories || {};
      return app.dividendHistories;
    })
    .catch(e => {
      console.warn('배당 이력 로드 실패:', e);
      app.dividendHistories = {}; // 실패 시 재시도하지 않는다.
      return app.dividendHistories;
    });
  return app.dividendLoadPromise;
}

export function getCardSortMetric(pair, mode = app.cardSortMode) {
  if (!pair?.current) return null;
  if (mode === 'preferredYield') return pair.current.preferredDivYield;
  if (mode === 'spreadWidening' || mode === 'spreadNarrowing') return pair.current.spreadChange;
  return pair.current.spread;
}

export function compareCardItems(a, b, mode = app.cardSortMode) {
  const direction = CARD_SORT_CONFIG[mode]?.direction || 'desc';
  const valueA = getCardSortMetric(a.pair, mode);
  const valueB = getCardSortMetric(b.pair, mode);
  const aMissing = valueA == null || Number.isNaN(valueA);
  const bMissing = valueB == null || Number.isNaN(valueB);
  if (aMissing && bMissing) return (a.pair.name || '').localeCompare(b.pair.name || '', 'ko');
  if (aMissing) return 1;
  if (bMissing) return -1;
  if (valueA !== valueB) {
    return direction === 'asc' ? valueA - valueB : valueB - valueA;
  }
  const spreadA = a.pair.current.spread ?? Number.NEGATIVE_INFINITY;
  const spreadB = b.pair.current.spread ?? Number.NEGATIVE_INFINITY;
  if (spreadB !== spreadA) return spreadB - spreadA;
  return (a.pair.name || '').localeCompare(b.pair.name || '', 'ko');
}

export function readStoredValue(key) {
  try {
    return localStorage.getItem(key);
  } catch (e) {
    return null;
  }
}

export function writeStoredValue(key, value) {
  try {
    localStorage.setItem(key, value);
  } catch (e) {
    // 저장 실패 시 현재 세션 설정만 적용한다.
  }
}

export function loadPinnedPairIds() {
  try {
    const parsed = JSON.parse(readStoredValue(PINNED_PAIRS_STORAGE_KEY) || '[]');
    return new Set(Array.isArray(parsed) ? parsed.filter(id => typeof id === 'string') : []);
  } catch (e) {
    return new Set();
  }
}

export function savePinnedPairIds() {
  writeStoredValue(PINNED_PAIRS_STORAGE_KEY, JSON.stringify([...app.pinnedPairIds]));
}

export function isGroupPinned(items) {
  return items.some(item => app.pinnedPairIds.has(item.pair.id));
}

export function getCardGroups() {
  const map = new Map();
  app.pairs.forEach((p, i) => {
    if (p.isAverage) {
      map.set('__avg__', [{ pair: p, idx: i }]);
      return;
    }
    const key = p.commonName;
    if (!map.has(key)) map.set(key, []);
    map.get(key).push({ pair: p, idx: i });
  });
  for (const items of map.values()) {
    items.sort((a, b) => compareCardItems(a, b));
  }
  const groups = [...map.values()];
  const averageGroup = groups.find(items => items[0]?.pair.isAverage) || null;
  const stockGroups = groups
    .filter(items => !items[0]?.pair.isAverage)
    .sort((a, b) => {
      // 관심종목 핀은 카드 표시 순서에만 영향 (그룹 구성/대표 선정은 그대로)
      const pinDiff = Number(isGroupPinned(b)) - Number(isGroupPinned(a));
      if (pinDiff !== 0) return pinDiff;
      return compareCardItems(a[0], b[0]);
    });

  return averageGroup ? [averageGroup, ...stockGroups] : stockGroups;
}

export function getRepresentativePairs() {
  return getCardGroups()
    .map(items => items[0].pair)
    .filter(pair => !pair.isAverage);
}

export function getAveragePair() {
  return app.pairs.find(pair => pair.isAverage);
}

export async function loadPairConfig() {
  try {
    const resp = await fetch('config.json?t=' + Date.now());
    if (!resp.ok) return app.pairConfigMap;
    const config = await resp.json();
    app.pairConfigMap = new Map(config.map(item => [item.id, item]));
  } catch (e) {
    // config.json 로드 실패 시 실시간 강제 갱신은 건너뛴다.
  }
  return app.pairConfigMap;
}

export function getGroupItemsByPairId(pairId) {
  return getCardGroups().find(items => items.some(item => item.pair.id === pairId)) || null;
}

export function getPrevDaySpread(pair, today) {
  const history = Array.isArray(pair.history) ? pair.history : [];
  for (let i = history.length - 1; i >= 0; i--) {
    if (history[i].date < today) return history[i].spread;
  }
  return null;
}

export function upsertHistoryPoint(pair, date, point) {
  const historyDate = normalizeDateText(date);
  if (!historyDate || !Array.isArray(pair.history)) return false;
  const existing = pair.history.find(entry => entry.date === historyDate);
  if (existing) {
    Object.assign(existing, point, { date: historyDate });
    return true;
  }

  const last = pair.history[pair.history.length - 1];
  if (!last || historyDate > last.date) {
    pair.history.push({ date: historyDate, ...point });
    return true;
  }
  return false;
}
