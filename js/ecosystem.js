// js/ecosystem.js — Value Compass 생태계 연동 (vendored ./vc-shell.js 가 만드는 전역 VCShell)
// - 선택 종목 → VCShell.setStock(code, name): 생태계 바에 '허브에서 분석 ↗' 칩 표시 (평균/미선택은 해제)
// - 선택 종목의 보통주가 holding_value(지주사 지분가치)에서 추적하는 지주사면 교차 링크(#holdingValueLink) 노출.
//   지주사 목록은 holding_value 가 발행하는 summary.json(생태계 데이터 계약 v1) → 없으면 config.json 에서
//   1회만 읽는다(같은 github.io origin, 실패 시 링크만 숨김). URL은 VCShell 레지스트리(tools[].url)에서 만든다.
// VCShell 이 없으면(스크립트 차단·로드 실패) 모든 동작이 조용히 no-op 이다.
import { app } from './state.js';
import { getTickerCode, normalizeTickerCode } from './format.js';

export const HOLDING_TOOL_ID = 'holding_value';
export const HOLDING_LINK_ID = 'holdingValueLink';
const HUB_CODE_RE = /^[0-9A-Z]{6}$/;

let holdingCodesPromise = null;

export function getShell(win = globalThis.window) {
  const shell = win && win.VCShell;
  return shell && typeof shell === 'object' ? shell : null;
}

function toHubCode(ticker) {
  const code = normalizeTickerCode(getTickerCode(ticker));
  return HUB_CODE_RE.test(code) ? code : '';
}

// 선택 페어 → 생태계 코드. stockCode 는 이 대시보드의 ?code= 규칙과 같게 우선주 코드(없으면 보통주).
export function getPairEcosystemCodes(pair, config) {
  if (!pair || pair.isAverage || !config) return null;
  const preferredCode = toHubCode(config.preferredTicker);
  const commonCode = toHubCode(config.commonTicker);
  const stockCode = preferredCode || commonCode;
  if (!stockCode) return null;
  const name = preferredCode
    ? (config.preferredName || pair.preferredName || pair.name || '')
    : (config.commonName || pair.commonName || pair.name || '');
  return { stockCode, commonCode, preferredCode, name: String(name).trim() };
}

// holding_value 발행물에서 지주사 종목코드 집합을 만든다.
// summary.json envelope({data:{pairs:[{code}]}}) 과 레거시 config.json 배열([{holdingTicker}]) 모두 지원.
export function extractHoldingCodes(payload) {
  const codes = new Set();
  const rows = Array.isArray(payload) ? payload : payload?.data?.pairs;
  if (!Array.isArray(rows)) return codes;
  rows.forEach(row => {
    const code = toHubCode(row?.code ?? row?.holdingTicker);
    if (code) codes.add(code);
  });
  return codes;
}

export function getToolBaseUrl(shell, toolId) {
  const tools = Array.isArray(shell?.tools) ? shell.tools : [];
  const tool = tools.find(t => t && t.id === toolId);
  if (!tool || typeof tool.url !== 'string' || !/^https?:\/\//.test(tool.url)) return '';
  return tool.url.replace(/\/+$/, '');
}

async function fetchJson(fetchImpl, url) {
  const resp = await fetchImpl(url, { cache: 'default' });
  if (!resp || !resp.ok) throw new Error(`HTTP ${resp ? resp.status : '?'} ${url}`);
  return resp.json();
}

// 페이지당 1회만 조회한다 (실패도 빈 집합으로 캐시 — 재시도 폭주 방지).
export function loadHoldingCodes({ shell = getShell(), fetchImpl = globalThis.fetch } = {}) {
  if (holdingCodesPromise) return holdingCodesPromise;
  const base = getToolBaseUrl(shell, HOLDING_TOOL_ID);
  if (!base || typeof fetchImpl !== 'function') return Promise.resolve(new Set());
  holdingCodesPromise = (async () => {
    for (const file of ['summary.json', 'config.json']) {
      try {
        const codes = extractHoldingCodes(await fetchJson(fetchImpl, `${base}/${file}`));
        if (codes.size) return codes;
      } catch (e) {
        // 다음 후보(레거시 config.json)로 폴백
      }
    }
    return new Set();
  })();
  return holdingCodesPromise;
}

export function resetEcosystemCache() {
  holdingCodesPromise = null;
}

export function updateHoldingValueLink(link, { codes, selection, shell }) {
  if (!link) return false;
  const commonCode = selection?.commonCode;
  const href = commonCode && codes?.has(commonCode) && typeof shell?.linkTo === 'function'
    ? shell.linkTo(HOLDING_TOOL_ID, { code: commonCode })
    : null;
  if (!href) {
    link.hidden = true;
    link.removeAttribute('href');
    return false;
  }
  link.href = href;
  link.hidden = false;
  const name = selection.name ? `${selection.name} ` : '';
  link.title = `지주사 지분가치 대시보드에서 ${name}보통주(${commonCode}) 보기`;
  return true;
}

// 선택 종목이 바뀌거나(테마 포함) 초기화될 때 호출한다. 반환 Promise 는 교차 링크 갱신 완료 시점.
export function syncEcosystemSelection({
  win = globalThis.window,
  doc = globalThis.document,
  fetchImpl = globalThis.fetch,
} = {}) {
  const shell = getShell(win);
  const pair = app.pairs[app.selectedIdx];
  const config = pair && !pair.isAverage ? app.pairConfigMap.get(pair.id) : null;
  const selection = getPairEcosystemCodes(pair, config);

  if (shell && typeof shell.setStock === 'function') {
    shell.setStock(selection ? selection.stockCode : null, selection ? selection.name || null : null);
  }

  const link = typeof doc?.getElementById === 'function' ? doc.getElementById(HOLDING_LINK_ID) : null;
  if (!link) return Promise.resolve(false);
  if (!shell || !selection?.commonCode) {
    updateHoldingValueLink(link, { codes: null, selection: null, shell });
    return Promise.resolve(false);
  }
  const selectedIdx = app.selectedIdx;
  return loadHoldingCodes({ shell, fetchImpl }).then(codes => {
    if (app.selectedIdx !== selectedIdx) return false; // 조회 중 다른 종목이 선택됨
    return updateHoldingValueLink(link, { codes, selection, shell });
  });
}
