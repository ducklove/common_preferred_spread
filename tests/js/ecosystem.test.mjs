// node --test tests/js/*.test.mjs 로 실행. Value Compass 생태계 연동을 검증한다:
// (1) index.html 구조 계약 — theme-boot 마커, vc-tokens.css/vc-shell.js 로드 순서, <vc-shell> + fallback 허브 링크,
//     보유 배지 ?v=, 벤더링 파일 존재
// (2) 인라인 theme-boot 동작 — ?theme 우선(저장 안 함), 레거시 preferred-theme 이관, ?embed
// (3) 벤더링된 vc-shell.js — linkTo/setStock 동작
// (4) js/ecosystem.js — 선택 종목 setStock, holding_value 교차 링크(summary.json → config.json 폴백)
// (5) views.js 테마 토글 — VCShell.setTheme 위임, 미로드 시 로컬 저장 폴백
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { URL, fileURLToPath } from 'node:url';
import path from 'node:path';
import { JSDOM } from 'jsdom';

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const html = readFileSync(path.join(root, 'index.html'), 'utf8');
const shellSource = readFileSync(path.join(root, 'vc-shell.js'), 'utf8');
const head = html.slice(0, html.indexOf('</head>'));

// ---------------------------------------------------------------- (1) 구조
test('index.html: theme-boot 마커는 1회, 모든 stylesheet 보다 앞에 채워져 있다', () => {
  const blocks = html.match(/<!-- vc:theme-boot -->[\s\S]*?<!-- \/vc:theme-boot -->/g) || [];
  assert.equal(blocks.length, 1);
  assert.match(blocks[0], /<script>[\s\S]*vc-theme-boot v1[\s\S]*<\/script>/);
  assert.ok(head.indexOf('<!-- vc:theme-boot -->') < head.indexOf('rel="stylesheet"'));
  // 저장소 자체 pre-paint 스크립트는 공용 부트로 대체됐다
  assert.doesNotMatch(html, /localStorage\.getItem\('theme'\) \|\| localStorage\.getItem\('preferred-theme'\)/);
});

test('index.html: vc-tokens.css 는 app.css 앞, vc-shell.js 는 defer 로 모듈 엔트리보다 먼저 로드된다', () => {
  const tokens = head.indexOf('href="./vc-tokens.css?v=');
  const app = head.indexOf('href="css/app.css"');
  assert.ok(tokens > 0 && app > tokens, 'vc-tokens.css must precede css/app.css');
  assert.match(head, /<script defer src="\.\/vc-shell\.js\?v=[^"]+"><\/script>/);
  assert.ok(html.indexOf('vc-shell.js?v=') < html.indexOf('<script type="module" src="js/main.js">'));
});

test('index.html: body 최상단 <vc-shell> 에 fallback 허브 링크가 있고, 헤더의 수제 허브 링크는 없다', () => {
  const body = html.slice(html.indexOf('<body>') + '<body>'.length).trimStart();
  assert.match(body, /^<vc-shell tool="common_preferred_spread"><a class="hub-link" href="https:\/\/ducklove\.duckdns\.org:3691" rel="noopener">Value Compass ↗<\/a><\/vc-shell>/);
  assert.equal((html.match(/class="hub-link"/g) || []).length, 1);
  assert.match(html, /id="themeToggle"/);
  assert.match(html, /id="holdingValueLink"[^>]*hidden/);
});

test('index.html: 보유 배지 스크립트는 레지스트리 heldBadges.version 을 쓴다', () => {
  assert.match(html, /portfolio-held-badges\.js\?v=20260930-vc"/);
  assert.doesNotMatch(html, /20260928-tooltip/);
});

test('벤더링 파일이 저장소 루트(Pages 루트)에 있고 직접 수정 금지 헤더를 유지한다', () => {
  for (const name of ['vc-shell.js', 'vc-tokens.css', 'vc_publish.py']) {
    assert.ok(existsSync(path.join(root, name)), `${name} missing`);
  }
  assert.match(shellSource, /do not edit copies/);
  assert.match(readFileSync(path.join(root, 'vc-tokens.css'), 'utf8'), /do not edit copies/);
});

test('app.css: 방향색·본문 폰트는 생태계 토큰 alias 를 쓴다', () => {
  const css = readFileSync(path.join(root, 'css/app.css'), 'utf8');
  assert.equal((css.match(/--up: var\(--vc-up/g) || []).length, 2);
  assert.equal((css.match(/--down: var\(--vc-down/g) || []).length, 2);
  assert.match(css, /font-family: var\(--vc-font-sans/);
});

// ---------------------------------------------------------------- (2) theme-boot
function bootDom(url, seed = {}) {
  return new JSDOM(html.replace(/<script type="module"[^>]*><\/script>/, ''), {
    url,
    runScripts: 'dangerously',
    beforeParse(window) {
      for (const [k, v] of Object.entries(seed)) window.localStorage.setItem(k, v);
    },
  });
}

test('theme-boot: ?theme=dark 는 적용만 하고 저장하지 않는다', () => {
  const dom = bootDom('https://example.com/?theme=dark', { theme: 'light' });
  assert.equal(dom.window.document.documentElement.dataset.theme, 'dark');
  assert.equal(dom.window.localStorage.getItem('theme'), 'light');
  dom.window.close();
});

test('theme-boot: 레거시 preferred-theme 을 공용 theme 키로 이관한다', () => {
  const dom = bootDom('https://example.com/', { 'preferred-theme': 'dark' });
  assert.equal(dom.window.document.documentElement.dataset.theme, 'dark');
  assert.equal(dom.window.localStorage.getItem('theme'), 'dark');
  dom.window.close();
});

test('theme-boot: ?embed 는 data-embed 를 켜고 ?embed=0 은 끈다', () => {
  const on = bootDom('https://example.com/?embed=1');
  assert.ok(on.window.document.documentElement.hasAttribute('data-embed'));
  on.window.close();
  const off = bootDom('https://example.com/?embed=0');
  assert.ok(!off.window.document.documentElement.hasAttribute('data-embed'));
  off.window.close();
});

// ---------------------------------------------------------------- (3) vc-shell.js
function shellDom(url = 'https://ducklove.github.io/common_preferred_spread/') {
  const dom = new JSDOM(`<!doctype html><html data-theme="light"><body>
    <vc-shell tool="common_preferred_spread"><a class="hub-link" href="https://ducklove.duckdns.org:3691">Value Compass ↗</a></vc-shell>
  </body></html>`, { url, runScripts: 'outside-only' });
  dom.window.eval(shellSource);
  return dom;
}

test('vc-shell.js: holding_value 종목 딥링크와 허브 분석 링크를 레지스트리로 만든다', () => {
  const dom = shellDom();
  const shell = dom.window.VCShell;
  assert.ok(shell && typeof shell.setStock === 'function');
  const url = new URL(shell.linkTo('holding_value', { code: '003550' }));
  assert.equal(url.origin + url.pathname, 'https://ducklove.github.io/holding_value/');
  assert.equal(url.searchParams.get('code'), '003550');
  assert.equal(url.searchParams.get('theme'), 'light');
  assert.equal(url.searchParams.get('from'), 'common_preferred_spread');
  assert.match(shell.hubAnalysisUrl('005935'), /^https:\/\/ducklove\.duckdns\.org:3691\/.*005935/);
  assert.equal(shell.hubAnalysisUrl('BRK.B'), null);
  shell.setStock('005935', '삼성전자우');
  const el = dom.window.document.querySelector('vc-shell');
  assert.equal(el.getAttribute('stock'), '005935');
  assert.equal(el.getAttribute('stock-name'), '삼성전자우');
  shell.setStock(null);
  assert.ok(!el.hasAttribute('stock'));
  dom.window.close();
});

// ---------------------------------------------------------------- (4) ecosystem.js + (5) 테마 토글
const dom = new JSDOM(`<!doctype html><html lang="ko" data-theme="light"><body>
  <button type="button" id="themeToggle"></button>
  <a class="zoom-reset-btn ecosystem-link" id="holdingValueLink" hidden>지주사 지분가치 ↗</a>
</body></html>`, { url: 'https://ducklove.github.io/common_preferred_spread/' });

// 셤 경로(한 프로세스에서 여러 파일 실행)에서는 다른 파일이 전역을 바꿔 둘 수 있어 테스트마다 재설치한다.
function installDom() {
  globalThis.window = dom.window;
  globalThis.document = dom.window.document;
  globalThis.localStorage = dom.window.localStorage;
  globalThis.requestAnimationFrame = (cb) => cb();
}
installDom();

// 테스트 본문에서 쓰는 로컬 별칭 (eslint tests/js 설정에는 브라우저 전역이 없다).
const { document, localStorage } = dom.window;

const { app } = await import('../../js/state.js');
const {
  extractHoldingCodes,
  getPairEcosystemCodes,
  loadHoldingCodes,
  resetEcosystemCache,
  syncEcosystemSelection,
} = await import('../../js/ecosystem.js');
const { toggleTheme, handleEcosystemThemeChange } = await import('../../js/views.js');

const CONFIG = [
  { id: 'samsung_elec', name: '삼성전자', commonTicker: '005930.KS', preferredTicker: '005935.KS', commonName: '삼성전자', preferredName: '삼성전자우' },
  { id: 'lg', name: 'LG', commonTicker: '003550.KS', preferredTicker: '003555.KS', commonName: 'LG', preferredName: 'LG우' },
];

function fakeShell() {
  const calls = { setStock: [], setTheme: [] };
  return {
    calls,
    tools: [{ id: 'holding_value', url: 'https://ducklove.github.io/holding_value' }],
    setStock(code, name) { calls.setStock.push([code, name]); },
    setTheme(t) { calls.setTheme.push(t); },
    linkTo(id, vars) { return `https://ducklove.github.io/${id}/?code=${vars.code}&from=common_preferred_spread`; },
  };
}

function fakeFetch(responses) {
  const seen = [];
  const impl = async (url) => {
    seen.push(url);
    const body = responses[url];
    if (body === undefined) return { ok: false, status: 404, json: async () => ({}) };
    return { ok: true, status: 200, json: async () => body };
  };
  return { impl, seen };
}

function selectPairById(id) {
  app.pairs = [
    { id: '_average', name: '평균', isAverage: true, current: {} },
    ...CONFIG.map(c => ({ id: c.id, name: c.preferredName, preferredName: c.preferredName, commonName: c.commonName, current: {} })),
  ];
  app.pairConfigMap = new Map(CONFIG.map(c => [c.id, c]));
  app.selectedIdx = Math.max(0, app.pairs.findIndex(p => p.id === id));
}

test('getPairEcosystemCodes: 우선주 코드를 stock 으로, 평균/설정 없음은 null', () => {
  selectPairById('lg');
  assert.deepEqual(getPairEcosystemCodes(app.pairs[2], CONFIG[1]), {
    stockCode: '003555', commonCode: '003550', preferredCode: '003555', name: 'LG우', commonName: 'LG',
  });
  assert.equal(getPairEcosystemCodes(app.pairs[0], null), null);
  assert.equal(getPairEcosystemCodes(app.pairs[1], undefined), null);
});

test('extractHoldingCodes: summary.json envelope 와 레거시 config.json 배열을 모두 읽는다', () => {
  assert.deepEqual([...extractHoldingCodes({ data: { pairs: [{ code: '003550' }, { code: 'bad' }] } })], ['003550']);
  assert.deepEqual([...extractHoldingCodes([{ holdingTicker: '034730.KS' }, { holdingTicker: null }])], ['034730']);
  assert.equal(extractHoldingCodes(null).size, 0);
});

test('loadHoldingCodes: summary.json 404 이면 config.json 으로 폴백하고 결과를 1회만 조회한다', async () => {
  resetEcosystemCache();
  const { impl, seen } = fakeFetch({
    'https://ducklove.github.io/holding_value/config.json': [{ holdingTicker: '003550.KS' }],
  });
  const shell = fakeShell();
  const codes = await loadHoldingCodes({ shell, fetchImpl: impl });
  assert.deepEqual([...codes], ['003550']);
  await loadHoldingCodes({ shell, fetchImpl: impl });
  assert.deepEqual(seen, [
    'https://ducklove.github.io/holding_value/summary.json',
    'https://ducklove.github.io/holding_value/config.json',
  ]);
  resetEcosystemCache();
});

test('syncEcosystemSelection: 지주사 페어는 setStock + holding_value 교차 링크 노출, 비지주사는 링크 숨김', async () => {
  installDom();
  resetEcosystemCache();
  const shell = fakeShell();
  const { impl } = fakeFetch({
    'https://ducklove.github.io/holding_value/summary.json': { data: { pairs: [{ code: '003550' }] } },
  });
  const link = document.getElementById('holdingValueLink');

  selectPairById('lg');
  assert.equal(await syncEcosystemSelection({ win: { VCShell: shell }, doc: document, fetchImpl: impl }), true);
  assert.deepEqual(shell.calls.setStock.at(-1), ['003555', 'LG우']);
  assert.equal(link.hidden, false);
  assert.match(link.getAttribute('href'), /holding_value\/\?code=003550/);
  assert.equal(link.title, '지주사 지분가치 대시보드에서 LG 보통주(003550) 보기');

  selectPairById('samsung_elec');
  assert.equal(await syncEcosystemSelection({ win: { VCShell: shell }, doc: document, fetchImpl: impl }), false);
  assert.deepEqual(shell.calls.setStock.at(-1), ['005935', '삼성전자우']);
  assert.equal(link.hidden, true);
  assert.equal(link.hasAttribute('href'), false);

  selectPairById('_average');
  await syncEcosystemSelection({ win: { VCShell: shell }, doc: document, fetchImpl: impl });
  assert.deepEqual(shell.calls.setStock.at(-1), [null, null]);
  assert.equal(link.hidden, true);
  resetEcosystemCache();
});

test('syncEcosystemSelection: VCShell 이 없으면 네트워크 없이 링크만 숨긴다', async () => {
  installDom();
  resetEcosystemCache();
  const { impl, seen } = fakeFetch({});
  selectPairById('lg');
  assert.equal(await syncEcosystemSelection({ win: {}, doc: document, fetchImpl: impl }), false);
  assert.equal(document.getElementById('holdingValueLink').hidden, true);
  assert.deepEqual(seen, []);
});

test('toggleTheme: VCShell 이 있으면 setTheme 에 위임하고, 없으면 공용 theme 키에 저장한다', () => {
  installDom();
  app.pairs = []; // 데이터 로드 전: 재렌더 없이 테마만 적용
  app.currentTheme = 'light';
  document.documentElement.dataset.theme = 'light';
  localStorage.clear();

  const shell = fakeShell();
  toggleTheme({ VCShell: shell });
  assert.deepEqual(shell.calls.setTheme, ['dark']);
  assert.equal(app.currentTheme, 'dark');
  assert.equal(localStorage.getItem('theme'), null, 'persistence belongs to VCShell.setTheme');

  toggleTheme({});
  assert.equal(app.currentTheme, 'light');
  assert.equal(document.documentElement.dataset.theme, 'light');
  assert.equal(localStorage.getItem('theme'), 'light');
});

test('handleEcosystemThemeChange: vc:themechange 의 테마를 저장 없이 반영한다', () => {
  installDom();
  app.pairs = [];
  app.currentTheme = 'light';
  localStorage.clear();
  handleEcosystemThemeChange({ detail: { theme: 'dark' } });
  assert.equal(app.currentTheme, 'dark');
  assert.equal(document.documentElement.dataset.theme, 'dark');
  assert.equal(document.getElementById('themeToggle').getAttribute('aria-label'), '일반 모드로 전환');
  assert.equal(localStorage.getItem('theme'), null);
});
