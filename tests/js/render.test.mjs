// node --test tests/js/ 로 실행. views.js의 DOM 렌더 경로를 jsdom 문서에서 실제로 돌려
// 검증한다: 카드/테이블 렌더 행 수, 정렬 반영, XSS 이스케이프, 즐겨찾기(핀) 토글 클래스,
// 지수 비중 모달 열림/닫힘, 오늘의 현황 오버뷰, 통계 박스, 빈 데이터 상태.
// state.js가 모듈 최상위에서 document/localStorage에 접근하므로 jsdom 전역을 먼저 심고
// 동적 import 한다 (views.test.mjs와 동일한 로드 순서 규칙).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';

const dom = new JSDOM(`<!doctype html>
<html lang="ko" data-theme="light">
<body>
  <button type="button" id="outsideFocusBtn">바깥 포커스</button>
  <div class="today-overview" id="todayOverview"></div>
  <div class="cards" id="cards"></div>
  <div class="table-section">
    <table>
      <thead><tr></tr></thead>
      <tbody id="tableBody"></tbody>
    </table>
  </div>
  <div class="stats-row" id="statsRow"></div>
  <div class="index-modal-backdrop" id="indexWeightModal" hidden>
    <div class="index-modal-panel" role="dialog" aria-modal="true">
      <button type="button" class="index-modal-close" id="indexWeightModalClose" aria-label="닫기">×</button>
      <div class="index-modal-summary" id="indexWeightSummary"></div>
      <div class="index-modal-note" id="indexWeightNote"></div>
      <table class="index-weight-table"><tbody id="indexWeightTableBody"></tbody></table>
    </div>
  </div>
</body>
</html>`, { url: 'https://example.com/' });

// CI 가 테스트 파일들을 한 프로세스에서 돌리는 경우(29147… 실패), 알파벳상
// 늦게 import 되는 스텁 기반 테스트 파일이 전역 document 를 덮어쓴 채로
// 이 파일의 테스트가 "실행"될 수 있다. import 시점 1회 설치로는 부족하므로
// 각 테스트 시작 시 jsdom 전역을 재설치한다 (installDom — 실행 순서 무관).
function installDom() {
  globalThis.window = dom.window;
  globalThis.document = dom.window.document;
  globalThis.localStorage = dom.window.localStorage;
  globalThis.requestAnimationFrame = (cb) => cb();
}
installDom();

// 테스트 본문에서 쓰는 로컬 별칭 (eslint tests/js 설정에는 브라우저 전역이 없다).
const { document, localStorage } = dom.window;

const { app, PINNED_PAIRS_STORAGE_KEY, TABLE_HEADER_CONFIG } = await import('../../js/state.js');
const {
  renderCards,
  renderTable,
  renderTableHeaders,
  bindTableSortHeaders,
  renderTodayOverview,
  renderStats,
  renderIndexWeightModalContent,
  openIndexWeightModal,
  closeIndexWeightModal,
} = await import('../../js/views.js');

function makeCurrent(overrides = {}) {
  return {
    spread: 30,
    spreadChange: 0.5,
    commonPrice: 50000,
    commonChange: 1.0,
    preferredPrice: 35000,
    preferredChange: -0.5,
    commonDivYield: 1.5,
    preferredDivYield: 3.0,
    commonSharesOutstanding: 1_000_000,
    preferredSharesOutstanding: 200_000,
    commonAvgTradedValue20: 5e9,
    preferredAvgTradedValue20: 1e9,
    ...overrides,
  };
}

function makePair(id, name, commonName, currentOverrides = {}, extra = {}) {
  return {
    id,
    name,
    commonName,
    preferredName: name,
    current: makeCurrent(currentOverrides),
    history: [],
    ...extra,
  };
}

// 평균 1 + 알파(우선주 2종 그룹) + 베타 + 감마 = 카드 4장 / 테이블 4행 구성.
function resetState() {
  app.pairs = [
    { id: '__avg__', name: '평균', isAverage: true, current: { spread: 30, spreadChange: 0.5 }, history: [] },
    makePair('alpha', '알파우', '알파', { spread: 40, spreadChange: 1.2 }),
    makePair('alpha2', '알파/2우B', '알파', { spread: 35, spreadChange: 1.5 }),
    makePair('beta', '베타우', '베타', { spread: 20, spreadChange: -0.8 }),
    makePair('gamma', '감마우', '감마', { spread: 30, spreadChange: 0.1 }),
  ];
  app.STOCK_DATA = { lastUpdated: '', pairs: app.pairs };
  app.selectedIdx = 1; // 알파우
  app.cardSortMode = 'spread';
  app.pinnedPairIds = new Set();
  app.tableSortState = { key: 'spread', direction: 'desc' };
  app.todayOverviewData = null;
  app.dividendHistories = null;
  localStorage.clear();
  document.getElementById('todayOverview').innerHTML = '';
  document.getElementById('cards').innerHTML = '';
  document.getElementById('tableBody').innerHTML = '';
  document.getElementById('statsRow').innerHTML = '';
}

function cardNames() {
  return [...document.querySelectorAll('#cards .card .name')]
    .map(el => el.textContent.replace(/\s+/g, ' ').trim());
}

function tableFirstCells() {
  return [...document.querySelectorAll('#tableBody tr td:first-child')]
    .map(el => el.textContent.replace(/\s+/g, ' ').trim());
}

test('renderCards: 그룹 수만큼 카드 렌더 — 평균 카드 선두 + 그룹은 보통주명으로 병합', () => {
  installDom();
  resetState();
  renderCards();
  const cards = document.querySelectorAll('#cards .card');
  assert.equal(cards.length, 4); // 평균 + 3개 그룹 (알파 2종은 1장으로 병합)
  assert.ok(cards[0].classList.contains('avg-card'));
  assert.equal(cards[0].querySelector('.name').textContent, '평균');
  const names = cardNames();
  assert.ok(names.includes('알파')); // 복수 우선주 그룹은 보통주명 표시
  assert.ok(names.includes('베타우'));
  // 핀 버튼은 평균 카드를 제외한 종목 카드에만 붙는다.
  assert.equal(document.querySelectorAll('#cards .card-pin').length, 3);
  assert.equal(cards[0].querySelector('.card-pin'), null);
});

test('renderCards: 정렬 모드 반영 — spread 내림차순 ↔ spreadNarrowing 오름차순', () => {
  installDom();
  resetState();
  renderCards();
  // spread desc: 알파(40) → 감마(30) → 베타(20)
  assert.deepEqual(cardNames().slice(1), ['알파', '감마우', '베타우']);

  app.cardSortMode = 'spreadNarrowing'; // spreadChange asc
  renderCards();
  // 베타(-0.8) → 감마(0.1) → 알파(대표 1.2)
  assert.deepEqual(cardNames().slice(1), ['베타우', '감마우', '알파']);
});

test('renderCards: 선택 그룹에 active 클래스, data-idx 는 대표 종목 인덱스', () => {
  installDom();
  resetState();
  app.selectedIdx = 2; // 알파/2우B — 같은 그룹이므로 알파 카드가 active
  renderCards();
  const activeCards = document.querySelectorAll('#cards .card.active');
  assert.equal(activeCards.length, 1);
  assert.equal(activeCards[0].querySelector('.name').textContent.trim(), '알파');
  assert.equal(activeCards[0].dataset.idx, '1'); // 대표(스프레드 40)는 idx 1
});

test('renderCards/renderTable: 악의 문자열은 이스케이프 — img/script 요소가 생기지 않는다', () => {
  installDom();
  resetState();
  const evilName = '<img src=x onerror=window.__xss1=1>악성우';
  const evilCommon = '<script>window.__xss2=1</script>악성';
  app.pairs.push(makePair('evil', evilName, evilCommon));
  renderCards();
  renderTable();

  assert.equal(document.querySelector('#cards img, #cards script'), null);
  assert.equal(document.querySelector('#tableBody img, #tableBody script'), null);
  // 원문 텍스트는 그대로 노출된다 (텍스트 노드로만).
  assert.ok(document.getElementById('cards').textContent.includes(evilName));
  assert.ok(document.getElementById('tableBody').textContent.includes(evilName));
  assert.equal(dom.window.__xss1, undefined);
  assert.equal(dom.window.__xss2, undefined);
});

test('핀 토글: .card-pin 클릭으로 pinned 클래스/aria-pressed/저장/카드 순서가 갱신된다', () => {
  installDom();
  resetState();
  renderCards();
  const betaIdx = app.pairs.findIndex(p => p.id === 'beta'); // 3
  const pinButton = document.querySelector(`#cards .card-pin[data-pin-idx="${betaIdx}"]`);
  assert.ok(pinButton);
  assert.equal(pinButton.classList.contains('pinned'), false);
  assert.equal(pinButton.getAttribute('aria-pressed'), 'false');

  pinButton.click(); // togglePinnedGroup → savePinnedPairIds → renderCards
  assert.ok(app.pinnedPairIds.has('beta'));
  assert.deepEqual(JSON.parse(localStorage.getItem(PINNED_PAIRS_STORAGE_KEY)), ['beta']);
  const pinned = document.querySelector(`#cards .card-pin[data-pin-idx="${betaIdx}"]`);
  assert.ok(pinned.classList.contains('pinned'));
  assert.equal(pinned.getAttribute('aria-pressed'), 'true');
  assert.equal(pinned.textContent, '★');
  // 핀 그룹은 (평균 카드 다음) 종목 카드 선두로 온다.
  assert.deepEqual(cardNames().slice(1), ['베타우', '알파', '감마우']);

  document.querySelector(`#cards .card-pin[data-pin-idx="${betaIdx}"]`).click(); // 해제
  assert.equal(app.pinnedPairIds.size, 0);
  assert.deepEqual(JSON.parse(localStorage.getItem(PINNED_PAIRS_STORAGE_KEY)), []);
  assert.equal(
    document.querySelector(`#cards .card-pin[data-pin-idx="${betaIdx}"]`).classList.contains('pinned'),
    false,
  );
  assert.deepEqual(cardNames().slice(1), ['알파', '감마우', '베타우']);
});

test('renderTable: 평균 쌍 제외 행 수 + 정렬 방향에 따른 행 순서 + 선택 행 표시', () => {
  installDom();
  resetState();
  renderTable();
  const rows = document.querySelectorAll('#tableBody tr');
  assert.equal(rows.length, 4); // 평균 제외 4종
  // spread desc: 알파우(40) → 알파/2우B(35) → 감마우(30) → 베타우(20)
  assert.deepEqual(tableFirstCells(), ['알파우', '알파/2우B', '감마우', '베타우']);
  assert.equal(document.querySelectorAll('#tableBody tr.selected-row').length, 1);
  assert.ok(rows[0].classList.contains('selected-row')); // selectedIdx=1 알파우

  app.tableSortState = { key: 'spread', direction: 'asc' };
  renderTable();
  assert.deepEqual(tableFirstCells(), ['베타우', '감마우', '알파/2우B', '알파우']);
});

test('renderTableHeaders: TABLE_HEADER_CONFIG 그대로 재생성 + 정렬 상태 클래스/aria-sort', () => {
  installDom();
  resetState();
  renderTableHeaders();
  const headers = document.querySelectorAll('.table-section thead th');
  assert.equal(headers.length, TABLE_HEADER_CONFIG.length);
  assert.deepEqual(
    [...headers].map(th => th.textContent),
    TABLE_HEADER_CONFIG.map(config => config.label),
  );
  const spreadTh = document.querySelector('.table-section thead th[data-sort-key="spread"]');
  assert.ok(spreadTh.classList.contains('sorted-desc'));
  assert.equal(spreadTh.getAttribute('aria-sort'), 'descending');
  assert.equal(spreadTh.getAttribute('tabindex'), '0');
  // 정렬 불가 컬럼은 sort-key 빈 값 + 포커스 제외.
  const priceTh = [...headers].find(th => th.textContent === '보통주');
  assert.equal(priceTh.dataset.sortKey, '');
  assert.equal(priceTh.getAttribute('tabindex'), '-1');
});

test('테이블 헤더 클릭: 새 키는 기본 방향으로, 같은 키 재클릭은 방향 반전 + 재렌더', () => {
  installDom();
  resetState();
  app.tableSortState = { key: 'name', direction: 'asc' };
  renderTable();
  bindTableSortHeaders();

  document.querySelector('.table-section thead th[data-sort-key="spread"]').click();
  assert.deepEqual(app.tableSortState, { key: 'spread', direction: 'desc' });
  assert.equal(tableFirstCells()[0], '알파우'); // 40이 첫 행

  document.querySelector('.table-section thead th[data-sort-key="spread"]').click();
  assert.deepEqual(app.tableSortState, { key: 'spread', direction: 'asc' });
  assert.equal(tableFirstCells()[0], '베타우'); // 20이 첫 행
});

test('renderTodayOverview: 요약 데이터로 오버뷰 카드 4장(지수/시장/확대/축소) 렌더', () => {
  installDom();
  resetState();
  app.todayOverviewData = {
    averageSpread: 32.5,
    averageSpreadChange: 0.4,
    averagePreferredChange: 1.1,
    averageCommonChange: 0.6,
    simpleAverageSpread: 31.0,
    representativeCount: 3,
    market: { id: 'KOSPI', name: '코스피', price: 2600.12, change: 10.2, changePct: 0.4 },
    topWidening: { id: 'alpha', name: '알파우', spread: 40, spreadChange: 1.2 },
    topWideningRunners: [{ id: 'gamma', name: '감마우', spread: 30, spreadChange: 0.1 }],
    topNarrowing: { id: 'beta', name: '베타우', spread: 20, spreadChange: -0.8 },
    topNarrowingRunners: [],
  };
  renderTodayOverview();

  const overview = document.getElementById('todayOverview');
  assert.equal(overview.querySelectorAll('.overview-card').length, 4);
  assert.match(overview.textContent, /오늘의 우선주 현황/);
  assert.match(overview.textContent, /32\.50%/); // 괴리율 지수
  assert.match(overview.textContent, /2,600\.12/); // KOSPI 지수
  assert.match(overview.textContent, /최고 괴리율 확대/);
  assert.match(overview.textContent, /최고 괴리율 축소/);
  // 리더 카드는 클릭 가능한 data-idx 카드, 러너는 순위 버튼으로 렌더.
  assert.ok(overview.querySelector('.overview-card.clickable[data-idx]'));
  const runner = overview.querySelector('.overview-rank-item[data-idx]');
  assert.ok(runner);
  assert.match(runner.textContent, /감마우/);
});

test('renderTodayOverview: 빈 데이터(요약 없음/종목 없음)면 오버뷰를 비운다', () => {
  installDom();
  resetState();
  const overview = document.getElementById('todayOverview');
  overview.innerHTML = '<div>이전 렌더 잔여물</div>';
  app.todayOverviewData = { averageSpread: null };
  renderTodayOverview();
  assert.equal(overview.innerHTML, '');

  overview.innerHTML = '<div>이전 렌더 잔여물</div>';
  app.todayOverviewData = null;
  app.pairs = []; // buildTodaySummaryFromPairs → null
  renderTodayOverview();
  assert.equal(overview.innerHTML, '');
});

test('지수 비중 모달: 열림 — hidden 해제/modal-open/닫기 버튼 포커스/비중 테이블 렌더', () => {
  installDom();
  resetState();
  const outside = document.getElementById('outsideFocusBtn');
  outside.focus();
  openIndexWeightModal();

  const modal = document.getElementById('indexWeightModal');
  assert.equal(modal.hidden, false);
  assert.ok(document.body.classList.contains('modal-open'));
  assert.equal(document.activeElement, document.getElementById('indexWeightModalClose'));
  // 발행사 단위 병합: 알파(2종 합산)/베타/감마 = 3행.
  assert.equal(document.querySelectorAll('#indexWeightTableBody tr').length, 3);
  assert.equal(document.querySelectorAll('#indexWeightSummary .index-modal-metric').length, 4);
  assert.match(document.getElementById('indexWeightSummary').textContent, /괴리율 지수/);
  assert.match(document.getElementById('indexWeightNote').textContent, /제곱근/);
  // 복수 우선주 발행사 행은 구성 종목별 발행사 내 비중을 병기한다.
  assert.match(document.getElementById('indexWeightTableBody').textContent, /알파우 \d+(\.\d+)?% · 알파\/2우B \d+(\.\d+)?%/);
});

test('지수 비중 모달: 닫힘 — hidden 복원/modal-open 제거/이전 포커스 복원', () => {
  installDom();
  resetState();
  const outside = document.getElementById('outsideFocusBtn');
  outside.focus();
  openIndexWeightModal();
  closeIndexWeightModal();

  assert.equal(document.getElementById('indexWeightModal').hidden, true);
  assert.equal(document.body.classList.contains('modal-open'), false);
  assert.equal(document.activeElement, outside);
  assert.equal(app.indexWeightModalLastFocus, null);
  // 이미 닫힌 상태에서 재호출해도 안전하다.
  closeIndexWeightModal();
  assert.equal(document.getElementById('indexWeightModal').hidden, true);
});

test('지수 비중 모달: 종목이 없으면 빈 상태 문구를 렌더한다', () => {
  installDom();
  resetState();
  app.pairs = [];
  renderIndexWeightModalContent();
  const body = document.getElementById('indexWeightTableBody');
  assert.ok(body.querySelector('td.index-weight-empty'));
  assert.match(body.textContent, /비중을 계산할 수 없습니다/);
  assert.equal(document.getElementById('indexWeightSummary').innerHTML, '');
  assert.equal(document.getElementById('indexWeightNote').textContent, '');
});

test('renderStats: 선택 종목 통계 박스 렌더 — 괴리율/가격/시총/배당 + 최근 배당 이력', () => {
  installDom();
  resetState();
  app.selectedIdx = 1; // 알파우
  app.dividendHistories = {
    alpha: {
      common: [{ date: '2026-04-01', amount: 361 }],
      preferred: [{ date: '2026-04-01', amount: 362 }],
    },
  };
  renderStats();

  const statsEl = document.getElementById('statsRow');
  const boxes = statsEl.querySelectorAll('.stat-box');
  assert.ok(boxes.length >= 10, `stat-box ${boxes.length}개`);
  assert.match(statsEl.textContent, /괴리율/);
  assert.match(statsEl.textContent, /40\.00%/); // 현재 괴리율
  assert.match(statsEl.textContent, /35,000/); // 우선주 현재가
  assert.match(statsEl.textContent, /배당수익률/);
  assert.match(statsEl.textContent, /최근 배당/);
  assert.match(statsEl.textContent, /362 \/ 361/); // 우선주/보통주 배당 이력
});

test('renderStats: 평균 쌍 선택 시 종목 전용 박스(상장일/최근 배당)는 생략된다', () => {
  installDom();
  resetState();
  app.selectedIdx = 0; // 평균
  renderStats();
  const statsEl = document.getElementById('statsRow');
  assert.ok(statsEl.querySelectorAll('.stat-box').length >= 8);
  assert.doesNotMatch(statsEl.textContent, /상장일/);
  assert.doesNotMatch(statsEl.textContent, /최근 배당/);
  assert.match(statsEl.textContent, /괴리율/);
});

// --- 거래정지(데이터 정체) 배지 ---
// 전체 최신일(2026-08-20)보다 크게 뒤처진 종목만 카드/테이블에 기준일 배지가 붙는지 검증한다.
function applyStaleFixture() {
  app.pairs.forEach(pair => { pair.lastHistoryDate = '2026-08-20'; });
  app.pairs.find(p => p.id === 'beta').lastHistoryDate = '2026-07-29'; // 22일 정체
  app.pairs.find(p => p.id === 'gamma').lastHistoryDate = '2026-08-13'; // 7일 정체
}

function staleBadgeTextsIn(containerId) {
  return [...document.querySelectorAll(`#${containerId} .stale-badge`)]
    .map(el => el.textContent.trim());
}

test('renderCards: 정체 종목 카드에만 기준일 배지 — 최신 종목에는 붙지 않는다', () => {
  installDom();
  resetState();
  applyStaleFixture();
  renderCards();

  assert.deepEqual(staleBadgeTextsIn('cards').sort(), ['2026.07.29 기준', '2026.08.13 기준']);
  const badgedNames = [...document.querySelectorAll('#cards .card')]
    .filter(card => card.querySelector('.stale-badge'))
    .map(card => card.querySelector('.name').textContent.trim());
  assert.deepEqual(badgedNames.sort(), ['감마우', '베타우']);
  // 배지는 종목명 행 안(투자매력도 칩과 같은 자리)에 들어간다.
  assert.ok(document.querySelector('#cards .card .name-row .stale-badge'));
  // 툴팁/스크린리더 문구에 마지막 시세일과 뒤처진 일수가 담긴다.
  const badge = document.querySelector('#cards .stale-badge');
  assert.match(badge.getAttribute('title'), /거래정지/);
  assert.match(badge.getAttribute('aria-label'), /\d+일/);
});

test('renderCards: 그룹 카드는 가장 오래 정체된 우선주 기준으로 배지가 붙는다', () => {
  installDom();
  resetState();
  applyStaleFixture();
  app.pairs.find(p => p.id === 'alpha2').lastHistoryDate = '2026-07-29'; // 대표(alpha)는 최신
  renderCards();

  const alphaCard = [...document.querySelectorAll('#cards .card')]
    .find(card => card.querySelector('.name').textContent.trim() === '알파');
  assert.equal(alphaCard.querySelector('.stale-badge').textContent.trim(), '2026.07.29 기준');
});

test('renderTable: 정체 종목 행에만 기준일 배지 (종목명 셀)', () => {
  installDom();
  resetState();
  applyStaleFixture();
  renderTable();

  assert.deepEqual(staleBadgeTextsIn('tableBody').sort(), ['2026.07.29 기준', '2026.08.13 기준']);
  const badgedRows = [...document.querySelectorAll('#tableBody tr')]
    .filter(row => row.querySelector('.stale-badge'))
    .map(row => row.querySelector('.table-name-button').textContent.trim());
  assert.deepEqual(badgedRows.sort(), ['감마우', '베타우']);
  // 종목명 셀(첫 칸)에만 붙는다.
  assert.equal(document.querySelectorAll('#tableBody td:not(:first-child) .stale-badge').length, 0);
});

test('renderCards/renderTable: 모든 종목이 같은 날짜면 배지가 하나도 없다', () => {
  installDom();
  resetState();
  app.pairs.forEach(pair => { pair.lastHistoryDate = '2026-08-20'; });
  renderCards();
  renderTable();

  assert.equal(document.querySelectorAll('.stale-badge').length, 0);
});

test('renderCards: 실시간 시세가 history에 반영돼도 파이프라인 기준일로 배지를 유지한다', () => {
  installDom();
  resetState();
  applyStaleFixture();
  // live.js의 upsertHistoryPoint가 오늘 날짜를 밀어 넣은 상황
  app.pairs.find(p => p.id === 'beta').history = [{ date: '2026-08-21', spread: 20 }];
  renderCards();

  assert.deepEqual(staleBadgeTextsIn('cards').sort(), ['2026.07.29 기준', '2026.08.13 기준']);
});
