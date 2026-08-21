// js/stale.js — 거래정지/데이터 정체 판정 + 배지 렌더 (format.js에만 의존하는 leaf 모듈)
//
// 배경: 거래정지 종목은 fetch_data.py의 carry_forward_missing_pairs가 직전 기록을
// 그대로 유지하므로 pair.current가 몇 주 전 가격인 채로 남는다. 파이프라인은
// find_stale_pair_warnings로 로그 경고만 남기므로, 대시보드에서도 "언제 기준 값인지"를
// 배지로 알린다.
import { escapeHtml, formatDateShort, getHistoryDateMs, normalizeDateText } from './format.js';

// fetch_data.py의 STALE_PAIR_WARN_DAYS(14일)는 "상장폐지/장기 정지 확인 필요" 알림 기준이라
// 더 느슨하다. UI 배지는 목적이 달라서(며칠 전 가격을 오늘 가격으로 읽는 것을 막는 것)
// 더 촘촘한 기준을 쓴다 — 주말을 포함해 5일(거래일 3~4일) 넘게 멈췄으면 이미 오늘 값이 아니다.
export const STALE_BADGE_WARN_DAYS = 5;

const DAY_MS = 24 * 60 * 60 * 1000;

// 실시간 시세가 history에 upsert되면 마지막 날짜가 오늘로 밀리므로,
// 로드 시점에 state.js가 심어 둔 pair.lastHistoryDate를 우선 사용한다.
export function getPairLastHistoryDate(pair) {
  const stamped = normalizeDateText(pair?.lastHistoryDate);
  if (stamped) return stamped;
  const history = Array.isArray(pair?.history) ? pair.history : [];
  return history.length ? normalizeDateText(history[history.length - 1].date) : null;
}

// 전체 종목의 최신 히스토리 날짜 (평균/지수 pair 제외 — 개별 종목 기준일과 비교해야 한다).
export function getLatestPairHistoryDate(pairs = []) {
  let latest = null;
  for (const pair of pairs) {
    if (pair?.isAverage) continue;
    const date = getPairLastHistoryDate(pair);
    if (date && (latest === null || date > latest)) latest = date;
  }
  return latest;
}

export function getDateGapDays(laterDate, earlierDate) {
  const laterMs = getHistoryDateMs(normalizeDateText(laterDate));
  const earlierMs = getHistoryDateMs(normalizeDateText(earlierDate));
  if (laterMs == null || earlierMs == null) return null;
  return Math.round((laterMs - earlierMs) / DAY_MS);
}

// 정체 종목이면 { lastDate, gapDays }, 아니면 null.
export function getPairStaleInfo(pair, latestDate, warnDays = STALE_BADGE_WARN_DAYS) {
  if (!pair || pair.isAverage) return null;
  const lastDate = getPairLastHistoryDate(pair);
  if (!lastDate) return null;
  const gapDays = getDateGapDays(latestDate, lastDate);
  if (gapDays == null || gapDays <= warnDays) return null;
  return { lastDate, gapDays };
}

// 카드는 한 그룹(같은 보통주)의 여러 우선주를 함께 보여주므로,
// 그룹 안에서 가장 오래 정체된 종목을 기준으로 배지를 붙인다.
export function getGroupStaleInfo(items = [], latestDate, warnDays = STALE_BADGE_WARN_DAYS) {
  let worst = null;
  for (const item of items) {
    const info = getPairStaleInfo(item?.pair || item, latestDate, warnDays);
    if (info && (worst === null || info.gapDays > worst.gapDays)) worst = info;
  }
  return worst;
}

export function formatStaleBadgeLabel(info) {
  return info ? `${formatDateShort(info.lastDate)} 기준` : '';
}

export function formatStaleBadgeTitle(info) {
  if (!info) return '';
  return `거래정지 추정 — 마지막 시세 ${info.lastDate} (전체 최신일보다 ${info.gapDays}일 뒤처짐)`;
}

export function renderStalePairBadge(info) {
  if (!info) return '';
  const title = formatStaleBadgeTitle(info);
  return `<span class="stale-badge" title="${escapeHtml(title)}" aria-label="${escapeHtml(title)}">`
    + `${escapeHtml(formatStaleBadgeLabel(info))}</span>`;
}
