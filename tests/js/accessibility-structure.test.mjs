import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const html = readFileSync(path.join(root, 'index.html'), 'utf8');
const views = readFileSync(path.join(root, 'js/views.js'), 'utf8');
const css = readFileSync(path.join(root, 'css/app.css'), 'utf8');

test('카드 정렬과 기간 선택은 보조기기에 선택 상태를 알린다', () => {
  assert.match(html, /data-card-sort="spread"[^>]*aria-pressed="true"/);
  assert.match(html, /data-days="0"[^>]*aria-pressed="true"/);
  assert.match(views, /setAttribute\('aria-pressed', String\(active\)\)/);
});

test('모바일 터치 영역과 모션 축소 계약을 유지한다', () => {
  assert.match(css, /min-height:\s*44px/);
  assert.match(css, /prefers-reduced-motion:\s*reduce/);
});
