// 일부 Node 22.x 빌드는 `node --test tests/js/`의 디렉터리 인자를 테스트 검색 대신
// 엔트리 모듈로 실행한다(디렉터리 → index.js 해석). 이 셤은 그 경우에도 모든 테스트가
// 실행되도록 테스트 파일을 동적 import 한다.
// 목록을 손으로 관리하면 새 파일이 조용히 빠지므로(accessibility-structure.test.mjs 누락 사례)
// 디렉터리의 *.test.mjs 를 전부 찾아 이름순으로 import 한다.
// 디렉터리 검색이 정상 동작하는 빌드에서는 *.test.mjs 패턴만 수집되므로 이 파일은 무시된다.
// 표준 실행 경로는 `npm test` / CI 의 `node --test tests/js/*.test.mjs` (셸 glob, 셤 불필요).
import { readdirSync } from 'node:fs';
import { URL } from 'node:url';

const dir = new URL('./', import.meta.url);
const files = readdirSync(dir).filter(name => name.endsWith('.test.mjs')).sort();
for (const name of files) {
  await import(new URL(name, dir).href);
}
