// 일부 Node 22.x 빌드는 `node --test tests/js/`의 디렉터리 인자를 테스트 검색 대신
// 엔트리 모듈로 실행한다(디렉터리 → index.js 해석). 이 셤은 그 경우에도 모든 테스트가
// 실행되도록 테스트 파일을 동적 import 한다.
// 디렉터리 검색이 정상 동작하는 빌드에서는 *.test.mjs 패턴만 수집되므로 이 파일은 무시된다.
import('./format.test.mjs');
import('./calc.test.mjs');
import('./state.test.mjs');
import('./views.test.mjs');
import('./render.test.mjs');
import('./live.test.mjs');
