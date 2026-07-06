# 보통주/우선주 괴리율 대시보드

한국 상장사 보통주–우선주 쌍의 가격 괴리율(스프레드)을 추적하는 정적 대시보드입니다.
GitHub Actions가 주기적으로 시세를 수집해 저장소에 커밋하고, GitHub Pages가 그대로 서빙합니다.

- 대시보드 URL 패턴: `https://<owner>.github.io/common_preferred_spread/`
- iframe 임베드 지원: `?embed`(헤더/푸터 숨김), `?theme=dark|light`(테마 강제)
  예: `<iframe src="https://<owner>.github.io/common_preferred_spread/?embed&theme=dark"></iframe>`

## 아키텍처

```
 Yahoo Finance   네이버 금융   프록시 히스토리 API   KIS Open API   내부 API(옵션)
      │              │               │                 │              │
      ├──────────────┼───────────────┤                 ├──────────────┤
      ▼              ▼               ▼                 ▼              ▼
        fetch_data.py (일 1회)              fetch_current.py (장중 30분 간격)
                │                                       │
                ▼                                       ▼
   data.js + data/(summary·history·dividends)      current.json
                │                                       │
                └────────────── git commit ─────────────┘
                                   │
                                   ▼
                  GitHub Pages ──▶ index.html (대시보드)
```

- `index.html` — 대시보드 본체(마크업 + 인라인 테마 부트스트랩). `data/summary.json`을 우선 로드하고 종목별 히스토리는 지연 로드합니다.
- `css/app.css` — 대시보드 전체 스타일 (embed 모드 CSS 포함).
- `js/` — 대시보드 로직 ES 모듈. 빌드 도구 없이 GitHub Pages가 그대로 서빙합니다.
  - `main.js`(진입점·부트스트랩) / `state.js`(공유 상태 `app`·상수·데이터 로더) / `format.js`(포맷터·날짜 유틸) / `calc.js`(지수·통계 계산) / `market.js`(시장 메트릭 병합·세션 로직) / `labels.js`(종목 라벨/배지) / `charts.js`(캔버스 차트·줌) / `views.js`(렌더·바인딩) / `live.js`(실시간 갱신)
- `admin.html` — 종목 쌍 설정(config.json) 관리 UI. GitHub API로 직접 커밋합니다.
- `analysis/` — 오프라인 연구 스크립트(백테스트, 이벤트 스터디). CI와 무관합니다.

## 데이터 포맷

| 파일 | 내용 |
|---|---|
| `data/summary.json` | 전 종목 메타데이터 + 현재가 스냅샷 (대시보드 최초 로드용) |
| `data/history/<pairId>.json` | 종목 쌍별 일별 히스토리 (컬럼형) |
| `data/dividends.json` | 보통주/우선주 배당 이력 |
| `data.js` | 레거시 호환·analysis/용 전체 데이터 (콤팩트 직렬화) |
| `current.json` | 장중 현재가·시장지표 스냅샷 |

`data/history/<pairId>.json` 스키마 예시:

```json
{
  "dates": ["2026-06-01", "2026-06-02"],
  "common": [55000, 55400],
  "preferred": [44000, 44550],
  "spread": [20.0, 19.58]
}
```

`_average` pair에만 `kospi` 배열이 추가로 들어갑니다.

## 워크플로우

| 워크플로우 | 주기 | 하는 일 | 커밋 대상 |
|---|---|---|---|
| `update-data.yml` | 매일 KST 05:00 | `fetch_data.py` 실행 — 일별 시세 증분 갱신 + 자동 프록시 백필 1종 | `data.js`, `data/`, `proxy_backfill_progress.json` |
| `update-current.yml` | 평일 장중 30분 간격 (+16시/21시) | `fetch_current.py` 실행 — 현재가/시장지표 수집 | `current.json` |

- **의존성 고정 정책**: 2026-05~06 한 달간, 미고정 `pip install yfinance pandas`가 lxml을 더 이상 전이 설치하지 않게 되면서 네이버 백필 경로의 `pd.read_html`이 `ModuleNotFoundError`로 크래시해 일별 워크플로우가 매일 실패했습니다. 이후 모든 의존성은 `requirements*.txt`에 버전 고정하며 lxml을 명시합니다.
- **데이터 품질 가드**: 기존 데이터 대비 히스토리 시작일이 후퇴하거나 데이터 포인트가 급감하면 `fetch_data.py`가 exit 1로 실행을 실패시켜 커밋을 차단합니다. 의도적인 재구축일 때만 `--allow-history-truncation` 플래그를 사용하세요.
- **비파괴 병합**: 증분 갱신·백필은 날짜 기준 병합으로 동작합니다 — 같은 날짜는 새 값이 이기고, 새 데이터에 없는 날짜의 기존 레코드는 보존됩니다(`history_rules.merge_history_by_date`, hodling-value와 동일 규칙). 데이터 소스가 축소된 히스토리를 반환해도 기존 구간이 유실되지 않습니다. 실제 사례: 2026-06 Yahoo가 `00279K.KS`(아모레G3우B) 과거 구간을 2025-10 이후만 반환하게 되면서, 자동 프록시 백필의 재구축 결과(858일)가 기존(1562일)보다 성겨져 가드 실패 → 진행 파일 미저장 → 같은 pair 재선택의 실패 루프가 25일간 반복됐습니다. `--allow-history-truncation`일 때만 새 구간이 기존 창을 대체합니다.
- 두 워크플로우는 각자 별도 concurrency 그룹(`data-commit`/`current-commit`)을 사용해 같은 워크플로우끼리만 직렬화합니다(일별 작업이 장중 작업 대기열에 밀려 취소되는 것 방지). 워크플로우 간 push 경합은 커밋 스텝의 rebase 재시도로 처리합니다.

## 환경변수

| 변수 | 설정 위치 | 설명 |
|---|---|---|
| `KIS_APP_KEY` / `KIS_APP_SECRET` | Actions **Secrets** | 한국투자증권 Open API 키 (`fetch_current.py`) |
| `PROXY_HISTORY_BASE_URL` | 저장소 **Variables** | 프록시 히스토리 API 베이스 URL. Settings → Secrets and variables → Actions → **Variables**에 설정해야 프록시 백필이 동작 |
| `INTERNAL_*_API_URL` (CLOSE/DAILY/INDICES/DIVIDENDS/FX/COMMODITIES) | 로컬 실행 시에만 | 내부망 보조 API. 미설정 시 해당 소스 비활성(기본). CI에서는 사용하지 않음 |

전체 목록과 형식은 [`.env.example`](.env.example)을 참고하세요.

## 로컬 실행

```bash
pip install -r requirements.txt

python fetch_data.py                                   # 증분 갱신
python fetch_data.py --full                            # 전체 데이터 다시 다운로드
python fetch_data.py --proxy-backfill samsung_elec cj_4pref   # 특정 pair 프록시 백필

python fetch_current.py                                # 현재가 갱신 (KIS_* 환경변수 필요)

python -m http.server 8000                             # http://localhost:8000/ 에서 대시보드 확인

ruff check .                                           # Python 린트 (ruff.toml, CI와 동일)
npx --yes eslint@9 --config eslint.config.mjs "js/**/*.js"   # JS 린트 (eslint.config.mjs, CI와 동일)
```

## 데이터 복구/백필 절차

1. **백업 복원** — `update-data.yml`은 매 실행 전 `data.js`·`data/`·`proxy_backfill_progress.json`을 Actions 아티팩트(`data-backup-<run_id>`, 90일 보관)로 업로드합니다. 데이터가 깨졌다면 정상이던 실행의 아티팩트를 내려받아 저장소에 덮어쓰고 커밋하면 됩니다.
2. **git 히스토리 복원** — 아티팩트가 만료됐어도 데이터 파일은 매일 커밋되므로 git 히스토리가 사실상 영구 백업입니다. `git log -S '<유실 구간의 날짜 문자열>' -- data.js`로 유실 커밋을 찾아 그 부모 커밋의 `data.js`에서 해당 구간만 현재 데이터에 병합(prepend)한 뒤 `python data_writer.py --migrate`로 분할 출력을 재생성하면 됩니다. (실제 사례: 2026-04-10 신규 종목 추가 시 전체 재수집으로 `samsung_elec` 1989-09-25~1996-06-24 구간 1,978일이 유실됐고, 2026-07-06 이 방법으로 복구함)
3. **유실 히스토리 재백필** — git 히스토리에도 없는 구간은 프록시 API로 재수집합니다:
   1. 저장소 Variables에 `PROXY_HISTORY_BASE_URL` 설정 (선행 필수)
   2. Actions → "Update Stock Data (Daily)" → Run workflow → `proxy_backfill` 입력란에 pair id(예: `samsung_elec cj_4pref`) 입력 후 실행

## 보안 주의

- `admin.html`은 GitHub PAT를 **브라우저 localStorage에 평문 저장**합니다. 반드시 이 저장소 한정·최소 권한(Contents: Read/Write)의 fine-grained 토큰만 사용하고 주기적으로 폐기/교체하세요.

## 참고 문서

- 구조·품질 상세 리뷰: [docs/refactoring-review-2026-06.html](docs/refactoring-review-2026-06.html)
