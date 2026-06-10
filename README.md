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

- `index.html` — 대시보드 본체. `data/summary.json`을 우선 로드하고 종목별 히스토리는 지연 로드합니다.
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
- 두 워크플로우는 `concurrency: data-commit` 그룹으로 묶여 push 경합이 직렬화되고, push 실패 시 rebase 후 재시도합니다.

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
```

## 데이터 복구/백필 절차

1. **백업 복원** — `update-data.yml`은 매 실행 전 `data.js`·`proxy_backfill_progress.json`을 Actions 아티팩트(`data-backup-<run_id>`, 90일 보관)로 업로드합니다. 데이터가 깨졌다면 정상이던 실행의 아티팩트를 내려받아 저장소에 덮어쓰고 커밋하면 됩니다.
2. **유실 히스토리 재백필** — 장기 히스토리가 유실된 경우(예: `samsung_elec`은 1989-09-25까지, `cj_4pref`는 2019-08-09까지 보유했었음):
   1. 저장소 Variables에 `PROXY_HISTORY_BASE_URL` 설정 (선행 필수)
   2. Actions → "Update Stock Data (Daily)" → Run workflow → `proxy_backfill` 입력란에 `samsung_elec cj_4pref` 입력 후 실행

## 보안 주의

- `admin.html`은 GitHub PAT를 **브라우저 localStorage에 평문 저장**합니다. 반드시 이 저장소 한정·최소 권한(Contents: Read/Write)의 fine-grained 토큰만 사용하고 주기적으로 폐기/교체하세요.

## 참고 문서

- 구조·품질 상세 리뷰: [docs/refactoring-review-2026-06.html](docs/refactoring-review-2026-06.html)
