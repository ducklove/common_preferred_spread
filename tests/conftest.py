"""pytest 공용 설정: repo 루트를 sys.path에 추가해 모듈 import를 보장한다."""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
