# Changelog

## [Unreleased]

---

## [0.2.0] - 2026-03-02

### Added
- `ETL_backpill_stable.py` `scrape_match_data()`에 `from_round` 파라미터 추가
  - 이미 적재된 라운드 skip 가능 (예: `from_round=34`)
  - 게임 선택 전 드롭다운 텍스트에서 라운드 파싱 → 불필요한 페이지 로딩 없이 skip

### Fixed
- **홈/어웨이 판정 로직 역전 버그 수정**
  - `경기명`의 `(H)`/`(A)` 의미 재확인: `(H)` = 현재 행의 팀이 홈, `(A)` = 어웨이
  - 기존 코드: `str.contains(r'\(A\)$')` → 홈여부=1 (반대로 저장됨)
  - 수정 코드: `str.contains(r'\(H\)$')` → 홈여부=1
  - 영향 파일: `ETL_ver4.py`, `ETL_backpill_stable.py`
  - DB 초기화 후 재적재 완료 (matches: 198, player_match_stats: 7919)

### Changed
- `scripts/kleague_scripts/` 폴더 정리
  - 사용 중: `ETL_ver4.py`, `ETL_backpill_stable.py`, `ETL_player_master.py`
  - 과거 버전 `_archive/` 폴더로 이동: `ETL.py`, `ETL_ver2.py`, `ETL_ver3.py`, `ETL_ver4(backup).py`, `ETL_backpill.py`, `ETL_portal.py`, `1test.py`
- `.gitignore` 추가 및 GitHub 초기 업로드

---

## [0.1.0] - 2026-02-xx

### Added
- K리그 데이터 ETL 파이프라인 초기 구축
- `ETL_ver4.py`: CSV → SQLite 적재 (`import_csv_to_db`, `insert_dataframe`)
- `ETL_backpill_stable.py`: 포털 스크래핑 + DB 적재
- `ETL_player_master.py`: 선수 마스터 스크래핑

### Fixed
- **참조무결성 개선**
  - `players` 테이블 UNIQUE 제약 변경: `(player_name, team_name)` → `(player_name, back_number)`
    - 이적선수(동명+동번호): 동일 `player_id`로 통합 → 기록 연속성 유지
    - 동명이인(동명+다른번호): 별도 `player_id` 유지
  - `players.team_id` FK 추가 (`INTEGER REFERENCES teams(team_id)`)
  - ETL player SELECT: `team_name` 기준 → `back_number` 기준으로 변경

- **팀명 더티데이터 정제**
  - `상대팀명` 컬럼의 ` 울산(H)` 형태 → 공백 및 `(H)/(A)` suffix 제거
  - `라운드` 컬럼 trailing whitespace 제거 (`"9R "` → `"9R"`)

- **이모지 print 인코딩 오류**
  - Windows cp949 환경에서 `✅`, `📂` 출력 시 UnicodeEncodeError 발생
  - `sys.stdout.reconfigure(encoding="utf-8", errors="replace")` 추가로 해결

- **match 중복 적재 문제**
  - ETL이 항상 `team_id`를 `home_team_id`로 저장 → 동일 경기가 양팀 관점에서 2회 적재
  - `홈여부` 파생 후 `home_id`/`away_id` 분기 처리로 수정
  - matches 348건 → 174건 → 198건(전 라운드 포함) 으로 정상화

### Data
- 2025 K리그1 1R~33R CSV 적재 완료
  - `2025K리그1_경기기록_R29.csv` (1R~29R)
  - `2025K리그1_경기기록_R30_34.csv` (30R~33R)
