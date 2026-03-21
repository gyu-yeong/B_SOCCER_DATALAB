# Changelog

## [Unreleased]

---

## [0.5.2] - 2026-03-21

### Changed
- **`docs/WEBDESIGN_GUIDE.md` 개편** — 색상 토큰 고정 방식 → 다크/라이트 모드 제약 방식으로 전환
  - 섹션 2: 고정 hex 토큰 제거, 모드별 명도 방향 제약으로 대체
  - 섹션 3: Noto Sans KR 필수 지정, 나머지 폰트 권장 사항으로 격하
  - 섹션 4: 컴포넌트 색상 참조 모드 상대적 표현으로 변경
  - 섹션 9 신설: K리그 아이덴티티 미확정 항목 (추후 작성)
  - 섹션 6 신설: 산출물 규칙 (버전 헤더, 컴포넌트 주석, 단계 전환)
  - 배포 URL 및 완성 파일 목록 업데이트

- **`docs/kleague-design-agent.md` 수정** — CSS 토큰 블록 제거, 모드 선택 + 색상 자유도 방식으로 교체
  - `docs/WEBDESIGN_GUIDE.md` 참조 방식으로 일원화

- **`.claude/claude.md` 업데이트** — 웹디자인 POC 에이전트 규칙 섹션 신설
  - `kleague-design-agent.md` 문서 테이블 등록
  - 트리거 키워드, 색상 원칙, 산출물 규칙, 추가 체크리스트 추가

---

## [0.5.1] - 2026-03-21

### Changed
- **README.md 전면 개편**
  - GitHub Pages 샘플 데모 URL 코멘트 추가
  - 개발 환경 설정·실행 섹션 제거
  - PRD v1.0 기반 프로젝트 소개·핵심 기능·데이터 커버리지·기술 스택 추가

---

## [0.5.0] - 2026-03-21

### Added
- **웹 디자인 가이드 신설** (`docs/WEBDESIGN_GUIDE.md`)
  - K리그 컬러 시스템, 타이포그래피, 컴포넌트 라이브러리 정의
  - 구현 우선순위 및 페이지 목록, 데이터 연결 전략 포함
  - 세션 간 디자인 일관성 유지를 위한 단일 진실 공급원

- **데모 HTML: 선수 랭킹 v1** (`demo html/kleague-datamb-ranking-v1.html`)
  - K리그 다크 테마 (라임 `#e8ff3c` 강조색)
  - 공중볼 경합 승리 Top 20 · 23세 이하 필터 · 90분당 기준
  - 값에 따른 라임→올리브 그라디에이션 pill 바 차트

- **데모 HTML: 선수 랭킹 v2** (`demo html/kleague-datamb-ranking-v2.html`)
  - DATAMB 라이트 테마 (블루 `#2563eb` 강조색)
  - 드롭다운 필터 (지표 / 포지션 / 연령 / 리그) + 아이콘 버튼
  - 연블루→진블루 그라디에이션 pill 바 차트 · 5개 지표 데이터셋

- **데모 HTML: 선수 비교 매트릭스 v3** (`demo html/kleague-comparison-v3-datamb.html`)
  - 기존 v2(다크)를 DATAMB 라이트 스타일로 전면 재설계
  - 플레이어 색상 P1=블루·P2=앰버·P3=에메랄드 / 라운드 카드 UI
  - 드롭다운 필터 + 시계·달력 아이콘 버튼 / 레이더 그리드 라이트 변환

- **GitHub Pages 자동 배포** (`.github/workflows/deploy-pages.yml`)
  - `demo html/` 변경 push 시 gh-pages 브랜치 자동 배포
  - index.html 동적 생성: 새 HTML 파일 추가 시 링크 목록 자동 갱신
  - 배포 URL: `https://gyu-yeong.github.io/B_SOCCER_DATALAB/`

---


## [0.4.0] - 2026-03-15

### Added
- **`player_master` 테이블 신설** — 선수 인물 원장 (2026시즌 Transfermarkt 기준)
  - 1인 = 1행, UNIQUE: `(name_original, birth_date)`
  - 한국인 865명 / 외국인 135명 (K리그 전체)
  - `citizenship_2` 컬럼: 이중국적자 26명 분리 (구분자 `\xa0\xa0` 기준)
  - 외국인 한국 음차명: K리그 데이터포털 `선수인적정보.xlsx` 기준 6단계 퍼지 매핑으로 135/135명 전원 자동 매핑
  - 관련 스크립트: `scripts/build_player_master.py`, `scripts/map_foreign_korean_names.py`, `scripts/import_korean_names.py`

- **`season_rosters` 테이블 신설** — 시즌별 선수-팀 스냅샷
  - PK: `(season_year, player_id, team_id)` — 시즌 중 이적 시 두 팀 모두 별도 행으로 기록
  - 소스: `player_match_stats.team_id` (경기 당시 실제 소속팀 직접 참조)
  - 2024시즌 490행 / 2025시즌 475행 (시즌 중 이적 12건 포함)

- **`players.master_id` 컬럼 추가** — `player_master` FK
  - 786명 중 581명 자동 매핑 (1:1 이름 매핑 560명 + 팀필터 추가 21명)
  - 205명 NULL 유지 (은퇴·해외이적으로 player_master 미등록 또는 동명이인 미분류)
  - 인덱스 추가: `idx_players_master_id`

### Fixed
- **`season_rosters` 팀 오기록 버그** — 초기 생성 시 `players.team_id`(스크래핑 시점 고정값) 사용으로 이적 선수의 이전 시즌 팀이 잘못 기록되던 문제
  - `players.team_id` → `player_match_stats.team_id`로 소스 변경
  - 동시에 PK를 `(season_year, player_id)` → `(season_year, player_id, team_id)`로 확장하여 시즌 중 이적 처리

### Known Issues
- `player_master`가 2026시즌 기준으로만 구축되어, 2025시즌 이후 K리그를 떠난 외국인 선수(예: 안양 야고)가 동명이인으로 오매핑될 수 있음
  - 해결 방법: 2025시즌 `선수정보.csv` 추가 수집 후 player_master 재구축 필요 (`docs/todo_list.md` 참조)

---


## [0.3.0] - 2026-03-14

### Added
- **Streamlit 선수 비교 매트릭스 앱 구축** (`app/main.py`, `app/db.py`)
  - 시즌 / 팀 / 선수 필터로 최대 3명 선택 (시즌 크로스 비교 지원)
  - 공격 / 패스 / 수비 카테고리 탭 전환
  - 지표 칩 토글 (개별 on/off), 90분 기준 토글
  - 표 형식 / 레이더 차트 전환 (canvas 기반 커스텀 드로잉)
  - `demo html/kleague-comparison-v2.html` 디자인 기반 다크테마 구현
  - 선수 표시 형식: `이름 (포지션, #등번호)` → 동명이인 구분 가능

- **`schedule` 테이블 신설** — 2026시즌 K리그 일정 마스터
  - K리그1 198경기 / K리그2 272경기 / 슈퍼컵 1경기 (총 471경기)
  - `competitions`, `teams`, `matches` 테이블과 FK 연결
  - 조인키: `(competition_id, round_number, home_team_id, away_team_id)` → `matches`와 1:1 매핑
  - 경기 결과 ETL 후 `match_id` 컬럼으로 실제 경기와 연결 가능

- **2026 competitions 추가**: K리그1, K리그2, K리그 슈퍼컵
- **신규 팀 16개 추가**: 부천(K리그1 승격) + K리그2 15팀
- **2024시즌 K리그1 데이터 적재 완료** (`ETL_backpill_stable.py` 포털 스크래핑)
- **2025시즌 34R~38R 데이터 적재 완료** (`from_round=34` 파라미터 활용)

### Changed
- `scripts/kleague_scripts/` 사용 스크립트 정리 → 미사용 7개 `_archive/`로 이동
- 프로젝트 GitHub 초기 업로드 (`docs/` 폴더 포함)

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
- 2024 K리그1 1R~38R 포털 스크래핑 적재 완료
- 2025 K리그1 34R~38R 포털 스크래핑 적재 완료
