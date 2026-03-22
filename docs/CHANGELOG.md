# Changelog

## [Unreleased]

---

## [0.6.0] - 2026-03-22

### Changed — player_master 완전 재구축 (scrape_tm_squads.py 단독 소스)

- **`player_master` 테이블 스키마 정리**
  - 제거 컬럼: `position_detail`, `citizenship_2`, `current_club`
  - `tm_player_id` 전 레코드 확보 (100% 커버리지)
  - UNIQUE 제약: `(name_original, birth_date)`
  - 새 UPSERT: 13개 파라미터, `tm_player_id = COALESCE(기존값, 신규값)` 보존

- **`scrape_tm_squads.py` 핵심 재작성**
  - 헤더 기반 동적 컬럼 매핑 도입: `get_col_map(table)` + `_cell_text(cells, col_map, key)`
    → thead 파싱으로 col_map 생성, 고정 인덱스 의존 완전 제거
  - `position_detail` 수집 제거, `parse_position()` 단일 문자열 반환으로 단순화
  - `_parse_player_row(tr, col_map, ...)` 시그니처 변경 (col_map 파라미터 추가)
  - `scrape_squad()` 내부에서 `get_col_map(table)` 호출 후 `_parse_player_row`에 전달

- **`players.master_id` 재매핑** — jersey_number + team 기반 자동 재매핑
  - player_master 재구축으로 master_id 전체 변경 → 기존 매핑 전면 무효화
  - 팀명 매핑(한국어 ↔ TM 영문) + `back_number = jersey_number` 조합으로 재매핑
  - 641/786명 성공 (81.5%), 나머지 145명 = 등번호 미등록 또는 TM 2026 미포함 선수

### Fixed
- **foot/height 컬럼 교체 버그 완전 해결** — 헤더 기반 col_map 도입으로 구조적 수정
  - 기존 원인: TM 테이블 "Current club" 컬럼이 인덱스 4에 삽입 → Height/Foot 인덱스가 밀림
  - 신규: `th.get_text()` → `col_map["Height"]`, `col_map["Foot"]` 으로 정확 추출

- **야고 동명이인 오매핑 해결**
  - 2025시즌 TM 데이터 수집으로 안양 야고(Yago César, 1997-05-26) 별도 master_id 할당
  - 울산 야고(Yago Cariello, 1999-07-27)와 정확히 분리됨

- **문선민 시즌별 팀 조회 버그** (이전 버전에서 양 시즌 모두 서울로 표시)
  - `season_rosters.team_id → teams.team_name` 조인으로 수정 완료
  - 검증: 2024시즌 전북 / 2025시즌 서울 정확 출력

### Data
- **player_master 완전 재구축 완료** (DROP → 3시즌 순차 적재)
  - 2024시즌 KL1 12팀 456명 + KL2 13팀 492명 = 948명
  - 2025시즌 KL1 12팀 681명 + KL2 13팀 628명 = 1,309명
  - 2026시즌 KL1 12팀 627명 + KL2 14팀 593명 = 1,220명
  - 최종 `player_master`: 1,496명 (UPSERT 중복 제거 후) / `tm_player_id` 100% 확보

---

## [0.5.5] - 2026-03-22

### Fixed
- **`scripts/player_info/scrape_tm_squads.py`** — Selenium → `requests` 전환으로 Transfermarkt 팝업/광고 차단 문제 완전 해소
  - `undetected_chromedriver` 의존성 제거, `requests.Session` 기반으로 전환
  - `make_session()` 추가 (노트북 동일 User-Agent)
  - `tm_saison_id(season)` 헬퍼 추가 (`season - 2` 변환, 노트북 확인값)
  - `LEAGUES` 상수 수정: `RSK → RSK1`, `slug` 필드 추가 (`k-league-1`)
  - URL 패턴 수정: `/x/startseite/…/saison_id/{n}` → `/{slug}/startseite/…/plus/?saison_id={n}`
  - `TEAM_LINK_PATTERN` 수정: `$` 제거 (TM이 `/saison_id/XXXX` suffix 붙여 반환하는 경우 대응)
  - `get_team_list()`, `scrape_squad()` requests 방식으로 재작성
  - 선수 셀 셀렉터 수정: `a.hauptlink` → `td.hauptlink a` (실제 HTML 구조 반영)

- **`scripts/player_info/backfill_tm_player_id.py`** — 동일 패턴으로 requests 전환
  - Selenium 제거, `make_session()` / `tm_saison_id()` 추가
  - `LEAGUES` 상수 동일하게 수정
  - `get_team_list()`, `scrape_kader_for_ids()` requests 방식으로 재작성
  - 선수 셀 셀렉터 수정: `a.hauptlink` → `td.hauptlink a`

### Data
- **K League 1 2026 스쿼드 스크래핑 완료**: 12팀 627명 수집 → `player_master` UPSERT
  - `data/raw/TM_squads_2026_KL1.csv` 저장
  - `player_master` 총 1520명 / `tm_player_id` 확보 546명

---

## [0.5.4] - 2026-03-21

### Added
- **`scripts/player_info/backfill_tm_player_id.py` 신규 생성** — 기존 player_master 레코드 tm_player_id 백필
  - 이름 표기 차이(한글 vs 영문)에 의존하지 않고 **생년월일 기준** 매칭
  - 1순위: birth_date 유일 매칭 → 자동 UPDATE
  - 2순위: birth_date 복수 후보 → current_club 유사도 필터 후 자동 UPDATE
  - 3순위: 미해소 → `data/raw/unmatched_tm_id_{season}.csv` 저장 (수동 처리)
  - `tm_player_id` 컬럼 없으면 자동 ALTER TABLE

### Changed
- **`scripts/player_info/scrape_tm_squads.py` 수정**
  - 선수 링크(`/spieler/{id}`)에서 `tm_player_id` 추출 추가
  - UPSERT SQL에 `tm_player_id` 포함 (COALESCE: 기존값 우선)
  - `alter_table_if_needed()`에 `tm_player_id` 컬럼 추가
- **`docs/schema.md`** — `player_master` ERD 및 컬럼 명세에 `tm_player_id` 반영
- **`docs/scripts.md`** — `backfill_tm_player_id.py` 항목 추가

---

## [0.5.3] - 2026-03-21

### Added
- **`scripts/player_info/scrape_tm_squads.py` 신규 생성** — Transfermarkt K리그 스쿼드 자동 스크래핑
  - CLI 인자 `--season`(필수), `--league kl1|kl2|all`(기본: all)으로 수집 대상 지정
  - URL 직접 구성 방식: 대회 페이지 팀 목록 동적 수집 → 팀별 `/kader/…/plus/1` 접근 (Detailed 뷰 자동)
  - `undetected_chromedriver` + 4~8초 랜덤 딜레이로 봇 탐지 우회
  - 선수 셀 2줄 구조(이름+포지션) 파싱 → `position`, `position_detail` 파생 컬럼 분리
  - CSV 백업 저장 (`data/raw/TM_squads_{season}_KL1|KL2.csv`) + `player_master` UPSERT 동시 실행
  - `player_master` 신규 컬럼 자동 추가: `foot`, `signed_from`, `contract_until`

### Changed
- **`docs/schema.md`** — `player_master` 테이블에 `foot`, `signed_from`, `contract_until` 컬럼 추가 반영
- **`docs/scripts.md`** — `scrape_tm_squads.py` 항목 추가

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
