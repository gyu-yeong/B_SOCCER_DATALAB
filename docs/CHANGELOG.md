# Changelog

## [Unreleased]

### Changed (2026-06-03) — 비교 시안 v3.0 (필터 폭·빠른검색·전체선택·반응형·swarm)
- **스탯 필터 테두리 콘텐츠 폭으로 축소** — `.filters` `width:fit-content`, 풀폭 카드 폐기 (1·2안)
- **필터 옆 빠른 검색 추가** — `선수 빠른 추가` 입력(네이티브 `datalist` 자동완성, `ROSTER_FLAT` 기반). 캐스케이딩 모달의 빠른 경로 보완
- **2레벨 드롭다운에 전체 선택 / 전체 해제** 버튼 추가
- **반응형** — 2안 카드 그리드 `repeat(auto-fit,minmax(260px,300px))`, 1안 히트맵은 `.heatscroll` 가로 스크롤(min-width 620)
- **2안 백분위 모드: 가로 swarm 차트** — 막대 대신, 스탯별 0~100 백분위 축에 분포를 점으로 표시. **beeswarm 패킹**(120점, 레이아웃 후 px 단위 충돌 회피 `placeSwarmDots`)으로 조밀하게 표현하고, 선택 선수(최대 3명, `PCOLORS` 색 구분)는 **색 링 + 연결선 + 이름·백분위 라벨 핀**으로 하이라이트. 0/25/50/75/100 축·격자선. resize 시 재배치. `comparison-data.js`에 `ROSTER_FLAT`·`PCOLORS` 추가
- 단위 total/per90 → 카드 바차트, pct → swarm 으로 2안 렌더 분기

### Changed (2026-06-03) — 비교 시안 v2.0 (필터·레이아웃·선수추가 흐름)
- **스탯 필터 타이틀 하단 이동** — 1레벨(슈팅/패스/수비) 버튼 → 2레벨 **드롭다운 체크리스트**(다중 선택, 기본 전체)로 변경. 기존 칩 나열 방식 폐기
- **단위 토글 기본값 90분당**으로 변경 (이전 전체합계)
- **선수 카드 정비** — team·season을 셀렉트→**텍스트** 표시, 포지션 옆 'K리그 데이터랩' 라벨 제거
- **선수 추가 캐스케이딩 모달** — `시즌 → 리그 → 팀 → 선수` 4단계 드릴다운 + 브레드크럼(상위 클릭 시 하위 리셋). 최대 3명, 중복 방지
- **`demo html/comparison-data.js` 신규** — 두 시안 공용. 로스터(2시즌×2리그×팀×선수) + 결정적 seeded 스탯 생성 + 백분위(시즌·리그 전체 선수 기준) 계산. 패스/수비 2레벨 스탯 잠정 정의 포함
- 검증: Chrome으로 1·2안 모두 필터 전환·드롭다운·캐스케이딩 추가(K리그2 선수 교차 추가 포함) 동작 확인

### Added (2026-06-03) — 선수 스탯 비교 페이지 시안
- **`docs/player_comparison_spec.md` 신규 작성** — 사용자 구술 요구 기반 비교 기능 명세 (기존 디자인 가이드와 독립 관리). 선수 선택 4단계(시즌→리그→팀→선수, 최대 3명), 스탯 2레벨 카테고리, 비교 단위 3종(전체합계/90분당/백분위), 시각화 1안(히트맵)·2안(바차트)
- **디자인 토큰 실측 확정** — `kleague.com/about/reference.do` computed style에서 추출: 색상(bg `#0A0A0A`/surface `#1C1C1C`/accent 네이비 `#001C48`), 폰트(국문 Noto Sans KR / 영문 Montserrat[Campton 무료 대체] / 숫자 Roboto). 이전 "K리그 레드 accent" 추정은 폐기(실제 네이비)
- **`demo html/kleague-comparison-v1-heatmap.html`** — 1안: 행=스탯/열=선수 그리드 히트맵, 셀 네이비 농도로 값 표현 + 행별 ★ 최고값
- **`demo html/kleague-comparison-v2-barchart.html`** — 2안: Squawka Matrix 구조 선수 카드(헤더 네이비+사선 모티브) + 스탯별 가로 바 + ★ 최고값 + 선수 추가 점선 슬롯
- 두 시안 공통: 단위 토글(전체합계/90분당/백분위) JS 동작, K리그1 2025 슈팅 카테고리 샘플 데이터 내장

### Docs / Cleanup (2026-06-03) — 디자인 문서 충돌 정리
- **`docs/WEBDESIGN_GUIDE_PART2.md` 삭제** — 디자인 가이드가 아닌 구현 스캐폴딩 계획서이고, git 미추적 + CLAUDE.md 문서표 미등록 + 내부 경로 모순(`kleague-matrix` vs `scratch/kleague-matrix`) 상태였음
- **백그라운드 세션 worktree 정리** — `.claude/worktrees/`의 worktree 3개(`elated-einstein-878c58`·`elegant-blackwell-575f73`·`strange-ramanujan-1c52d1`) 및 동명 `claude/*` 브랜치 제거 (main에 미병합된 디자인 문서 편집 폐기, blackwell의 Priority 5·6 포함)
- **`docs/WEBDESIGN_GUIDE.md` 정비**
  - 기술 스택 미확정(Next.js/D3 vs React/Streamlit/Chart.js) 충돌을 상단 경고 배너로 명시
  - §7 산출물 인벤토리를 실제 `demo html/`와 동기화 (v4-realdata·matrix-v5·seoul-season-analysis 추가)
- **`docs/todo_list.md`**: "0. 웹 구현 기술 스택 정본(SSoT) 확정" 이슈 등록

---

## [0.8.6] - 2026-05-11

### Fixed (CRITICAL)
- **`kleague1_2026.csv` 혼합 적재 문제 해결** — 파일에 K리그1(198)·K리그2(272)·슈퍼컵(1)이 섞여 있는데 모두 K리그1으로 적재되던 이슈
  - `load_csv()`: 파일 단위 hardcoded label 대신 **row별 INFO 컬럼**으로 competition 결정 (info_label은 INFO가 빈 row의 fallback으로만 사용)
  - `get_or_create_competition()`: 키워드 추출 로직 개선 — `"1" in info` 단순 체크 → `K리그1/K리그2/슈퍼컵` 명시적 매칭 (이전 로직은 "슈퍼컵"을 K리그2로 잘못 분류)
- **2026 schedule 재적재 결과**:
  - cid=33117 (하나은행 K리그1): 471 → **198건**
  - cid=33113 (K리그2): 0 → **272건** (신규 분리)
  - cid=33114 (K리그 슈퍼컵): 0 → **1건** (신규 분리)
- schedule.match_id 재연결: 1R~7R × 6경기 = 42건 정상 유지

### Added
- `ETL_scheduler.py`: `--from-round N` CLI 인자 추가 (자동 from_round 무시하고 강제 지정)
  - 사용 예: 자동값이 1로 잡히지만 8R부터 적재하고 싶을 때 `--from-round 8`

### Backup
- `database/kleague.db.bak_reload_2026` 생성 (2026 schedule 삭제·재적재 전 시점)

---

## [0.8.5] - 2026-04-25

### Fixed (CRITICAL)
- **`competitions` 중복 entry 통합** — schedule ↔ matches 조인 단절 문제 해결
  - 같은 시즌·리그가 두 cid로 중복 존재해 `update_schedule_match_ids()`가 항상 0건 연결되던 이슈
  - 통합: `33115('K리그2 2024') → 34786('하나은행 K리그2')`, `33112('K리그1') → 33117('하나은행 K리그1')`
  - schedule.match_id 연결: 0건 → **702건**
- **`schedule.round_number` 형식 통일** — `R{N}` → `{N}R` (matches 테이블 형식과 일치)
  - 1,403건 변환

### Added
- `scripts/kleague_scripts/migrate_competitions_dedup.py` — competitions 중복 통합 + round 정규화 + match_id 연결 일회성 마이그레이션 (idempotent)
- `ETL_ver4.get_or_create_competition_id()` — 신규 헬퍼 함수, 'K리그1'/'K리그2' 키워드 LIKE 매칭으로 중복 cid 생성 방지

### Changed
- `ETL_ver4.insert_dataframe` (2곳) — competition INSERT를 `get_or_create_competition_id()` 호출로 교체
- `load_schedule.normalize_round` — `f"R{N}"` → `f"{N}R"` (matches 테이블 형식 일치)

### Verified
- schedule.match_id 연결률: 2024 K리그2 99%, 2024·2025 K리그1 98%, 2026 K리그1 7R까지 적재분 100%
- 미연결 13건은 PO·승강PO·슈퍼컵 (일정만 존재, matches 라운드 번호 매칭 불가)

---

## [0.8.4] - 2026-04-25

### Data
- **2024 K리그2 전 시즌(1R~41R) 경기 결과 적재 완료** — `ETL_scheduler.py --competition K리그2 --year 2024`
  - player_match_stats: 19,902건 → **28,395건** (+8,493건)
  - 13팀 × 41라운드 전체 수집

### Fixed
- `ETL_backpill_stable.py`: 팀 루프 레벨 try/except 추가 — 팀 드롭다운 실패 시 해당 팀만 skip
- `ETL_backpill_stable.py`: 팀 완료마다 버퍼 누적 로그 출력 (진행 상황 가시성 개선)
- `ETL_scheduler.py`: `--year` CLI 인자 추가 (특정 시즌만 선택 실행)
- `ETL_scheduler.py`: `fetch_unscraped_targets` SQL을 `IN` 정확 매칭 → `LIKE` 부분 매칭으로 변경
  - `"K리그2 2024"`, `"하나은행 K리그1"` 등 스폰서명·연도 포함 competition_name도 정상 탐지
- `ETL_scheduler.py`: `resolve_meet_value()` 헬퍼 추가 — 부분 문자열로 meet_value 결정

---

## [0.8.3] - 2026-04-19

### Data
- **2026 K리그1 1R~7R 경기 결과 적재 완료** — `ETL_scheduler.py --to-round 7`
  - player_match_stats: 18,233건 → **19,902건** (+1,669건)
  - 라운드별 6경기 × 7라운드 = 42경기 / 라운드당 평균 238명

### Fixed
- `ETL_backpill_stable.py`: `scrape_match_data`에 `to_round` 상한 파라미터 추가
  - 기존: `from_round`만 존재 → 지정 라운드 이후 전체 수집
  - 변경: `to_round=None` 추가 → `to_round` 초과 라운드 스킵
- `ETL_scheduler.py`: `--to-round` CLI 인자 추가 (기본값 None = 상한 없음)
- `ETL_backpill_stable.py`: `webdriver_manager` NOTICES 파일 반환 버그 우회 (`chromedriver.exe` 경로 보정)

---

## [0.8.2] - 2026-04-19

### Removed
- **미사용 스크립트 4종 → `_archive/` 이동**
  - `scripts/kleague_scripts/ETL_player_master.py` — 미완성 stub (57줄, 실로직 없음)
  - `scripts/player_info/build_player_master.py` — `build_db.py`로 완전 대체
  - `scripts/player_info/map_foreign_korean_names.py` — Phase 1c + 외국인 음차명 자동 매핑 방식 폐기, `name_kor` 직접 입력 방식으로 대체
  - `scripts/player_info/import_korean_names.py` — `map_foreign_korean_names.py` 산출물 적재용, 위와 함께 미사용

### Changed
- `docs/scripts.md` 현행화 — 아카이브된 스크립트 섹션 제거, 아카이브 경로 안내 추가

---

## [0.8.1] - 2026-04-19

### Added
- **`patch_homonym_masters.py` 신규**: 동명이인 선수 master_id 수동 패치 스크립트
  - `(player_name, team_id)` 조합으로 `player_match_stats.master_id` 직접 UPDATE
  - 대상: 가브리엘(강원/광주), 마테우스(울산/안양) 총 144건

### Data
- **player_match_stats master_id**: 18,019건 (98%) → **18,163건 (99%)**
- **미매핑 잔여**: 214건 → **70건** (이상민 동명이인, 윤석영·안재준·임지민·황재환)

---

## [0.8.0] - 2026-04-19

### Changed
- **`build_db.py` Phase 1a: TM CSV `name_kor` 컬럼 우선 사용**
  - 외국인 선수에 대해 `name_kor` 컬럼이 채워져 있으면 해당 한글 등록명으로 `player_master` 직접 등록
  - 미기재 시 기존대로 `name_original`(영문) 임시 등록 → Phase 1b에서 교체
  - KL1 2024·2025·2026 외국인 선수 한글명 수동 입력(225명) 반영

### Removed
- **`build_db.py` Phase 1c 완전 삭제**
  - `players` 테이블 + TM squad jersey/team 기반 역매핑 로직 제거
  - Phase 1a `name_kor` 직접 등록 방식으로 동일 목적 달성 → Phase 1c 불필요

### Data
- **player_match_stats master_id**: 17,429건 (95.6%) → **18,019건 (98%)**
- **미매핑 잔여**: 804건 → **214건** (외국인 TM 미수록 + KL2 미적재 + 동명이인)
- **player_master**: 1,618명 → 1,650명
- **season_roster**: 3,130건 → 3,137건

---

## [0.7.5] - 2026-04-13

### Fixed
- **`build_db.py` Phase 1c 정확도 개선 — 3중 버그 수정**
  1. `birth_date` 단독 조회 → `name_original + birth_date` 정확 매칭으로 변경
     - 동일 birth_date 보유 선수가 여러 명일 때 엉뚱한 player_master 엔트리에 이름을 덮어쓰던 문제 해결
  2. `pm_cands` 중복 삽입 → `master_id` 기준 dict 중복 제거
     - 같은 선수가 2024·2025 두 시즌 TM CSV에 모두 등재 시 동일 master_id가 두 번 추가되어 `len(pm_cands) == 2` 오판으로 skip되던 문제 해결
  3. `season` 필터 추가 — pms 활동 시즌 기반으로 TM squad 후보를 시즌 범위로 좁힘
     - 같은 팀·등번호를 다른 시즌에 다른 선수가 착용한 경우 오매핑 방지
- **싸박(Pablo Sabbag) 매핑 확정**: birth_date=1997-06-11, 수원FC 2025 → master_id=1893

### Data
- **Phase 1c 갱신**: 27명 → 32명 (+5명 추가 해소)
- **player_match_stats master_id**: 17,238건 (94%) → **17,429건 (95.6%)**
  - 이전 97%와의 차이: 구 Phase 1c가 birth_date 충돌 선수를 잘못된 master_id에 매핑하던 부분이 이번 수정으로 올바르게 skip됨 → 정확도 향상, 수치는 소폭 조정
- **미매핑 잔여**: 804건 (33명 외국인 TM 미수록·등번호 충돌 + 이상민 동명이인 2명 + 가브리엘 동명이인 2명)

---

## [0.7.4] - 2026-04-12

### Fixed
- **`build_db.py` Phase 1b 소스 확장**: `선수인적정보.xlsx`(2026 스냅샷) 단독 → `player_info/2025시즌.csv` · `2024시즌.csv` 추가
  - 2026시즌에 K리그에 없는 선수(해외이적·은퇴)가 영문명으로 player_master에 잔류하던 문제 해결
  - `_apply_korean_names()` 헬퍼로 다중 소스 공통 로직 분리
  - 우선순위: xlsx 2026 → player_info 2025 → player_info 2024 (먼저 갱신된 선수는 이후 소스가 덮어쓰지 않음)
- **`build_db.py` Phase 1b_ext 신규 추가**: birth_date 충돌 선수 팀 기반 disambiguation
  - Phase 1b에서 동일 birth_date 복수 선수로 갱신 불가했던 한국인 선수 93명 추가 해결
  - pms 소속팀 → TM squad CSV 교차 → name_original 특정 → player_master UPDATE

### Data
- **Phase 1b 한글명 갱신**: 747명 → 1,139명 (+392명)
- **Phase 1b_ext 팀 기반 disambiguation**: +93명
- **player_match_stats master_id**: 16,695건 (91%) → **17,734건 (97%)**
- **미매핑**: 1,538건 → 499건 (외국인 TM 미수록 22명 + 동명이인 이상민 2명)

---

## [0.7.3] - 2026-04-12

### Fixed
- **`scrape_tm_squads.py` 포지션별 테이블 누락 버그**: `select_one("table.items")` → `select("table.items")` 로 수정
  - TM `plus/1` 뷰는 GK/DEF/MID/FWD 별로 `table.items`가 분리됨 → 기존 코드는 첫 번째 테이블(GK)만 파싱
  - 수정 후 팀당 수집 인원이 대폭 증가 (포항 2024: 38명 → 60명 등)
- **`scrape_tm_squads.py` saison_id 오프셋 버그**: `season - 2` → `season - 1` 로 수정
  - TM은 K리그 캘린더 시즌을 `year - 1`로 인덱싱 (2024시즌 = saison_id=2023)
  - 기존 코드는 1시즌 이전 데이터를 수집해 파일명과 실제 데이터 불일치 발생
  - 예: `TM_squads_2024_KL1.csv`가 실제로는 2023 시즌 데이터를 담고 있었음
- **`scrape_tm_squads.py` OUTPUT_DIR**: `data/raw/` → `data/raw/TM_squads/` 로 수정 (파일 이동 반영)
- **`scrape_tm_squads.py` load_to_db 제거**: 구버전 스키마 기반 DB 적재 로직 제거 — build_db.py가 담당

### Data — TM_squads CSV 전 시즌 재수집 + build_db.py 재실행
- **수집량** (행 수): 2024 KL1 495→681, KL2 492→628 / 2025 KL1 681→627, KL2 628→593 / 2026 KL1 627→435, KL2 593→582
- **player_master**: 1657명 → 1618명 (saison_id 수정으로 시즌 데이터 정확도 개선)
- **season_roster**: 3286건 → 3250건
- **player_match_stats master_id**: 17895건 (98%) → **16695건 (91%)**
  - saison_id 버그 수정으로 시즌 매칭이 실제 시즌 기준으로 재정렬됨 → 단기 확보율 소폭 하락, 정확도 개선
- **조르지(포항 2024) 수동 등록 가능**: Jorge Teixeira(1999-06-21) 이제 `TM_squads_2024_KL1.csv`에 포함됨

---

## [0.7.2] - 2026-04-12

### Changed
- **`build_db.py` Phase 1a 소스 교체**: `player_info/{season}시즌.csv` → `TM_squads/TM_squads_*.csv`
  - 기존 파일들은 모두 2026 소속팀 기준으로 수집된 데이터였음 (시즌별 실제 소속 반영 안 됨)
  - TM_squads CSV는 시즌별 실제 등록 스쿼드 → 올바른 소스
  - `name_kor = name_original` (영문/키릴)로 초기 등록 → Phase 1b/1c에서 한글명 교체
- **TM_squads 파일 이동**: `data/raw/TM_squads_*.csv` → `data/raw/TM_squads/` 디렉터리로 정리
- **`build_db.py` TM_SQ_DIR 상수 추가**: `data/raw/TM_squads/` 경로를 별도 상수로 분리

### Data
- **player_master**: 1601명 → 1657명 (+56)
- **season_roster**: 2631건 → 3286건 (+655, 시즌별 커버리지 개선)
- **player_match_stats master_id**: 17724건 (97%) → 17895건 (**98%**)
- **미매핑 선수**: 31명 → 19명 (외국인 16명 + 동명이인 3명)

---

## [0.7.1] - 2026-04-12

### Removed
- **`season_rosters` 테이블 삭제** — v0.4.0에서 `players.player_id` 기반으로 생성된 구버전 테이블 (965건)
  - 신버전 `season_roster` (`master_id` 기반, 3286건)로 완전 대체됨
  - 어떠한 현재 스크립트도 참조하지 않음을 확인 후 DROP

---

## [0.7.0] - 2026-04-12

### Changed — DB 전면 재설계: player_master 재구축 + season_roster 신설

#### 핵심 설계 변경
- **player_master 재구축**: TM 영문명 + 시장가치 기반 → `name_kor` + `birth_date` 기반으로 전환
  - 기존 UNIQUE `(name_original, birth_date)` → 신규 UNIQUE `(name_kor, birth_date)`
  - `name_kor`: 한국인=한글명, 외국인=한글 음차명 (영문/키릴 임시명에서 업데이트)
  - 불필요 컬럼 제거: `name_original`, `is_korean`, `foot`, `joined`, `signed_from`, `contract_until`, `market_value_eur`
  - 1601명 확보 (birth_date 100% 채움)
- **season_roster 재설계**: 기존 `season_rosters (season_year, player_id, team_id)` → 신규 `season_roster (master_id, season, team_id)`
  - `players` 테이블 FK 제거, `player_master` FK로 직접 참조
  - UNIQUE: `(master_id, season, team_id)` — jersey_number는 UNIQUE 아님 (여름 이적시장 재배정 가능)
  - 2631건 적재 (2024·2025·2026 3시즌)
- **player_match_stats.master_id 추가**: `player_id` 참조 유지하면서 `master_id` 컬럼 신규 추가
  - 17724/18233건 master_id 확보 (97%)
  - 미매칭 3%: TM squad CSV 미수집 팀(강원FC 등), 2024·2025 미포함 외국인 선수

#### 구축 소스 및 Phase 전략 (`build_db.py`)
| Phase | 소스 | 내용 |
|---|---|---|
| 1a | `player_info/{season}시즌.csv` | 한국인 한글명 + 외국인 영문명 → player_master |
| 1b | `선수인적정보.xlsx` (2026 포털) | 외국인 한글 음차명 업데이트 (107명) |
| 1c | 기존 `players` 테이블 + TM squad CSV | 2024·2025 외국인 음차명 보완 (42명) |
| 2 | `TM_squads_{season}_KL*.csv` | birth_date → master_id 조회 → season_roster 적재 |
| 3 | `player_match_stats` 전체 | name_kor 기반 master_id 백필 (97%) |

#### ETL 변경
- **`ETL_ver4.py`** `import_csv_to_db()` / `insert_dataframe()`: `master_id` 조회 로직 추가
  - `name_kor → player_master → master_id` 1순위
  - 동명이인: `season_roster(team_id)` disambiguation 2순위
  - `player_match_stats` INSERT에 `master_id` 컬럼 추가

### Fixed
- **is_ascii_name → is_non_korean**: 키릴 문자(세르비아·몬테네그로 선수) 영문명과 동일하게 비한글로 판별하도록 수정
  - 기존: `str.isascii()` → 키릴 = non-ASCII → UPDATE 스킵
  - 수정: 한글 유니코드(`\uAC00-\uD7A3`) 미포함 여부 판별
- **TM squad CSV birth_date 파싱**: `parse_birth_tm` → `parse_birth_portal` 교체 (CSV 이미 YYYY-MM-DD 포맷)
- **TM_TO_KOR 팀명 보완**: `Ulsan Hyundai`, `Jeju United` 추가 (구 팀명 → no_team 0으로 해소)

### Data
- **player_master**: 1601명 / birth_date NULL 0명
- **season_roster**: 2631건 (2024 KL1 344 + KL2 331 / 2025 KL1 540 + KL2 480 / 2026 KL1 503 + KL2 433)
- **player_match_stats master_id**: 17724/18233건 (97%)

---

## [0.6.1] - 2026-03-22

### Fixed
- **`players.master_id` 추가 보완** — 전 시즌(2024·2025·2026) TM CSV 기반 재매핑
  - 분석: master_id=NULL 145명 전원 player_master에 인적정보 존재 확인
  - 1:1 매칭(53) + 복수시즌 동일인(30) = 83명 중 67명 자동 업데이트
    - 42명: 1:1 신규 매핑
    - 25명: 동일인 player_id 복수 케이스 master_id 공유
    - 16명: jersey_number가 다른 선수를 가리켜 오매핑 위험으로 스킵
  - 최종 708/786명 확보 (90.1%), NULL 78명 잔존
  - NULL 78명 원인: 등번호 충돌(44) · TM 미등록(18) · 오매핑 위험 스킵(16)

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
