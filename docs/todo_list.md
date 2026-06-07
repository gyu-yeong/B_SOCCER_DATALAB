# TODO List

## 🔵 진행 예정 (적재 작업)

### A. 2026 K리그1 8R~12R 적재
- **현황 (2026-05-11)**: schedule cid=33117 / 8R~13R 미적재과거 6건씩 (현 시점 기준 12R까지가 경기 완료 분)
- **선결 조건**: ✅ v0.8.6에서 schedule 재적재 + match_id 정합성 정상화 완료
- **실행**:
  ```bash
  python scripts/kleague_scripts/ETL_scheduler.py \
    --competition K리그1 --year 2026 --from-round 8 --to-round 12
  ```

### B. 2026 K리그2 전체 적재
- **현황 (2026-05-11)**: schedule cid=33113 / 272건 등재 / matches 0건
- **선결 조건**: ✅ v0.8.6에서 K리그2 cid 분리 완료
- **실행**:
  ```bash
  python scripts/kleague_scripts/ETL_scheduler.py --competition K리그2 --year 2026
  ```

### C. 2025 K리그2 전체 적재
- **현황**: schedule 275건 등재 / matches 0건 (이전 세션에서 대상으로만 등록되고 미실행)
- **실행**:
  ```bash
  python scripts/kleague_scripts/ETL_scheduler.py --competition K리그2 --year 2025
  ```

---

## 🟠 높음 (정합성 개선)

### 9. 2024 K리그2 player_master 미수록 선수 보강
- **현황 (2026-04-25)**: 2024 K리그2 player_match_stats 8,493건 중 6,553건만 master_id 매핑 (77.2%) — **1,940건(22.8%) 미매핑**
- **미매핑 고유 선수**: 115명
- **출전수 TOP 미매핑 선수**:

  | 선수명 | 팀 | 등번호 | 출전 |
  |--------|-----|--------|------|
  | 변경준 | 서울E | #16 | 37 |
  | 마테우스 | 안양 | #7 | 36 |
  | 뮬리치 | 수원 | #9 | 35 |
  | 유상훈 | 성남 | #1 | 34 |
  | 박태용 | 전남 | #88 | 33 |
  | 박한근 | 충남아산 | #1 | 33 |
  | 야고 | 안양 | #10 | 33 |
  | 최봉진 | 전남 | #1 | 33 |
  | 김승호 | 충남아산 | #21 | 32 |
  | 루페타 | 부천 | #42 | 32 |
  | 이지승 | 안산 | #28 | 32 |
  | 황준호 | 부산 | #45 | 32 |

- **원인 추정**: 2024 K리그2 전용 선수가 TM 스쿼드 수집 누락 또는 player_master에 미등록. K리그2 13팀 season_roster 559명 vs 매핑된 출전 선수 349명 → **210명 차이**
- **해결 방법**:
  ```bash
  python scripts/player_info/scrape_tm_squads.py --season 2024 --league kl2
  python scripts/kleague_scripts/build_db.py    # Phase 1c·2·3 재실행
  ```
- **검증 쿼리**:
  ```sql
  SELECT p.player_name, p.team_name, COUNT(*) games
  FROM player_match_stats pms JOIN players p ON pms.player_id=p.player_id
  JOIN matches m ON pms.match_id=m.match_id JOIN competitions c ON m.competition_id=c.competition_id
  WHERE c.year=2024 AND c.competition_name LIKE '%K리그2%' AND pms.master_id IS NULL
  GROUP BY p.player_name, p.team_name ORDER BY games DESC;
  ```

---

### 10. 동명이인 50개 그룹 추가 패치 (K리그2 적재 후 신규 케이스 검출)
- **현황 (2026-04-25)**: player_master 1,650명 중 동명이인 50개 그룹 검출
- **주요 그룹**:

  | 이름 | 인원 |
  |------|------|
  | 가브리엘 | 4명 |
  | 김민준 | 4명 |
  | Min-ho Kim, 김도윤, 김민재, 김성주, 김태환, 김현우, 이상민, 이지훈, 이탈로 | 3명씩 |

- **현재 패치 범위**: `patch_homonym_masters.py`는 K리그1 케이스만 처리 (가브리엘 강원/광주, 마테우스 울산/안양 총 144건)
- **이슈**: K리그2 적재 후 새로운 동명이인 충돌 발생 가능. master_id 미매핑 또는 잘못된 매핑 위험.
- **해결 방법**:
  1. K리그2 출전 동명이인 선수 birth_date·team_id 확인
  2. `patch_homonym_masters.py`의 `PATCH_RULES`에 케이스 추가
  3. 재실행 → master_id 정확성 검증

### 11. master_id 충돌로 시즌 출전수 과대 집계 (비교 시안 EDA에서 검출)
- **현황 (2026-06-07)**: `export_comparison_data.py`로 (시즌·리그·선수) 집계 시, **(master,match) dedup 후에도 출전수>42인 선수-시즌 11건** 검출. K리그1=38R / K리그2≤41R 한계를 초과 → 한 `master_id`에 **서로 다른 두 사람(동명이인)** 의 경기가 병합된 것으로 추정 (distinct player_id=2).
- **대표 케이스**: 이동경(id=56) 2025 KL1 63경기·2024 KL1 52경기, 박민수(id=850) 2024 KL2 61경기, 이동경(id=1055)·문창진(id=1095) 등
- **영향**: 해당 11명의 합계·90분당·백분위 모집단이 왜곡. 비교 시안(`players.generated.json`)에 그대로 반영됨.
- **해결 방법 (근본 — 미완)**:
  1. 해당 master_id들의 player_id 2개가 동일인인지 동명이인인지 확인 (birth_date·team_id·season_roster)
  2. 동명이인이면 master 분리 + `player_match_stats.master_id` 재매핑 (#10 패치 룰 연계)
  3. 재매핑 후 `export_comparison_data.py` 재실행 → 드롭 0건 검증
- **임시 조치 (✅ 적용, 2026-06-07)**: `export_comparison_data.py`가 `games > 리그상한+2`(KL1>40/KL2>43) **11건을 추출에서 드롭**. 경계 2건(2025 KL1 39·40경기)은 승강PO 가능성으로 보존+로그. → 데모(`players.generated.json`)는 정상화됨. **근본 재매핑은 여전히 필요.**

---

## 🟡 보통 (품질 개선)

### 0. 웹 구현 기술 스택 — 현황 정리 (방향 결정됨, 추적 종료)
- **결정 (2026-06-07)**: 데모/POC 단계는 **SQLite(`kleague.db`) → 정적 JSON(`players.generated.json`) → vanilla JS** 방식으로 확정. **이는 데모 한정이며 추후(FastAPI 등) 변경될 수 있음** — 단, 전환은 별도 작업 항목으로 트래킹하지 않음(필요 시점에 착수).
- **문서 정합성 정리 완료**:
  - (2026-06-03) `WEBDESIGN_GUIDE_PART2.md` 삭제(가이드 아닌 구현 계획서·git 미추적·내부 경로 모순), 백그라운드 worktree 3개 + 브랜치 제거
  - (2026-06-07) `WEBDESIGN_GUIDE.md` 배너를 "현재 SQLite+JSON 확정 / 추후 변경 가능"으로 갱신, `README.md` 표를 "목표 아키텍처"로 명시, `player_comparison_spec.md`를 `CLAUDE.md` 문서표 등록
- **참고**: `README.md`의 Next.js+D3+PostgreSQL+Redis는 미확정 **지향점**. 비교 페이지 실제 색·폰트·measure 정본은 `player_comparison_spec.md` + `export_comparison_data.py`.

---

### 1. player_match_stats.master_id 미매핑 0.4% (70건) 보완
- **현황 (2026-04-19)**: build_db.py + patch_homonym_masters.py 실행 후 18,233건 중 18,163건 (99%) master_id 확보. 70건 미매핑 (5명).
- **미매핑 원인**:

  | 선수명 | 팀·시즌 | 건수 | 원인 |
  |--------|---------|------|------|
  | ~~가브리엘~~ | ~~강원 2024·2025, 광주 2024·2025~~ | ~~85~~ | ~~동명이인~~ → patch_homonym_masters.py로 해소 |
  | ~~마테우스~~ | ~~울산 2024, 안양 2025~~ | ~~59~~ | ~~동명이인~~ → patch_homonym_masters.py로 해소 |
  | 윤석영 | 강원 2024 | 27 | 강원 내 1990-02-13 동일 생일 2명(윤석영·박청효) → Phase 1b_ext collision |
  | 이상민 | 김천 2024, 대전 2024 | 21 | 동명이인 — season_roster disambiguation 실패 |
  | 안재준 | 포항 2024·2025 | 18 | Phase 1b_ext collision (동일 birth_date 복수) 또는 TM 미수록 |
  | 황재환 | 광주 2025 | 3 | TM jersey_number '-' → Phase 1b_ext 매칭 불가 |
  | 임지민 | 대구 2024 | 1 | Phase 1b_ext collision 또는 TM 미수록 |

- **해결 방법**:
  - 동명이인(가브리엘, 이상민, 마테우스): 각 선수 birth_date 확인 → season_roster 수동 보완 → build_db.py Phase 3 재실행
  - 한국인 collision(윤석영, 안재준, 임지민): player_info CSV에 birth_date 있음 → player_master에 직접 INSERT + Phase 3 재실행
  - 황재환: TM squad에 jersey_number 없음 → player_master 수동 INSERT 후 Phase 3 재실행

---

### 2. 2026시즌 경기 데이터 ETL 연동
- **현황**: 2026시즌 schedule 테이블은 구축되어 있으나 실제 경기기록(player_match_stats) 미적재
- **해결 방법**: `ETL_scheduler.py` 또는 `ETL_backpill_stable.py`로 2026시즌 경기 시작 후 수집
- **주의**: ETL 실행 시 `build_db.py` Phase 3 재실행으로 신규 적재분 master_id 백필 필요

---

### 3. matches 테이블 competition_name · season 접근 방안
- **현황**: `matches`에 `competition_id` FK만 존재. `competition_name` · 시즌(year) 컬럼 없음
- **분석**: `competitions` 테이블에 이미 `year` + `competition_name` 컬럼 있음 → JOIN으로 접근 가능하므로 matches에 중복 컬럼 추가는 불필요
- **해결 방법**:
  - 대시보드·분석 쿼리에서 `matches JOIN competitions USING (competition_id)` 패턴 사용
  - 자주 쓰이는 경우 편의용 VIEW 생성 검토:
    ```sql
    CREATE VIEW v_matches AS
    SELECT m.*, c.year AS season, c.competition_name
    FROM matches m JOIN competitions c USING (competition_id);
    ```

---

### 5. matches.match_date 전체 NULL — 백필 필요
- **현황 (2026-04-19 확인)**: matches 456건(2024·2025시즌) 전체 `match_date = NULL`
- **소스 현황**:
  - `schedule` 테이블(471건)은 **2026시즌**만 적재 (competition_id: 33112·33113·33114)
  - `matches` 테이블은 **2024·2025시즌**만 적재 (competition_id: 23992·14879) → competition_id 교집합 없어 현재 JOIN 불가
  - `schedule.match_id`도 전건 NULL — 2026 ETL 미실행으로 matches에 2026 행 없음
- **수집 현황 (2026-04-19)**:
  - `data/raw/KLEAGUE_SCHEDULE/kleague1_2024.csv` ✅ 수집 완료
  - `data/raw/KLEAGUE_SCHEDULE/kleague2_2024.csv` ✅ 수집 완료
  - `data/raw/KLEAGUE_SCHEDULE/kleague2_2025.csv` ✅ 수집 완료
  - `data/raw/KLEAGUE_SCHEDULE/kleague1_2026.csv` ✅ 수집 완료
  - `data/raw/KLEAGUE_SCHEDULE/kleague1_2025.csv` 🔄 수집 중 — 완료 시 한번에 적재 예정
- **해결 방법**:
  - **2024·2025**: KLEAGUE_SCHEDULE CSV → `schedule` 테이블 적재 스크립트 작성 → `matches.match_date` UPDATE
  - **2026**: `ETL_scheduler.py` 실행 → matches에 2026 행 생성 → schedule JOIN으로 match_date + match_id 동시 백필
    ```sql
    UPDATE matches SET match_date = (
        SELECT s.match_date FROM schedule s
        WHERE s.competition_id = matches.competition_id
          AND s.round_number   = matches.round_number
          AND s.home_team_id   = matches.home_team_id
          AND s.away_team_id   = matches.away_team_id
    ) WHERE match_date IS NULL;
    ```

---

### 8. player_master.tm_player_id NULL 7건 보완
- **현황 (2026-04-19)**: master_id 3547~3553 (김성동·김태백·노승익·이탈로·진준서·마촙·가르시아) — TM squad 미수록으로 tm_player_id·citizenship·position·height_cm 미채움
- **해결 방법**: `backfill_tm_player_id.py --season 2026` 실행 또는 TM 수동 조회 후 `UPDATE player_master SET tm_player_id = '...' WHERE master_id = ...`

---

## 🟢 낮음 (장기 개선)

### 6. 강원FC TM squad 데이터 갭
- **현황**: `TM_squads_2024_KL2.csv`, `2025_KL2.csv` 등에 강원FC 데이터 없음
- **영향**: 강원FC 외국인 선수 season_roster 미등록 → Phase 3 동명이인 disambiguation 불가
- **해결 방법**: `scrape_tm_squads.py --season 2024 --league kl2` 재수집 후 build_db.py 재실행

### 7. player_master 시즌별 인적정보 이력 관리
- **현황**: market_value_eur 등이 제거되어 현재는 시장가치 미관리
- **고려**: 시장가치 데이터가 대시보드에 필요해질 경우 `player_master_history` 테이블 분리 검토
