# TODO List

## 🟡 보통 (품질 개선)

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
- **해결 방법**:
  - **2024·2025**: K리그 포털에서 과거 일정표 CSV 수집 → `schedule` 테이블 적재 → UPDATE matches
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
