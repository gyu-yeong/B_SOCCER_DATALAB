# TODO List

## 🔴 긴급 (데이터 정합성 이슈)

### 1. player_master 동명이인 오매핑 수정
- **문제**: player_master가 2026시즌 Transfermarkt 기준으로만 구축되어, 2025시즌 이후 K리그를 떠난 선수가 누락됨
- **증상**: 안양 야고(2025시즌, 동명이인)가 강원/울산 야고(master_id=377)와 동일인물로 잘못 매핑됨
- **원인**: players 테이블에 birth_date가 없어 이름+팀으로만 식별 → player_master에 없는 선수가 동명이인에 흡수됨
- **해결 방법**:
  1. 2025시즌 `선수정보.csv` (Transfermarkt) 수집
  2. `build_player_master.py`를 `INSERT OR IGNORE` 방식으로 재실행 → 2025에만 있는 선수가 별도 master_id로 추가됨
  3. `players.master_id` 재매핑 실행
- **필요 데이터**: 2025시즌 Transfermarkt `선수정보.csv` (2026 버전과 동일 형식)

---

## 🟡 보통 (품질 개선)

### 2. players.master_id NULL 205명 처리
- **현황**: 786명 중 205명이 master_id = NULL
  - 166명: 2026시즌 K리그 미등록 (은퇴·해외이적 등)
  - 39명: 동명이인 미분류 (이름+팀으로 특정 불가)
- **해결 방법**:
  - 166명: 위 1번 작업(2025 데이터 추가)으로 일부 해소 가능
  - 39명: 수동 매핑 또는 K리그 데이터포털 과거 시즌 선수인적정보로 보완

### 3. season_rosters 2026시즌 자동 적재 연동
- **현황**: 2026시즌 stats 적재 시 season_rosters에도 자동 반영되지 않음
- **해결 방법**: ETL 스크립트에 `season_rosters` INSERT 로직 추가
  ```sql
  INSERT OR IGNORE INTO season_rosters (season_year, player_id, team_id, back_number)
  VALUES (?, ?, ?, ?)
  ```

---

## 🟢 낮음 (장기 개선)

### 4. player_master 시즌별 인적정보 이력 관리
- **현황**: current_club, market_value_eur 등이 최신 시즌 단일값으로만 관리됨
- **고려**: `player_master_history` 테이블로 시즌별 이력 분리 여부 검토
