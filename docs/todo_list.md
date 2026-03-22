# TODO List

## 🔴 긴급 (데이터 정합성 이슈)

> 현재 긴급 이슈 없음

---

## 🟡 보통 (품질 개선)

### 1. players.master_id NULL 145명 처리
- **현황**: 786명 중 145명이 master_id = NULL
  - TM 2026 kader 페이지에 등번호 미등록(`-`)이거나 2026시즌 K리그 미등록 선수
- **해결 방법**:
  - 등번호 미등록 선수: `back_number`와 동일 팀 선수명을 수동으로 대조하여 master_id 보완
  - 미등록 선수: 2024/2025 TM 데이터로 이미 player_master에 있으므로 수동 매핑 후 UPDATE

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
