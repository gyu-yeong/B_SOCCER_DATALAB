# TODO List

## 🔴 긴급 (데이터 정합성 이슈)

> 현재 긴급 이슈 없음

---

## 🟡 보통 (품질 개선)

### 1. players.master_id NULL 78명 처리
- **현황**: 786명 중 78명이 master_id = NULL (v0.6.1에서 67명 자동 보완, 708/786 확보)
- **원인별 분류**:
  - 44명: 등번호 충돌 (같은 팀·등번호에 시즌별 다른 선수 혼재)
  - 18명: TM 등번호 미등록(`-`)
  - 16명: jersey_number 매칭이 다른 선수를 가리켜 오매핑 위험으로 스킵 (김천 12건 포함)
- **해결 방법**:
  - players.back_number + 선수명을 TM CSV와 수동 대조 후 `UPDATE players SET master_id = ? WHERE player_id = ?`
  - jersey_number 방식 한계 → 향후 K리그 포털 생년월일 수집 후 `birth_date + team` 매핑으로 전환 검토

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
