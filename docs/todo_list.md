# TODO List

## 🔴 긴급 (데이터 정합성 이슈)

### 1. players.master_id 오매핑 전면 재구축 필요

- **현황**: 현재 708/786명이 master_id를 가지고 있으나, **동일 master_id에 서로 다른 player_name이 매핑된 케이스가 166건** 발견됨 → 실제 정확 매핑 수는 불명확
- **근본 원인: jersey_number 기반 매핑의 구조적 결함**
  - `players.back_number`는 포털 스크래핑 시점(특정 시즌) 기준이고, TM 2026 CSV는 2026시즌 기준 → 시즌이 다르면 같은 팀+번호가 전혀 다른 선수를 가리킴
  - 예: 모재현(김천 #27, 2025시즌 기준) → TM 2026 김천 #27 = 추상훈 → `master_id=719(추상훈)` 오매핑
  - 동일 선수가 이적 또는 등번호 변경 시 `players`에 새 player_id로 중복 생성 → 오매핑 연쇄 발생

- **오매핑 케이스 예시**:

  | master_id | 실제 PM 선수 | 잘못 매핑된 players |
  |---|---|---|
  | 40 | Dong-jun Lee (이동준) | 에르난데스, 한교원, 원두재 |
  | 128 | Min-kyu Joo (주민규) | 호사, 마사 |
  | 266 | (특정 선수) | 유인수, 제갈재민, 박주영 |

- **해결 방향 (두 가지 선택지, 미결정)**:

  #### 방향 A — 현행 TM 기반 player_master 유지 + 매핑 재구축
  - `players.master_id` 전체 NULL 리셋
  - `data/raw/player_info/{season}시즌.csv` 활용
    - `Name in home country`(한글명) → `players.player_name` 직접 매칭
    - `Date of birth/Age` → `player_master.birth_date` 매칭
    - jersey_number 미사용 → 이적/등번호 변경에 무관
  - 시뮬레이션 결과: 527명 1:1 자동 매핑 가능 / 140명 birth_date 중복(팀명 2차 필터) / 119명 수동 필요
  - **유지되는 것**: TM 기반 player_master (tm_player_id, 시장가치, 계약 만료일 등)

  #### 방향 B — K리그 포털을 인적정보 마스터로 전환
  - `player_info/{season}시즌.csv` (한글명 + birth_date + position + height)를 player_master 역할로 사용
  - `players` 테이블에 `birth_date` 컬럼 추가 → 브릿지 매핑 불필요, 직접 조인 가능
  - TM player_master는 tm_player_id / 시장가치 조회용 별도 테이블로 격하 또는 폐기
  - **장점**: 구조 단순화, 오매핑 리스크 구조적 제거
  - **고려사항**: 대시보드에서 시장가치·TM 연동 데이터 사용 여부에 따라 결정

- **결정 기준**: 대시보드에서 **TM 시장가치 / 이적 이력 / 계약 만료일** 표시 필요 여부
  - 필요 → 방향 A (TM 기반 유지)
  - 불필요 → 방향 B (포털 기반 단순화)

- **보유 파일**:
  - `data/raw/player_info/2024시즌.csv` (1,106행), `2025시즌.csv` (1,057행), `2026시즌.csv` (1,000행)
    - 컬럼: `Name in home country`, `Date of birth/Age`, `Current club`, `Position`, `Height`, `Citizenship` 등

---

## 🟡 보통 (품질 개선)

### 2. players.master_id NULL 78명 잔존
- **현황**: master_id 오매핑 재구축 작업(이슈 1) 선행 후 재집계 필요
- **원인별 분류** (현 기준):
  - 44명: 등번호 충돌 (같은 팀·등번호에 시즌별 다른 선수 혼재)
  - 18명: TM 등번호 미등록(`-`)
  - 16명: jersey_number가 다른 선수를 가리켜 오매핑 위험으로 스킵

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
- **현황**: market_value_eur 등이 최신 시즌 단일값으로만 관리됨
- **고려**: `player_master_history` 테이블로 시즌별 이력 분리 여부 검토
