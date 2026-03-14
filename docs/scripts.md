# 스크립트 설명서

**경로**: `scripts/kleague_scripts/`

현재 사용 중인 스크립트 4개에 대한 역할, 실행 방법, 주요 함수를 정리합니다.

---

## 1. `ETL_ver4.py` — CSV → SQLite 적재

### 역할
로컬에 저장된 K리그 경기기록 CSV 파일을 읽어 `kleague1.db`에 적재합니다.
포털에서 다운로드한 CSV가 있을 때 사용하는 1회성 적재 도구입니다.

### 실행 방법
```bash
# scripts/kleague_scripts/ 디렉터리에서 실행
python ETL_ver4.py
```
`data/raw/2025_KLEAGUE1/*.csv` 패턴으로 CSV를 자동 탐색하여 적재합니다.
다른 시즌/경로를 적재하려면 `__main__` 블록의 `glob` 경로를 수정하세요.

### 주요 함수

| 함수 | 설명 |
|---|---|
| `import_csv_to_db(csv_path)` | CSV 파일 1개를 읽어 DB에 적재 |
| `insert_dataframe(df)` | DataFrame을 받아 DB에 적재 (다른 스크립트에서 import해서 사용) |
| `clean_numeric_value(value)` | NaN/빈값/"-" → 0 변환 |
| `safe_get_column(row, names)` | 컬럼명 후보 리스트 중 존재하는 것으로 값 추출 |

### 적재 대상 테이블
`competitions` → `teams` → `matches` → `players` → `player_match_stats` 순으로 INSERT OR IGNORE

### 특이사항
- `STAT_MAPPING` dict가 한국어 컬럼명 → DB 컬럼명 매핑을 담당 (컬럼명 표기 차이 흡수)
- `홈여부` 파생: `경기명`의 `(H)` suffix → 1, `(A)` suffix → 0
- `insert_dataframe()`은 `ETL_backpill_stable.py`와 `ETL_scheduler.py`에서 import하여 사용

---

## 2. `ETL_backpill_stable.py` — 포털 스크래핑 + DB 적재

### 역할
K리그 데이터 포털(`data.kleague.com`)을 Selenium으로 스크래핑하여 경기별 선수 스탯을 수집하고 DB에 적재합니다.
CSV가 없는 시즌(과거 시즌 소급 적재)에 사용합니다.

### 실행 방법
```bash
python ETL_backpill_stable.py
```
`__main__` 블록에서 연도·대회·from_round를 지정합니다:
```python
df = scrape_match_data(driver, year_value=2024, meet_value=1, from_round=1)
```

| 파라미터 | 설명 | 예시 |
|---|---|---|
| `year_value` | 시즌 연도 | `2024` |
| `meet_value` | 대회 코드 (포털 드롭다운 value) | `1` = K리그1 |
| `from_round` | 이 라운드부터 수집 (이전 라운드 skip) | `34` = 34R부터 |

### 주요 함수

| 함수 | 설명 |
|---|---|
| `create_driver()` | Chrome 드라이버 생성 + 포털 접속 + 경기 기록 화면으로 이동 |
| `scrape_match_data(driver, year, meet, from_round)` | 팀·경기 드롭다운 순회하며 스탯 수집 → DataFrame 반환 |
| `normalize_to_etl_schema(df)` | 스크래핑 raw 데이터를 DB 적재용 스키마로 정규화 |
| `safe_select(driver, select_id, value)` | StaleElementReferenceException 방어 포함 셀렉트 박스 선택 |
| `restore_state(driver, year, meet, team)` | driver restart 후 드롭다운 상태 복구 |

### 특이사항
- 20경기마다 driver restart (장시간 실행 시 메모리/세션 안정화)
- `from_round` 최적화: 드롭다운 텍스트에서 라운드 번호 파싱 → 페이지 로드 없이 skip
- `insert_dataframe()` 은 `ETL_ver4`에서 import

---

## 3. `ETL_player_master.py` — 선수 마스터 스크래핑

### 역할
K리그 데이터 포털의 **선수 목록 페이지**(`moveMainFrame('0415')`)를 스크래핑하여 선수 인적정보(이름, 포지션, 등번호 등)를 수집합니다.

### 실행 방법
```bash
python ETL_player_master.py
```

### 특이사항
- 경기 기록 스크래핑(`ETL_backpill_stable.py`)과 별개로, 선수 마스터 정보만 별도 수집
- 포털 이동 코드: `moveMainFrame('0415')` (경기 기록은 `'0208'`)
- `insert_dataframe()` 은 `ETL_ver4`에서 import

---

## 4. `ETL_scheduler.py` — 배치 스케줄러

### 역할
`schedule` 테이블(2026시즌 일정 마스터)을 기반으로 **오늘 이전에 경기가 끝났지만 아직 DB에 결과가 없는 경기**를 자동으로 탐색하고 스크래핑합니다.
2026시즌부터 정기 실행하는 메인 운영 도구입니다.

### 실행 방법
```bash
python ETL_scheduler.py
```
파라미터 지정 없이 실행하면 자동으로 미적재 경기를 탐색합니다.

### 동작 순서
1. `print_status()` — schedule 현황 출력 (전체/연결됨/미적재 수)
2. `fetch_unscraped_targets()` — `match_date <= 오늘 AND match_id IS NULL` 조건으로 스크래핑 필요 대상 탐색
3. 대회별 `from_round` 자동 계산 → 이미 적재된 라운드 skip
4. `scrape_match_data()` + `insert_dataframe()` 실행
5. `update_schedule_match_ids()` — `schedule.match_id` 업데이트

### 주요 함수

| 함수 | 설명 |
|---|---|
| `fetch_unscraped_targets()` | 미적재 과거 경기 대상 목록 반환 |
| `update_schedule_match_ids()` | matches와 schedule 조인 → match_id 연결 |
| `print_status()` | 대회·시즌별 적재 현황 테이블 출력 |

### 설정값 (수정 필요 시)
```python
COMPETITION_MEET_MAP = {
    "K리그1": 1,   # 포털 selectMeetSeq value
    "K리그2": 2,   # ← 포털에서 실제 value 확인 필요
}
```

### `schedule.match_id` 연결 조인키
```
(competition_id, round_number, home_team_id, away_team_id)
```
`matches` 테이블의 UNIQUE 제약과 동일 → 1:1 매핑 보장

---

## 스크립트 의존 관계

```
ETL_ver4.py
  └── insert_dataframe()  ←  ETL_backpill_stable.py
                          ←  ETL_player_master.py
                          ←  ETL_scheduler.py (via ETL_backpill_stable)

ETL_backpill_stable.py
  └── scrape_match_data() ←  ETL_scheduler.py
```

---

## 언제 어떤 스크립트를 쓰나

| 상황 | 스크립트 |
|---|---|
| 로컬 CSV 파일이 있고 DB에 적재할 때 | `ETL_ver4.py` |
| 과거 시즌 전체를 포털에서 소급 수집할 때 | `ETL_backpill_stable.py` |
| 선수 인적정보(이름/포지션 등)를 별도 수집할 때 | `ETL_player_master.py` |
| **2026시즌 진행 중 정기 실행** | `ETL_scheduler.py` |
