# 스크립트 설명서

현재 사용 중인 스크립트에 대한 역할, 실행 방법, 주요 함수를 정리합니다.

> **아카이브 경로**
> - `scripts/kleague_scripts/_archive/` — ETL_player_master.py 외 구버전 ETL
> - `scripts/player_info/_archive/` — build_player_master.py, map_foreign_korean_names.py, import_korean_names.py (build_db.py + name_kor 직접 입력 방식으로 대체)

---

## player_info 스크립트 (`scripts/player_info/`)

### 1. `scrape_tm_squads.py` — Transfermarkt 스쿼드 자동 스크래핑

#### 역할
Transfermarkt에서 K League 1·2 전 팀의 스쿼드 상세 정보를 자동 수집합니다.
CLI 인자로 시즌과 리그를 지정하면 `data/raw/TM_squads/` 하위에 CSV로 저장합니다.
**DB 적재는 이 스크립트에서 수행하지 않으며 `build_db.py`가 담당합니다.**

#### 실행 방법
```bash
# 프로젝트 루트에서 실행
python scripts/player_info/scrape_tm_squads.py --season 2026
python scripts/player_info/scrape_tm_squads.py --season 2024 --league kl1
python scripts/player_info/scrape_tm_squads.py --season 2025 --league kl2
```

#### 옵션
| 인자 | 필수 | 설명 |
|------|------|------|
| `--season` | ✅ | 수집할 시즌 연도 (예: 2026) |
| `--league` | ❌ | `kl1` / `kl2` / `all` (기본값: `all`) |

#### 탐색 전략 (URL 직접 구성, 2단계)
1. **대회 페이지** → 팀 slug·tm_id 동적 수집
   `https://www.transfermarkt.com/k-league-1/startseite/wettbewerb/RSK1/plus/?saison_id={season-1}`
2. **스쿼드 페이지** → 선수별 상세 정보 파싱 (Detailed 뷰 `/plus/1`, 포지션별 전체 테이블 순회)
   `https://www.transfermarkt.com/{slug}/kader/verein/{tm_id}/plus/1?saison_id={season-1}`

> **saison_id 공식**: `saison_id = 실제시즌 - 1` (K리그 캘린더 시즌 TM 표기 규칙, 예: 2026시즌 → saison_id=2025)

#### 수집 컬럼 (CSV)
`jersey_number`, `name_original`, `tm_player_id`, `position`, `birth_date`,
`citizenship`, `height_cm`, `foot`, `joined`, `signed_from`, `contract_until`,
`market_value_eur`, `is_korean`, `player_name`, `team_name`, `league`, `season`

#### 입출력
| 구분 | 경로 |
|---|---|
| 입력 | transfermarkt.com (웹) |
| 출력 | `data/raw/TM_squads/TM_squads_{season}_KL1.csv` |
| 출력 | `data/raw/TM_squads/TM_squads_{season}_KL2.csv` |

#### 핵심 함수

| 함수 | 설명 |
|---|---|
| `get_col_map(table)` | `thead > th` 텍스트 → 셀 인덱스 dict 생성 (헤더 기반 동적 매핑) |
| `_cell_text(cells, col_map, key)` | col_map 키로 셀 텍스트 추출 (인덱스 범위 안전 처리) |
| `parse_position(val)` | `"Attack - Right Winger"` → `"Attack"` (detail 제거) |
| `_parse_player_row(tr, col_map, ...)` | 1개 `<tr>` → 선수 데이터 dict 변환 |
| `get_team_list(sess, season, league)` | 대회 페이지에서 팀 slug·tm_id 목록 수집 (재시도 3회) |
| `scrape_squad(sess, team, season, ...)` | 팀 kader 페이지 파싱 — 포지션별 `table.items` 전체 순회 → 선수 목록 반환 |

#### 특이사항
- `requests.Session` 기반 (Selenium 미사용), 팀 간 4~8초 랜덤 딜레이
- **헤더 기반 col_map**: `thead > th` 파싱으로 컬럼 인덱스를 동적 결정
- **포지션별 테이블 전체 파싱**: `select("table.items")`로 GK/DEF/MID/FWD 각 테이블 모두 순회 (v0.7.3 수정)
- `jersey_number`는 시즌마다 달라지므로 CSV에만 보존, DB 적재 제외
- `is_korean`: `citizenship`에 "Korea" 포함 여부로 판정
- `tm_player_id`: 선수 링크 `/spieler/(\d+)` 정규식으로 추출, COALESCE로 기존값 우선 보존
- `player_name`은 INSERT 시 항상 NULL (한국명은 별도 매핑 단계에서 채움)

---

### 2. `backfill_tm_player_id.py` — 기존 player_master에 tm_player_id 백필

#### 역할
`scrape_tm_squads.py` 도입 이전에 수동 CSV로 적재된 기존 `player_master` 레코드의
`tm_player_id`를 Transfermarkt kader 페이지 스크래핑으로 역으로 채운다.
이름 표기 차이(한글명 vs 영문명)에 의존하지 않고 **생년월일 기준**으로 매칭한다.

#### 실행 방법
```bash
# 2026시즌 기준으로 전체 백필 (첫 실행 시 권장)
python scripts/player_info/backfill_tm_player_id.py --season 2026

# K League 1만
python scripts/player_info/backfill_tm_player_id.py --season 2026 --league kl1
```

#### 매칭 전략 (3단계)
| 순위 | 조건 | 처리 |
|------|------|------|
| 1순위 | `birth_date` 정확 일치 + DB 내 유일 후보 | 자동 UPDATE |
| 2순위 | `birth_date` 일치 + 복수 후보 → `current_club` 유사도 필터 후 1명 | 자동 UPDATE |
| 3순위 | 매칭 실패 | `data/raw/unmatched_tm_id_{season}.csv` 출력 (수동 처리) |

#### 입출력
| 구분 | 경로 |
|---|---|
| 입력 | transfermarkt.com kader 페이지 (웹) |
| 입력 | `database/kleague.db` → `player_master` (tm_player_id IS NULL) |
| 출력 | `database/kleague.db` → `player_master.tm_player_id` UPDATE |
| 출력 | `data/raw/unmatched_tm_id_{season}.csv` (수동 처리 대상) |

#### 특이사항
- `tm_player_id` 컬럼 없으면 자동 `ALTER TABLE` (멱등 실행 가능)
- 이미 `tm_player_id`가 채워진 레코드는 건드리지 않음
- 같은 생년월일 선수가 여러 명일 때: `current_club` 유사도로 2차 필터
- 수동 처리: `unmatched_tm_id_{season}.csv` 확인 후 `UPDATE player_master SET tm_player_id = '...' WHERE master_id = ...`

---

### `patch_homonym_masters.py` — 동명이인 master_id 수동 패치

#### 역할
`build_db.py Phase 3`의 team 기반 disambiguation이 실패한 동명이인 선수에 대해
`(player_name, team_id)` 조합으로 `player_match_stats.master_id`를 직접 UPDATE합니다.

#### 실행 방법
```bash
# build_db.py 실행 직후 실행
python scripts/kleague_scripts/patch_homonym_masters.py
```

#### 패치 규칙 (`PATCH_RULES`)
| player_name | team_id | master_id | TM 원명 | 비고 |
|-------------|---------|-----------|---------|------|
| 가브리엘 | 30078 (강원) | 571 | Vitor Gabriel (2000-01-20) | 임시 패치 |
| 가브리엘 | 30158 (광주) | 677 | Gabriel Tigrão (2001-10-13) | 임시 패치 |
| 마테우스 | 29757 (울산) | 162 | Matheus Sales (1995-05-13) | 임시 패치 |
| 마테우스 | 29758 (안양) | 1925 | Matheus Oliveira (1997-09-28) | 임시 패치 |

#### 특이사항
- `build_db.py` 실행 후 항상 재실행 필요 (build_db가 master_id 전체 리셋)
- 향후 K리그 포털 등록명이 구분명으로 확정되면 `TM_squads CSV name_kor` 수정 + `build_db.py` 재실행으로 이 스크립트 대체 가능

---

## ETL 스크립트 (`scripts/kleague_scripts/`)

**경로**: `scripts/kleague_scripts/`

현재 사용 중인 스크립트 5개에 대한 역할, 실행 방법, 주요 함수를 정리합니다.

---

## 0. `build_db.py` — player_master + season_roster 재구축 (v0.8.0)

### 역할
player_master 테이블과 season_roster 테이블을 전면 재구축하고, `player_match_stats.master_id`를 백필합니다.
TM squad CSV + 선수인적정보.xlsx + player_info CSV + players 테이블을 조합하여 선수 인물 원장을 구축합니다.

### 실행 방법
```bash
# 프로젝트 루트에서 실행
cd C:\b_soccer_datalab
python scripts/kleague_scripts/build_db.py
```
기존 `player_master`, `season_roster` 테이블을 DROP 후 재생성합니다. (player_match_stats는 유지)

### 처리 단계 (Phase)

| Phase | 소스 | 내용 |
|---|---|---|
| 1a | `data/raw/TM_squads/TM_squads_*.csv` | name_kor 컬럼 우선 사용 (외국인 한글 등록명 직접 반영); 미기재 시 name_original(영문) 임시 등록 |
| 1b | `data/raw/2026_KLEAGUE/선수인적정보.xlsx` + `player_info/` CSV | birth_date 기준으로 비한글명 → 한글명 갱신 (xlsx 2026 → 2025 → 2024 순) |
| 1b_ext | `players` 테이블 + TM squad CSV | birth_date 충돌 선수 팀 기반 disambiguation (93명) |
| 2 | `data/raw/TM_squads/TM_squads_*.csv` | birth_date → master_id → season_roster 적재 |
| 3 | `player_match_stats` 전체 | name_kor 기반 master_id 백필 |

> Phase 1c (players 테이블 + jersey/team 기반 역매핑)는 v0.8.0에서 삭제.
> Phase 1a `name_kor` 직접 등록 방식으로 대체.

### 외국인 선수 한글명 입력 워크플로

TM squad CSV의 `name_kor` 컬럼에 K리그 포털 등록명을 직접 기입한다.
입력 보조 파일 경로: `data/raw/TM_squads/name_kor_input/foreign_namekor_{season}_{league}.csv`

1. `scrape_tm_squads.py`로 TM_squads CSV 수집
2. 외국인 행만 별도 CSV로 추출 → `name_kor_input/` 저장 (자동화 스크립트 또는 수동)
3. `name_kor` 컬럼에 포털 등록명 수기 입력
4. `name_kor_input/` CSV → `TM_squads_*.csv` 병합 (tm_player_id 매칭)
5. `build_db.py` 재실행

### 주요 함수

| 함수 | 설명 |
|---|---|
| `phase_1a(conn)` | TM_squads CSV → player_master 초기 적재 (name_kor 컬럼 우선) |
| `phase_1b(conn)` | 선수인적정보.xlsx + player_info → 비한글명 → 한글명 갱신 |
| `phase_1b_disambig(conn)` | birth_date 충돌 선수 팀 기반 disambiguation |
| `phase_2(conn)` | TM squad CSV → season_roster 적재 |
| `phase_3(conn)` | player_match_stats.master_id 백필 |
| `_upsert_player(cur, ...)` | player_master UPSERT (ON CONFLICT DO UPDATE) |
| `is_non_korean(s)` | 한글 미포함 이름 판별 (영문·키릴 포함) |
| `parse_birth_tm(val)` | `"22/07/1996 (29)"` → `"1996-07-22"` |
| `parse_birth_portal(val)` | Timestamp / `"1996.07.22"` → `"1996-07-22"` |

### 특이사항
- player_master UNIQUE: `(name_kor, birth_date)` — ETL 매핑 기본 키
- season_roster UNIQUE: `(master_id, season, team_id)` — jersey_number는 UNIQUE 아님 (여름 이적시장 재배정)
- Phase 3 동명이인 처리: 동일 name_kor 2개 이상 → season_roster(team_id) 교차 필터
- 재실행 안전: 멱등 설계 (DROP → CREATE → Phase 순서)

---

## 1. `ETL_ver4.py` — CSV → SQLite 적재

### 역할
로컬에 저장된 K리그 경기기록 CSV 파일을 읽어 `kleague.db`에 적재합니다.
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
`competitions` → `teams` → `matches` → `players` → **`player_master (name_kor 조회)`** → `player_match_stats` 순으로 처리

### 특이사항
- `STAT_MAPPING` dict가 한국어 컬럼명 → DB 컬럼명 매핑을 담당 (컬럼명 표기 차이 흡수)
- `홈여부` 파생: `경기명`의 `(H)` suffix → 1, `(A)` suffix → 0
- `insert_dataframe()`은 `ETL_backpill_stable.py`와 `ETL_scheduler.py`에서 import하여 사용
- **v0.7.0**: `master_id` 조회 로직 추가 — `name_kor → player_master` 1순위, `season_roster(team_id)` disambiguation 2순위
- `player_match_stats` INSERT에 `master_id` 컬럼 포함 (player_id는 레거시로 유지)

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
