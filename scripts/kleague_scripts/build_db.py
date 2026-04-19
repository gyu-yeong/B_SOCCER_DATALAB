#!/usr/bin/env python3
"""
build_db.py — player_master + season_roster 재구축 및 player_match_stats.master_id 백필

사용법:
    cd C:\\b_soccer_datalab
    python scripts/kleague_scripts/build_db.py

처리 순서:
    Phase 1a  TM_squads_{season}_KL*.csv   → player_master
              (외국인: name_kor 컬럼 우선 사용; 미기재 시 name_original 임시 등록)
    Phase 1b  선수인적정보.xlsx + player_info/2024·2025시즌.csv → 한글명 갱신
    Phase 2   TM_squads_{season}_KL*.csv   → season_roster
    Phase 3   player_match_stats           → master_id 백필 (player_name 기반)
    Phase 4   리포트
"""

import sys, os, re, glob, sqlite3
import pandas as pd

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DB_PATH     = os.path.join(BASE_DIR, "database", "kleague1.db")
RAW_DIR     = os.path.join(BASE_DIR, "data", "raw")
TM_SQ_DIR   = os.path.join(RAW_DIR, "TM_squads")   # data/raw/TM_squads/


# ═══════════════════════════════════════════════════════════════
# 헬퍼
# ═══════════════════════════════════════════════════════════════

def parse_birth_tm(val) -> str | None:
    """'22/07/1996 (29)'  →  '1996-07-22'"""
    if not val or (isinstance(val, float) and pd.isna(val)):
        return None
    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", str(val))
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else None


def parse_birth_portal(val) -> str | None:
    """pandas Timestamp / '1996.07.22' / '1996-07-22'  →  '1996-07-22'"""
    if val is None:
        return None
    if hasattr(val, "strftime"):          # Timestamp
        return val.strftime("%Y-%m-%d")
    s = str(val).strip()
    if s in ("", "nan", "NaT"):
        return None
    m = re.match(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
    return None


def parse_height(val) -> int | None:
    """'1,85 m'  →  185"""
    if not val or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return int(round(float(str(val).replace(",", ".").replace(" m", "").strip()) * 100))
    except Exception:
        return None


def parse_position(val) -> str | None:
    """'Attack - Centre-Forward'  →  'Attack'"""
    if not val or (isinstance(val, float) and pd.isna(val)):
        return None
    return str(val).split(" - ")[0].strip() or None


def is_non_korean(s: str) -> bool:
    """한글 미포함 이름 — 외국인 임시 이름 판별용 (영문·키릴 등 모두 포함)"""
    return not any('\uAC00' <= c <= '\uD7A3' for c in str(s))


# TM 영문 팀명 → 한글 팀명
TM_TO_KOR: dict[str, str] = {
    "Ulsan HD FC": "울산", "Pohang Steelers": "포항", "Jeju SK": "제주",
    "Jeonbuk Hyundai Motors": "전북", "FC Seoul": "서울",
    "Daejeon Hana Citizen": "대전", "Daegu FC": "대구", "Gangwon FC": "강원",
    "Gwangju FC": "광주", "FC Anyang": "안양", "Suwon FC": "수원FC",
    "Gimcheon Sangmu": "김천", "Incheon United": "인천",
    "Ulsan Hyundai": "울산", "Jeju United": "제주",
    "Suwon Samsung Bluewings": "수원", "Gyeongnam FC": "경남",
    "Jeonnam Dragons": "전남", "Seoul E-Land": "서울E", "Gimpo FC": "김포",
    "Ansan Greeners": "안산", "Seongnam FC": "성남", "Busan IPark": "부산",
    "Chungnam Asan": "충남아산", "Bucheon FC 1995": "부천",
    "Cheonan City": "천안", "Chungbuk Cheongju FC": "충북청주",
    "Hwaseong FC": "화성",
}


# ═══════════════════════════════════════════════════════════════
# DDL
# ═══════════════════════════════════════════════════════════════

DDL = """
CREATE TABLE IF NOT EXISTS player_master (
    master_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    name_kor     TEXT    NOT NULL,
    birth_date   TEXT,
    citizenship  TEXT,
    position     TEXT,
    height_cm    INTEGER,
    tm_player_id TEXT,
    UNIQUE (name_kor, birth_date)
);

CREATE TABLE IF NOT EXISTS season_roster (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    master_id     INTEGER NOT NULL REFERENCES player_master(master_id),
    season        INTEGER NOT NULL,
    team_id       INTEGER NOT NULL REFERENCES teams(team_id),
    jersey_number INTEGER,
    joined_date   TEXT,
    UNIQUE (master_id, season, team_id)
);
"""


def _upsert_player(cur, name_kor, birth_date, citizenship=None, position=None, height_cm=None):
    cur.execute("""
        INSERT INTO player_master (name_kor, birth_date, citizenship, position, height_cm)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(name_kor, birth_date) DO UPDATE SET
            citizenship = COALESCE(player_master.citizenship, excluded.citizenship),
            position    = COALESCE(player_master.position,    excluded.position),
            height_cm   = COALESCE(player_master.height_cm,   excluded.height_cm)
    """, (name_kor, birth_date, citizenship, position, height_cm))


def _get_or_create_team(cur, team_kor: str) -> int:
    cur.execute("INSERT OR IGNORE INTO teams (team_name) VALUES (?)", (team_kor,))
    return cur.execute("SELECT team_id FROM teams WHERE team_name=?", (team_kor,)).fetchone()[0]


# ═══════════════════════════════════════════════════════════════
# Phase 1a  TM_squads CSV → player_master
# ═══════════════════════════════════════════════════════════════

def phase_1a(conn):
    """TM_squads_{season}_KL*.csv를 시즌별 실제 소속 선수 인프라로 사용.
    - 외국인(is_korean != 1): name_kor 컬럼이 채워져 있으면 해당 값으로 등록
                              미기재 시 name_original(영문) 임시 등록 → Phase 1b에서 교체
    - 한국인(is_korean == 1): name_original 그대로 사용 (이미 한글명)
    """
    print("\n[Phase 1a] TM_squads CSV → player_master")
    cur = conn.cursor()

    files = sorted(glob.glob(os.path.join(TM_SQ_DIR, "TM_squads_*.csv")))
    if not files:
        print("  ⚠ TM_squads 파일 없음")
        return

    for path in files:
        df = pd.read_csv(path)
        fname = os.path.basename(path)
        cnt = 0
        for _, row in df.iterrows():
            name_original = str(row.get("name_original") or "").strip()
            birth_date    = parse_birth_portal(row.get("birth_date"))
            if not name_original or not birth_date:
                continue

            # name_kor 결정: CSV name_kor 컬럼 우선, 없으면 name_original
            name_kor_csv = str(row.get("name_kor") or "").strip()
            if name_kor_csv and name_kor_csv.lower() != "nan":
                name_kor = name_kor_csv
            else:
                name_kor = name_original

            citizenship  = str(row.get("citizenship") or "").strip() or None
            position     = parse_position(row.get("position"))
            height_cm    = row.get("height_cm")
            if height_cm is not None:
                try:
                    height_cm = int(float(height_cm))
                except Exception:
                    height_cm = None
            tm_player_id = str(row.get("tm_player_id") or "").strip() or None
            _upsert_player(cur, name_kor, birth_date, citizenship, position, height_cm)
            # tm_player_id 보완
            if tm_player_id:
                cur.execute(
                    "UPDATE player_master SET tm_player_id = COALESCE(tm_player_id, ?) "
                    "WHERE name_kor = ? AND birth_date = ?",
                    (tm_player_id, name_kor, birth_date)
                )
            cnt += 1

        conn.commit()
        total = conn.execute("SELECT COUNT(*) FROM player_master").fetchone()[0]
        print(f"  {fname}: {cnt}행 처리 → player_master 누계 {total}명")


# ═══════════════════════════════════════════════════════════════
# Phase 1b  한글명 갱신 — xlsx(2026) + player_info CSV(2024·2025)
# ═══════════════════════════════════════════════════════════════

def _apply_korean_names(cur, pairs: list[tuple[str, str]]) -> tuple[int, int]:
    """(name_kor, birth_date) 쌍 목록을 player_master에 반영.
    birth_date 1:1 매칭 + 현재 name이 비한글일 때만 UPDATE.
    Returns (updated, inserted).
    """
    updated = inserted = 0
    for name_kor, birth_date in pairs:
        if not name_kor or not birth_date:
            continue
        existing = cur.execute(
            "SELECT master_id, name_kor FROM player_master WHERE birth_date = ?",
            (birth_date,)
        ).fetchall()
        if len(existing) == 1:
            mid, cur_name = existing[0]
            if cur_name != name_kor and is_non_korean(cur_name):
                try:
                    cur.execute(
                        "UPDATE player_master SET name_kor = ? WHERE master_id = ?",
                        (name_kor, mid)
                    )
                    updated += 1
                except sqlite3.IntegrityError:
                    pass
        elif len(existing) == 0:
            try:
                cur.execute(
                    "INSERT INTO player_master (name_kor, birth_date) VALUES (?, ?)",
                    (name_kor, birth_date)
                )
                inserted += 1
            except sqlite3.IntegrityError:
                pass
    return updated, inserted


def phase_1b(conn):
    """한글명 갱신 소스 (우선순위 순):
      1. 선수인적정보.xlsx (2026 K리그 등록 선수 — 생년월일 정밀도 높음)
      2. player_info/2025시즌.csv (2025 시즌 기준 — 해외이적·은퇴 선수 포함)
      3. player_info/2024시즌.csv (2024 시즌 기준 — 양민혁 등 추가 커버)
    한 번 한글명으로 갱신된 선수는 이후 소스에서 덮어쓰지 않음.
    """
    print("\n[Phase 1b] 한글명 갱신 (xlsx 2026 + player_info 2024·2025)")
    cur = conn.cursor()
    total_updated = total_inserted = 0

    # ── 소스 1: 선수인적정보.xlsx ──────────────────────────────
    xlsx_path = os.path.join(RAW_DIR, "2026_KLEAGUE", "선수인적정보.xlsx")
    if os.path.exists(xlsx_path):
        df = pd.read_excel(xlsx_path)
        col_name = col_bdate = None
        for c in df.columns:
            lc = str(c).strip().lower()
            if lc in ("성명", "선수명", "이름", "name"):
                col_name = c
            elif lc in ("생년월일", "birth_date", "생일"):
                col_bdate = c
        if col_name and col_bdate:
            print(f"  컬럼 탐지: 성명={col_name}, 생년월일={col_bdate}")
            pairs = [
                (str(r[col_name] or "").strip(), parse_birth_portal(r[col_bdate]))
                for _, r in df.iterrows()
            ]
            u, i = _apply_korean_names(cur, pairs)
            conn.commit()
            total_updated += u; total_inserted += i
            print(f"  [xlsx 2026] 갱신 {u}명 / 신규 {i}명")
        else:
            print(f"  ⚠ xlsx 컬럼 탐지 실패: {df.columns.tolist()}")
    else:
        print("  ⚠ 선수인적정보.xlsx 없음 — 스킵")

    # ── 소스 2·3: player_info/{season}시즌.csv ────────────────
    for season in ["2025", "2024"]:
        csv_path = os.path.join(RAW_DIR, "player_info", f"{season}시즌.csv")
        if not os.path.exists(csv_path):
            continue
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        name_col = next((c for c in df.columns if "home country" in c.lower()), None)
        bd_col   = next((c for c in df.columns if "birth" in c.lower()), None)
        if not name_col or not bd_col:
            print(f"  ⚠ player_info/{season}시즌.csv 컬럼 탐지 실패")
            continue
        pairs = [
            (str(r[name_col] or "").strip(), parse_birth_tm(r[bd_col]))
            for _, r in df.iterrows()
        ]
        u, i = _apply_korean_names(cur, pairs)
        conn.commit()
        total_updated += u; total_inserted += i
        print(f"  [player_info {season}] 갱신 {u}명 / 신규 {i}명")

    print(f"  → 음차명 갱신: {total_updated}명 / 신규 추가: {total_inserted}명")


# ═══════════════════════════════════════════════════════════════
# Phase 1b_ext  birth_date 충돌 선수 팀 기반 disambiguation
# ═══════════════════════════════════════════════════════════════

def phase_1b_disambig(conn):
    """Phase 1b 보완: birth_date 중복으로 갱신 못한 한국인 선수를
    pms 소속팀 + TM squad CSV 교차로 특정하여 한글명으로 갱신.

    전략:
    1. pms에서 아직 master_id=NULL인 한국인 선수(player_info birth_date 존재)를 수집
    2. 해당 선수의 팀(pms.team_id→teams.team_name)을 확인
    3. TM squad CSV에서 같은 팀 + 같은 birth_date인 name_original 조회
    4. player_master에서 (name_kor=name_original, birth_date) 엔트리를 한글명으로 UPDATE
    """
    print("\n[Phase 1b_ext] birth_date 충돌 선수 팀 기반 disambiguation")
    cur = conn.cursor()

    # player_info CSV에서 (한글명, birth_date) 매핑 로드
    pi_map: dict[str, str] = {}  # name_kor → birth_date
    for season in ["2026", "2025", "2024"]:
        path = os.path.join(RAW_DIR, "player_info", f"{season}시즌.csv")
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path, encoding="utf-8-sig")
        nc = next((c for c in df.columns if "home country" in c.lower()), None)
        bc = next((c for c in df.columns if "birth" in c.lower()), None)
        if not nc or not bc:
            continue
        for _, r in df.iterrows():
            name = str(r[nc] or "").strip()
            bd   = parse_birth_tm(r[bc])
            if name and bd and name not in pi_map:
                pi_map[name] = bd

    # TM squad CSV에서 (team_kor, birth_date) → name_original 매핑
    tm_bd_team: dict[tuple, str] = {}  # (team_kor, birth_date) → name_original
    for path in sorted(glob.glob(os.path.join(TM_SQ_DIR, "TM_squads_*.csv"))):
        df = pd.read_csv(path)
        for _, row in df.iterrows():
            bd = parse_birth_portal(row.get("birth_date"))
            team_kor = TM_TO_KOR.get(str(row.get("team_name", "")).strip())
            name_orig = str(row.get("name_original") or "").strip()
            if bd and team_kor and name_orig:
                tm_bd_team[(team_kor, bd)] = name_orig

    # pms에서 미매핑 한국인 선수 수집 (player_name, team_name)
    unmapped = cur.execute("""
        SELECT DISTINCT p.player_name, t.team_name
        FROM player_match_stats pms
        JOIN players p ON pms.player_id = p.player_id
        JOIN teams t   ON pms.team_id   = t.team_id
        WHERE pms.master_id IS NULL
    """).fetchall()

    updated = 0
    for player_name, team_name in unmapped:
        birth_date = pi_map.get(player_name)
        if not birth_date:
            continue

        # player_master에 이미 한글명으로 존재하면 스킵 (Phase 3 문제)
        if cur.execute(
            "SELECT 1 FROM player_master WHERE name_kor=?", (player_name,)
        ).fetchone():
            continue

        # TM squad에서 해당 팀+생년월일 name_original 조회
        name_orig = tm_bd_team.get((team_name, birth_date))
        if not name_orig:
            continue

        # player_master에서 (name_kor=name_orig, birth_date) 엔트리 찾아 UPDATE
        pm = cur.execute(
            "SELECT master_id, name_kor FROM player_master WHERE name_kor=? AND birth_date=?",
            (name_orig, birth_date)
        ).fetchone()
        if pm and is_non_korean(pm[1]):
            try:
                cur.execute(
                    "UPDATE player_master SET name_kor=? WHERE master_id=?",
                    (player_name, pm[0])
                )
                updated += 1
            except sqlite3.IntegrityError:
                pass

    conn.commit()
    print(f"  → 팀 기반 disambiguation 갱신: {updated}명")


# ═══════════════════════════════════════════════════════════════
# Phase 2  TM squad CSV → season_roster
# ═══════════════════════════════════════════════════════════════

def phase_2(conn):
    print("\n[Phase 2] TM squad CSV → season_roster")
    cur = conn.cursor()

    inserted = no_master = no_team = 0

    for path in sorted(glob.glob(os.path.join(TM_SQ_DIR, "TM_squads_*.csv"))):
        df = pd.read_csv(path)
        fname = os.path.basename(path)
        file_inserted = 0

        for _, row in df.iterrows():
            birth_date = parse_birth_portal(row.get("birth_date") or row.get("Date of birth/Age"))
            if not birth_date:
                no_master += 1
                continue

            team_kor = TM_TO_KOR.get(str(row.get("team_name", "")).strip())
            if not team_kor:
                no_team += 1
                continue

            try:
                season = int(row["season"])
            except Exception:
                no_master += 1
                continue

            jn_raw = str(row.get("jersey_number", "")).strip()
            jersey_number = None
            if jn_raw not in ("-", "", "nan"):
                try:
                    jersey_number = int(float(jn_raw))
                except Exception:
                    pass

            joined_date  = str(row.get("joined") or "").strip() or None
            tm_player_id = str(row.get("tm_player_id") or "").strip() or None

            # master_id 조회 (birth_date 기준)
            masters = cur.execute(
                "SELECT master_id FROM player_master WHERE birth_date = ?",
                (birth_date,)
            ).fetchall()

            if len(masters) == 0:
                no_master += 1
                continue
            elif len(masters) == 1:
                master_id = masters[0][0]
            else:
                # 동일 생년월일 복수 → 영문명으로 시도
                name_eng = str(row.get("name_original") or "").strip()
                pm = cur.execute(
                    "SELECT master_id FROM player_master WHERE birth_date=? AND name_kor=?",
                    (birth_date, name_eng)
                ).fetchone()
                if pm:
                    master_id = pm[0]
                else:
                    no_master += 1
                    continue

            team_id = _get_or_create_team(cur, team_kor)

            # tm_player_id 보완
            if tm_player_id:
                cur.execute(
                    "UPDATE player_master SET tm_player_id = COALESCE(tm_player_id, ?) WHERE master_id = ?",
                    (tm_player_id, master_id)
                )

            try:
                cur.execute("""
                    INSERT INTO season_roster (master_id, season, team_id, jersey_number, joined_date)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(master_id, season, team_id) DO UPDATE SET
                        jersey_number = COALESCE(season_roster.jersey_number, excluded.jersey_number),
                        joined_date   = COALESCE(season_roster.joined_date,   excluded.joined_date)
                """, (master_id, season, team_id, jersey_number, joined_date))
                inserted += 1
                file_inserted += 1
            except sqlite3.IntegrityError:
                pass

        conn.commit()
        print(f"  {fname}: {file_inserted}건 처리")

    total = conn.execute("SELECT COUNT(*) FROM season_roster").fetchone()[0]
    print(f"  → season_roster 합계: {total}건 (no_master={no_master}, no_team={no_team})")


# ═══════════════════════════════════════════════════════════════
# Phase 3  player_match_stats.master_id 백필
# ═══════════════════════════════════════════════════════════════

def phase_3(conn):
    print("\n[Phase 3] player_match_stats.master_id 백필")
    cur = conn.cursor()

    # master_id 컬럼 추가 (없을 시)
    cols = [r[1] for r in cur.execute("PRAGMA table_info(player_match_stats)").fetchall()]
    if "master_id" not in cols:
        cur.execute(
            "ALTER TABLE player_match_stats ADD COLUMN master_id INTEGER "
            "REFERENCES player_master(master_id)"
        )
        conn.commit()
        print("  master_id 컬럼 추가 완료")

    tables = [r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    if "players" not in tables:
        print("  players 테이블 없음 — 백필 불가")
        return

    # 백필 대상 rows
    rows = cur.execute(
        "SELECT stat_id, player_id, team_id FROM player_match_stats WHERE master_id IS NULL"
    ).fetchall()
    print(f"  백필 대상: {len(rows)}건")

    # 캐시
    pid_to_name:   dict[int, str | None]  = {}
    name_to_master: dict[str, int | None] = {}

    updated = no_match = 0

    for stat_id, player_id, team_id in rows:

        # player_id → player_name
        if player_id not in pid_to_name:
            r = cur.execute(
                "SELECT player_name FROM players WHERE player_id=?", (player_id,)
            ).fetchone()
            pid_to_name[player_id] = r[0] if r else None

        pname = pid_to_name[player_id]
        if not pname:
            no_match += 1
            continue

        # player_name → master_id
        if pname not in name_to_master:
            masters = cur.execute(
                "SELECT master_id FROM player_master WHERE name_kor=?", (pname,)
            ).fetchall()

            if len(masters) == 1:
                name_to_master[pname] = masters[0][0]
            elif len(masters) > 1:
                # 동명이인 → team_id 기반 season_roster로 disambiguation
                sr = cur.execute("""
                    SELECT sr.master_id FROM season_roster sr
                    WHERE sr.team_id = ?
                      AND sr.master_id IN (
                          SELECT master_id FROM player_master WHERE name_kor = ?
                      )
                    LIMIT 1
                """, (team_id, pname)).fetchone()
                name_to_master[pname] = sr[0] if sr else None
            else:
                name_to_master[pname] = None

        master_id = name_to_master[pname]
        if master_id:
            cur.execute(
                "UPDATE player_match_stats SET master_id=? WHERE stat_id=?",
                (master_id, stat_id)
            )
            updated += 1
        else:
            no_match += 1

        if (updated + no_match) % 2000 == 0:
            conn.commit()

    conn.commit()

    total  = conn.execute("SELECT COUNT(*) FROM player_match_stats").fetchone()[0]
    filled = conn.execute(
        "SELECT COUNT(*) FROM player_match_stats WHERE master_id IS NOT NULL"
    ).fetchone()[0]
    pct    = 100 * filled // total if total else 0

    print(f"  → 성공: {updated}건 / 미매칭: {no_match}건")
    print(f"  → master_id 확보율: {filled}/{total} ({pct}%)")


# ═══════════════════════════════════════════════════════════════
# 메인
# ═══════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("DB 재구축: player_master + season_roster + master_id 백필")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    # 기존 테이블 드롭 후 재생성
    print("\n[초기화] player_master / season_roster 드롭 및 재생성")
    cur.execute("DROP TABLE IF EXISTS season_roster")
    cur.execute("DROP TABLE IF EXISTS player_master")
    conn.commit()
    for stmt in DDL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            cur.execute(stmt)
    conn.commit()

    # player_master 재구축 시 master_id 전체 재부여 → 기존 pms master_id 무효
    # Phase 3에서 올바르게 재백필되도록 전체 NULL 리셋
    cur.execute("UPDATE player_match_stats SET master_id = NULL")
    conn.commit()
    print("  → player_master / season_roster 재생성 완료")
    print("  → player_match_stats.master_id 전체 NULL 리셋 완료")

    phase_1a(conn)
    phase_1b(conn)
    phase_1b_disambig(conn)
    phase_2(conn)
    phase_3(conn)

    # ── 최종 리포트 ──────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("최종 현황")
    print("=" * 60)

    pm    = conn.execute("SELECT COUNT(*) FROM player_master").fetchone()[0]
    pm_bd = conn.execute("SELECT COUNT(*) FROM player_master WHERE birth_date IS NULL").fetchone()[0]
    sr    = conn.execute("SELECT COUNT(*) FROM season_roster").fetchone()[0]
    sr_nj = conn.execute("SELECT COUNT(*) FROM season_roster WHERE jersey_number IS NULL").fetchone()[0]
    pms_t = conn.execute("SELECT COUNT(*) FROM player_match_stats").fetchone()[0]
    pms_f = conn.execute("SELECT COUNT(*) FROM player_match_stats WHERE master_id IS NOT NULL").fetchone()[0]

    print(f"  player_master       : {pm}명  (birth_date 없음 {pm_bd}명)")
    print(f"  season_roster       : {sr}건  (등번호 없음 {sr_nj}건)")
    print(f"  player_match_stats  : {pms_t}건  master_id 확보 {pms_f}건 "
          f"({100*pms_f//pms_t if pms_t else 0}%)")

    conn.close()
    print("\n✅ 완료")


if __name__ == "__main__":
    main()
