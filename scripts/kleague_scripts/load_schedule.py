#!/usr/bin/env python3
"""
load_schedule.py — KLEAGUE_SCHEDULE CSV → schedule 테이블 적재

대상 파일:
    data/raw/KLEAGUE_SCHEDULE/kleague1_2024.csv
    data/raw/KLEAGUE_SCHEDULE/kleague2_2024.csv
    data/raw/KLEAGUE_SCHEDULE/kleague1_2025.csv
    data/raw/KLEAGUE_SCHEDULE/kleague2_2025.csv
    data/raw/KLEAGUE_SCHEDULE/kleague1_2026.csv  (미래 일정, 점수·관중 없음)

CSV 포맷:
    2024/2025: INFO, DATE, ROUND, TIME, HOME, HOME_SCR, AWAY_SCR, AWAY, STADIUM, CROWD
    2026     : INFO, ROUND, DATE, TIME, HOME, AWAY, STADIUM  (점수·관중 없음)

실행:
    python scripts/kleague_scripts/load_schedule.py
"""

import sys, os, csv, re, sqlite3

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE_DIR  = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DB_PATH   = os.path.join(BASE_DIR, "database", "kleague.db")
SCHED_DIR = os.path.join(BASE_DIR, "data", "raw", "KLEAGUE_SCHEDULE")

# ── CSV 파일 목록 (INFO 컬럼값, 연도)
CSV_FILES = [
    ("kleague1_2024.csv", "K리그1", 2024),
    ("kleague2_2024.csv", "K리그2", 2024),
    ("kleague1_2025.csv", "K리그1", 2025),
    ("kleague2_2025.csv", "K리그2", 2025),
    ("kleague1_2026.csv", "K리그1", 2026),
]


def normalize_round(raw: str) -> str:
    """'1R' 또는 'R1' → '1R' 형식으로 통일 (포털·matches 테이블 형식과 일치)"""
    m = re.search(r"\d+", raw)
    return f"{m.group()}R" if m else raw.strip()


def normalize_date(raw: str) -> str:
    """'2024.03.01' → '2024-03-01'"""
    return raw.strip().replace(".", "-")


def safe_int(val: str):
    try:
        v = str(val).strip()
        return int(v) if v else None
    except (ValueError, TypeError):
        return None


def get_or_create_competition(cur: sqlite3.Cursor, year: int, info: str) -> tuple[int, str]:
    """competitions 테이블 조회·등록. 후원사명·연도 표기 차이를 LIKE로 흡수."""
    # 키워드 추출: K리그1/K리그2/슈퍼컵
    if "K리그1" in info:
        keyword = "K리그1"
    elif "K리그2" in info:
        keyword = "K리그2"
    elif "슈퍼컵" in info:
        keyword = "슈퍼컵"
    else:
        keyword = info  # 알 수 없는 대회는 그대로

    row = cur.execute(
        "SELECT competition_id, competition_name FROM competitions "
        "WHERE year = ? AND competition_name LIKE ?",
        (year, f"%{keyword}%"),
    ).fetchone()
    if row:
        return row[0], row[1]

    # 없으면 신규 삽입
    comp_name = f"{keyword} {year}"
    cur.execute(
        "INSERT OR IGNORE INTO competitions (year, competition_name) VALUES (?, ?)",
        (year, comp_name),
    )
    row = cur.execute(
        "SELECT competition_id, competition_name FROM competitions WHERE year=? AND competition_name=?",
        (year, comp_name),
    ).fetchone()
    print(f"  [competitions] 신규 등록: {comp_name} (id={row[0]})")
    return row[0], row[1]


def build_team_map(cur: sqlite3.Cursor) -> dict[str, int]:
    rows = cur.execute("SELECT team_name, team_id FROM teams").fetchall()
    return {name: tid for name, tid in rows}


def load_csv(
    cur: sqlite3.Cursor,
    fname: str,
    info_label: str,
    year: int,
    team_map: dict[str, int],
) -> tuple[int, int, int]:
    """단일 CSV 적재. (inserted, skipped, unmapped) 반환

    각 row의 INFO 컬럼을 기준으로 competition을 분리 적재.
    info_label은 INFO 컬럼이 비어있을 때만 fallback으로 사용.
    (kleague1_2026.csv처럼 한 파일에 K리그1+K리그2가 섞여있는 경우 대응)
    """
    fpath = os.path.join(SCHED_DIR, fname)
    if not os.path.exists(fpath):
        print(f"  ⚠ 파일 없음: {fpath}")
        return 0, 0, 0

    inserted = skipped = unmapped = 0
    comp_cache: dict[str, tuple[int, str]] = {}  # info_value → (cid, name)

    with open(fpath, encoding="utf-8-sig") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            # INFO 컬럼 우선, 비어있으면 파일 단위 label fallback
            info_value = (row.get("INFO") or "").strip() or info_label
            if info_value not in comp_cache:
                comp_cache[info_value] = get_or_create_competition(cur, year, info_value)
            competition_id, competition_name = comp_cache[info_value]

            home_name = row.get("HOME", "").strip()
            away_name = row.get("AWAY", "").strip()
            round_num  = normalize_round(row.get("ROUND", ""))
            match_date = normalize_date(row.get("DATE", ""))
            match_time = row.get("TIME", "").strip() or None
            stadium    = row.get("STADIUM", "").strip() or None

            # 점수 (2026은 없음)
            home_score = safe_int(row.get("HOME_SCR", ""))
            away_score = safe_int(row.get("AWAY_SCR", ""))

            # 관중수: '관중수' 컬럼 우선(2025 KL1은 CROWD=경기장명, 관중수=실수치)
            if "관중수" in row and str(row["관중수"]).strip():
                crowd_raw = row["관중수"].strip()
            else:
                crowd_raw = row.get("CROWD", "").strip()
            attendance = safe_int(crowd_raw)

            # 팀 매핑
            home_id = team_map.get(home_name)
            away_id = team_map.get(away_name)
            if not home_id or not away_id:
                print(f"  ⚠ 팀 미매핑: {home_name!r} vs {away_name!r} — 스킵")
                unmapped += 1
                continue

            try:
                cur.execute("""
                    INSERT INTO schedule
                        (competition_id, competition_name, season,
                         round_number, match_date, match_time,
                         home_team_id, away_team_id,
                         home_team_name, away_team_name,
                         stadium, home_score, away_score, attendance)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(competition_id, round_number, home_team_id, away_team_id)
                    DO UPDATE SET
                        home_score  = excluded.home_score,
                        away_score  = excluded.away_score,
                        attendance  = excluded.attendance,
                        match_date  = excluded.match_date,
                        match_time  = excluded.match_time,
                        stadium     = excluded.stadium,
                        season      = excluded.season
                """, (
                    competition_id, competition_name, year,
                    round_num, match_date, match_time,
                    home_id, away_id,
                    home_name, away_name,
                    stadium, home_score, away_score, attendance,
                ))
                inserted += 1
            except sqlite3.IntegrityError as e:
                skipped += 1
                print(f"  IntegrityError: {e} — {home_name} vs {away_name} R{round_num}")

    return inserted, skipped, unmapped


def migrate_schema(conn: sqlite3.Connection) -> None:
    """신규 컬럼 추가 (이미 있으면 무시)"""
    cur = conn.cursor()
    existing = {row[1] for row in cur.execute("PRAGMA table_info(schedule)").fetchall()}
    adds = [
        ("season",     "INTEGER"),
        ("home_score", "INTEGER"),
        ("away_score", "INTEGER"),
        ("attendance", "INTEGER"),
    ]
    for col, dtype in adds:
        if col not in existing:
            cur.execute(f"ALTER TABLE schedule ADD COLUMN {col} {dtype}")
            print(f"  [schema] ADD COLUMN schedule.{col} {dtype}")
    conn.commit()


def main() -> None:
    print("=" * 60)
    print("KLEAGUE_SCHEDULE CSV → schedule 테이블 적재")
    print("=" * 60)

    if not os.path.exists(DB_PATH):
        print(f"⚠ DB 없음: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    # 1. 스키마 마이그레이션
    print("\n[스키마 마이그레이션]")
    migrate_schema(conn)

    cur = conn.cursor()
    team_map = build_team_map(cur)

    # 2. CSV 적재
    total_ins = total_skip = total_unmap = 0
    for fname, info_label, year in CSV_FILES:
        print(f"\n[{fname}]")
        ins, skip, unmap = load_csv(cur, fname, info_label, year, team_map)
        print(f"  → INSERT/UPDATE {ins}건 | 중복스킵 {skip}건 | 팀미매핑 {unmap}건")
        total_ins  += ins
        total_skip += skip
        total_unmap += unmap

    conn.commit()

    # 3. 결과 요약
    total = cur.execute("SELECT COUNT(*) FROM schedule").fetchone()[0]
    by_season = cur.execute(
        "SELECT season, COUNT(*) FROM schedule GROUP BY season ORDER BY season"
    ).fetchall()

    print("\n" + "=" * 60)
    print(f"총 처리: INSERT/UPDATE {total_ins}건 | 스킵 {total_skip}건 | 미매핑 {total_unmap}건")
    print(f"schedule 총 {total}건")
    for s, cnt in by_season:
        print(f"  {s}시즌: {cnt}건")

    # 4. 관중수 적재 현황
    att = cur.execute(
        "SELECT season, COUNT(*) FROM schedule WHERE attendance IS NOT NULL GROUP BY season ORDER BY season"
    ).fetchall()
    print("\n[관중수 적재 현황]")
    for s, cnt in att:
        print(f"  {s}시즌: {cnt}건")

    conn.close()
    print("\n✅ 완료")


if __name__ == "__main__":
    main()
