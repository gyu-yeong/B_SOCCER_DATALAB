"""
player_master 테이블 생성 ETL 스크립트
Source: 선수정보.csv (Transfermarkt)
Target: database/kleague1.db → player_master 테이블
"""

import sqlite3
import pandas as pd
import re
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

CSV_PATH = Path("C:/Users/koaro/Downloads/선수정보.csv")
DB_PATH = Path("database/kleague1.db")

# ─────────────────────────────────────────────
# 파싱 유틸
# ─────────────────────────────────────────────

def parse_birth_date(val: str) -> str | None:
    """'11/12/2003 (22)' → '2003-12-11' (DD/MM/YYYY)"""
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", str(val))
    if m:
        day, month, year = m.groups()
        return f"{year}-{month}-{day}"
    return None


def parse_joined(val: str) -> str | None:
    """'04/06/2025' → '2025-06-04' (DD/MM/YYYY)"""
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", str(val))
    if m:
        day, month, year = m.groups()
        return f"{year}-{month}-{day}"
    return None


def parse_height(val: str) -> int | None:
    """'1,73 m' → 173"""
    m = re.match(r"(\d+),(\d+)", str(val))
    if m:
        return int(m.group(1)) * 100 + int(m.group(2))
    return None


def parse_value(val: str) -> int | None:
    """'€150k' → 150000 / '€1.50m' → 1500000 / '-' → None"""
    val = str(val).strip()
    if val == "-":
        return None
    mk = re.match(r"€([\d.]+)k$", val)
    mm = re.match(r"€([\d.]+)m$", val)
    if mk:
        return int(float(mk.group(1)) * 1_000)
    if mm:
        return int(float(mm.group(1)) * 1_000_000)
    return None


def parse_position(val: str) -> tuple[str | None, str | None]:
    """'Attack - Right Winger' → ('Attack', 'Right Winger')"""
    if pd.isna(val) or str(val).strip() == "":
        return None, None
    parts = str(val).split(" - ", 1)
    pos = parts[0].strip() or None
    detail = parts[1].strip() if len(parts) > 1 else None
    return pos, detail


# ─────────────────────────────────────────────
# DDL
# ─────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS player_master (
    master_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    player_name         TEXT,           -- 한국어 이름 (외국인은 매핑 완료 후 채워짐)
    name_original       TEXT NOT NULL,  -- CSV 원본 이름 (외국인:영문, 한국인:한국어)
    birth_date          TEXT,           -- YYYY-MM-DD
    height_cm           INTEGER,
    citizenship         TEXT,
    is_korean           INTEGER,        -- 1:한국인 0:외국인
    position            TEXT,           -- Attack / Defender / Midfield / Goalkeeper
    position_detail     TEXT,           -- Right Winger / Centre-Back / etc.
    current_club        TEXT,           -- 현재 소속 클럽 (영문)
    joined              TEXT,           -- 현재 클럽 합류일 YYYY-MM-DD
    market_value_eur    INTEGER,        -- 유로 단위 정수 (null = 정보없음)
    UNIQUE (name_original, birth_date)
)
"""


# ─────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────

def main():
    print(f"[1/4] CSV 읽기: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    print(f"      총 {len(df)}명")

    print("[2/4] 컬럼 파싱 중...")
    rows = []
    for _, r in df.iterrows():
        name_original = str(r["Name in home country"]).strip()
        is_korean = 1 if "Korea" in str(r.get("Citizenship", "")) else 0
        # 한국인은 player_name = name_original, 외국인은 None (나중에 매핑)
        player_name = name_original if is_korean else None

        birth_date = parse_birth_date(r.get("Date of birth/Age", ""))
        height_cm = parse_height(r.get("Height", ""))
        citizenship = str(r.get("Citizenship", "")).strip() or None
        position, position_detail = parse_position(r.get("Position", ""))
        current_club = str(r.get("Current club", "")).strip() or None
        joined = parse_joined(r.get("Joined", ""))
        market_value_eur = parse_value(r.get("Value", "-"))

        rows.append({
            "player_name":      player_name,
            "name_original":    name_original,
            "birth_date":       birth_date,
            "height_cm":        height_cm,
            "citizenship":      citizenship,
            "is_korean":        is_korean,
            "position":         position,
            "position_detail":  position_detail,
            "current_club":     current_club,
            "joined":           joined,
            "market_value_eur": market_value_eur,
        })

    parsed_df = pd.DataFrame(rows)
    korean_cnt = parsed_df["is_korean"].sum()
    foreign_cnt = len(parsed_df) - korean_cnt
    print(f"      한국인: {korean_cnt}명 / 외국인: {foreign_cnt}명")

    print("[3/4] DB 적재 중...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()

    inserted = skipped = 0
    for row in rows:
        try:
            conn.execute(
                """INSERT INTO player_master
                   (player_name, name_original, birth_date, height_cm,
                    citizenship, is_korean, position, position_detail,
                    current_club, joined, market_value_eur)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    row["player_name"], row["name_original"], row["birth_date"],
                    row["height_cm"], row["citizenship"], row["is_korean"],
                    row["position"], row["position_detail"], row["current_club"],
                    row["joined"], row["market_value_eur"],
                ),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            skipped += 1

    conn.commit()

    # 결과 확인
    total = conn.execute("SELECT COUNT(*) FROM player_master").fetchone()[0]
    korean_db = conn.execute(
        "SELECT COUNT(*) FROM player_master WHERE is_korean=1"
    ).fetchone()[0]
    foreign_db = conn.execute(
        "SELECT COUNT(*) FROM player_master WHERE is_korean=0"
    ).fetchone()[0]
    foreign_no_name = conn.execute(
        "SELECT COUNT(*) FROM player_master WHERE is_korean=0 AND player_name IS NULL"
    ).fetchone()[0]

    print(f"      삽입: {inserted}건 / 중복 스킵: {skipped}건")
    print(f"\n[4/4] player_master 현황")
    print(f"      전체: {total}명")
    print(f"      한국인: {korean_db}명 (player_name 완비)")
    print(f"      외국인: {foreign_db}명 (한국명 미입력: {foreign_no_name}명)")

    # ── 외국인 한국명 매핑 템플릿 CSV 출력 ──────────────────
    print("[부록] 외국인 선수 한국명 매핑 템플릿 생성...")
    foreign_df = pd.read_sql(
        """SELECT master_id, name_original, birth_date, citizenship,
                  position, position_detail, current_club
           FROM player_master WHERE is_korean=0 ORDER BY current_club, name_original""",
        conn,
    )
    foreign_df.insert(3, "korean_name", "")   # 사용자가 채울 컬럼
    out_path = Path("C:/Users/koaro/Downloads/외국인선수_한국명매핑.csv")
    foreign_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"      저장됨 → {out_path}")
    print(f"      'korean_name' 컬럼에 한국어 이름 입력 후 import_korean_names.py 실행")

    conn.close()
    print("\n완료.")


if __name__ == "__main__":
    main()
