"""
외국인 선수 한국명 매핑 적용 스크립트
Source: 외국인선수_한국명매핑.csv (사용자 입력 완료 버전)
Target: database/kleague1.db → player_master.player_name 업데이트
"""

import sqlite3
import pandas as pd
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

CSV_PATH = Path("C:/Users/koaro/Downloads/외국인선수_한국명매핑.csv")
DB_PATH = Path("database/kleague1.db")


def main():
    print(f"[1/3] 매핑 CSV 읽기: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")

    filled = df[df["korean_name"].notna() & (df["korean_name"].str.strip() != "")]
    empty  = df[df["korean_name"].isna()  | (df["korean_name"].str.strip() == "")]
    print(f"      입력됨: {len(filled)}명 / 미입력: {len(empty)}명")

    print("[2/3] DB 업데이트 중...")
    conn = sqlite3.connect(DB_PATH)
    updated = 0

    for _, row in filled.iterrows():
        cur = conn.execute(
            "UPDATE player_master SET player_name=? WHERE master_id=?",
            (row["korean_name"].strip(), int(row["master_id"])),
        )
        updated += cur.rowcount

    conn.commit()

    # 현황 확인
    total_foreign = conn.execute(
        "SELECT COUNT(*) FROM player_master WHERE is_korean=0"
    ).fetchone()[0]
    completed = conn.execute(
        "SELECT COUNT(*) FROM player_master WHERE is_korean=0 AND player_name IS NOT NULL"
    ).fetchone()[0]
    remaining = total_foreign - completed

    print(f"      업데이트: {updated}건")
    print(f"\n[3/3] 외국인 선수 한국명 현황")
    print(f"      완료: {completed}/{total_foreign}명")
    if remaining > 0:
        print(f"      미완료: {remaining}명 (아래 목록)")
        rows = conn.execute(
            """SELECT master_id, name_original, citizenship, current_club
               FROM player_master WHERE is_korean=0 AND player_name IS NULL
               ORDER BY current_club, name_original"""
        ).fetchall()
        for r in rows:
            print(f"        [{r[0]}] {r[1]} ({r[2]}) - {r[3]}")

    conn.close()
    print("\n완료.")


if __name__ == "__main__":
    main()
