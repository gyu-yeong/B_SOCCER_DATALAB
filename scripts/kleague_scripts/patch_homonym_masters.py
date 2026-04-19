#!/usr/bin/env python3
"""
patch_homonym_masters.py — 동명이인 선수 master_id 수동 패치

build_db.py Phase 3의 team 기반 disambiguation이 실패한 동명이인 선수에 대해
(player_name, team_id) 조합으로 master_id를 직접 UPDATE합니다.

대상 선수:
  - 가브리엘: 강원(Vitor Gabriel, 2000-01-20) vs 광주(Gabriel Tigrão, 2001-10-13)
  - 마테우스: 울산(Matheus Sales, 1995-05-13) vs 안양(Matheus Oliveira, 1997-09-28)

실행:
    python scripts/kleague_scripts/patch_homonym_masters.py

주의:
  - build_db.py 실행 후 실행할 것 (master_id 전체 리셋 시 재실행 필요)
  - 향후 포털 등록명이 확정되면 TM_squads CSV name_kor 수정 + build_db.py 재실행으로 대체
"""

import sys, os, sqlite3

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DB_PATH  = os.path.join(BASE_DIR, "database", "kleague.db")

# ── 패치 규칙 ──────────────────────────────────────────────────
# (player_name, team_id) → master_id
#
# master_id 확인:
#   가브리엘: Vitor Gabriel   (2000-01-20, master_id=571) → 강원 (team_id=30078)
#             Gabriel Tigrão  (2001-10-13, master_id=677) → 광주 (team_id=30158)
#   마테우스: Matheus Sales   (1995-05-13, master_id=162) → 울산 (team_id=29757)
#             Matheus Oliveira(1997-09-28, master_id=1925)→ 안양 (team_id=29758)
# ──────────────────────────────────────────────────────────────

PATCH_RULES: list[tuple[str, int, int]] = [
    # (player_name,        team_id, master_id)
    ("가브리엘",            30078,   571),    # Vitor Gabriel — 강원
    ("가브리엘",            30158,   677),    # Gabriel Tigrão — 광주
    ("마테우스",            29757,   162),    # Matheus Sales — 울산
    ("마테우스",            29758,   1925),   # Matheus Oliveira — 안양
]


def run(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    # 사전 검증: master_id / team_id 존재 여부 확인
    print("[검증]")
    for player_name, team_id, master_id in PATCH_RULES:
        pm = cur.execute(
            "SELECT name_kor, birth_date FROM player_master WHERE master_id=?", (master_id,)
        ).fetchone()
        team = cur.execute(
            "SELECT team_name FROM teams WHERE team_id=?", (team_id,)
        ).fetchone()
        if not pm:
            print(f"  ⚠ master_id={master_id} 없음 — build_db.py 먼저 실행하세요")
            return
        if not team:
            print(f"  ⚠ team_id={team_id} 없음 — 확인 필요")
            return
        print(f"  OK  {player_name} → master_id={master_id} ({pm[0]}, {pm[1]}) / 팀={team[0]}")

    print()
    print("[UPDATE]")
    total_updated = 0

    for player_name, team_id, master_id in PATCH_RULES:
        # 해당 (player_name, team_id) 조합에서 master_id IS NULL인 pms rows를 UPDATE
        result = cur.execute("""
            UPDATE player_match_stats
            SET master_id = ?
            WHERE master_id IS NULL
              AND team_id = ?
              AND player_id IN (
                  SELECT player_id FROM players WHERE player_name = ?
              )
        """, (master_id, team_id, player_name))
        n = result.rowcount
        total_updated += n
        team_name = cur.execute(
            "SELECT team_name FROM teams WHERE team_id=?", (team_id,)
        ).fetchone()[0]
        print(f"  {player_name} ({team_name}) → master_id={master_id}: {n}건 UPDATE")

    conn.commit()

    # 결과 리포트
    total  = cur.execute("SELECT COUNT(*) FROM player_match_stats").fetchone()[0]
    filled = cur.execute(
        "SELECT COUNT(*) FROM player_match_stats WHERE master_id IS NOT NULL"
    ).fetchone()[0]
    pct = 100 * filled // total if total else 0

    print(f"\n  → 총 {total_updated}건 패치 완료")
    print(f"  → master_id 확보율: {filled}/{total} ({pct}%)")


def main() -> None:
    print("=" * 55)
    print("동명이인 master_id 수동 패치")
    print("=" * 55)

    if not os.path.exists(DB_PATH):
        print(f"  ⚠ DB 없음: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    try:
        run(conn)
    finally:
        conn.close()

    print("\n✅ 완료")


if __name__ == "__main__":
    main()
