#!/usr/bin/env python3
"""
patch_master_conflicts.py — 동명이인 병합(master_id 충돌) 재매핑 패치

문제 (todo_list #11):
  build_db.py 의 이름 기반 매핑이 동명이인을 한 master_id 로 병합해,
  (시즌·리그·선수) 집계 시 출전수가 리그 라운드 상한(KL1 38 / KL2 41)을 초과하는
  선수-시즌 11건이 발생. 한 master_id 에 서로 다른 두 사람의 경기가 섞임.

해결:
  정답 소스 season_roster(season, team_id, jersey_number → master_id)와
  이름·생년 검증으로 각 player_id(=팀·등번호 = 한 사람)의 올바른 master_id 를 확정.
  아래 REMAP(player_id → 정정 master_id)으로 player_match_stats / players 를 UPDATE.

특징:
  - idempotent: 이미 정정된 행은 건너뜀(WHERE master_id != target)
  - 사전 검증: 정정 대상 master_id 존재 여부 확인
  - 사후 검증: (시즌·리그·선수) 출전수 > 상한+2 인 충돌 0건 확인

실행:
    python scripts/kleague_scripts/patch_master_conflicts.py

주의:
  - build_db.py 실행(=master_id 전체 리셋) 후 재실행 필요 (patch_homonym_masters.py 와 동일)
  - 적용 후 `python scripts/export_comparison_data.py` 재실행으로 데모 JSON 갱신
"""

import sys, os, sqlite3

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DB_PATH  = os.path.join(BASE_DIR, "database", "kleague.db")

# ── 재매핑 규칙: player_id → 정정 master_id ──────────────────────
# 근거: season_roster(season, team_id, jersey) 정답 + 이름/생년 검증 (2026-06-07)
#   player_id | 팀#번호      | 이름   | 현재→정정 | 정정 master(생년)
REMAP: dict[int, int] = {
    16621: 92,    # 전북#23 김태환 394→92   (김태환 1989-07-24)
    26274: 92,    # 전북#39 김태환 394→92   (김태환 1989-07-24)
    19029: 1667,  # 강원#35 김태환 394→1667 (김태환 2006-05-29)
    17794: 290,   # 대전#23 김민우 174→290  (김민우 2002-03-16)
    28227: 290,   # 대전#55 김민우 174→290  (김민우 2002-03-16)
    19518: 643,   # 광주#1  김경민 56→643   (김경민 1991-11-01, GK)
    20102: 1091,  # 안양#22 김동진 512→1091 (김동진 1992-12-28)
    18489: 623,   # 대구#44 김정현 1095→623 (Jeong-hyun Kim 2000-06-09)
    42325: 517,   # 충북청주#28 김정현 1095→517 (Jung-hyun Kim 2004-06-29)
    29031: 2209,  # 대구#17 이탈로 400→2209 (Ítalo Carvalho 1996-11-07)
    33950: 40,    # 인천#15 서재민 1055→40  (Jae-min Seo 2003-09-16)
    37435: 869,   # 경남#77 박민서 850→869  (Min-seo Park 1998-06-30)
}

ROUND_CAP = {"K리그1": 38, "K리그2": 41}


def norm_league(name: str | None) -> str | None:
    if not name:
        return None
    if "K리그1" in name:
        return "K리그1"
    if "K리그2" in name:
        return "K리그2"
    return None


def overcount_cases(cur: sqlite3.Cursor) -> list[tuple]:
    """(시즌·리그·선수) 출전수 > 상한+2 인 충돌 케이스 반환."""
    rows = cur.execute("""
        SELECT c.year, c.competition_name, pms.master_id, pm.name_kor,
               pms.match_id, COALESCE(pms.minutes_played,0) mins
        FROM player_match_stats pms
        JOIN matches m ON pms.match_id = m.match_id
        JOIN competitions c ON m.competition_id = c.competition_id
        JOIN player_master pm ON pms.master_id = pm.master_id
        WHERE pms.master_id IS NOT NULL
    """).fetchall()
    # dedup (year, league, master, match) → distinct matches
    seen = {}
    for y, cn, mid, name, match, mins in rows:
        lg = norm_league(cn)
        if not lg:
            continue
        k = (y, lg, mid, match)
        if k not in seen or mins > seen[k][1]:
            seen[k] = (name, mins)
    games = {}
    for (y, lg, mid, _m), (name, _mins) in seen.items():
        key = (y, lg, mid, name)
        games[key] = games.get(key, 0) + 1
    hits = []
    for (y, lg, mid, name), g in games.items():
        if g > ROUND_CAP.get(lg, 41) + 2:
            hits.append((y, lg, mid, name, g))
    return sorted(hits, key=lambda x: -x[4])


def run(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    print("[사전 검증] 정정 대상 master_id 존재 확인")
    for pid, mid in REMAP.items():
        pm = cur.execute(
            "SELECT name_kor, birth_date FROM player_master WHERE master_id=?", (mid,)
        ).fetchone()
        if not pm:
            print(f"  ⚠ master_id={mid} 없음 — 중단")
            return
    print(f"  OK ({len(set(REMAP.values()))}개 대상 master 모두 존재)")

    print("\n[적용 전] 과대집계 충돌")
    before = overcount_cases(cur)
    for y, lg, mid, name, g in before:
        print(f"  {y} {lg} master={mid} {name} games={g}")
    print(f"  → {len(before)}건")

    print("\n[UPDATE]")
    pms_total = players_total = 0
    for pid, mid in REMAP.items():
        r1 = cur.execute(
            "UPDATE player_match_stats SET master_id=? "
            "WHERE player_id=? AND (master_id <> ? OR master_id IS NULL)",
            (mid, pid, mid),
        )
        r2 = cur.execute(
            "UPDATE players SET master_id=? "
            "WHERE player_id=? AND (master_id <> ? OR master_id IS NULL)",
            (mid, pid, mid),
        )
        pms_total += r1.rowcount
        players_total += max(r2.rowcount, 0)
        print(f"  player_id={pid:5} → master_id={mid:5}: pms {r1.rowcount}건")
    conn.commit()
    print(f"  → player_match_stats {pms_total}건 / players {players_total}건 재매핑")

    print("\n[적용 후] 과대집계 충돌")
    after = overcount_cases(cur)
    for y, lg, mid, name, g in after:
        print(f"  ⚠ {y} {lg} master={mid} {name} games={g}")
    print(f"  → {len(after)}건")
    if not after:
        print("\n✅ 충돌 0건 — 정상화 완료")
    else:
        print("\n⚠ 잔여 충돌 존재 — 추가 검토 필요")


def main() -> None:
    print("=" * 55)
    print("master_id 충돌(동명이인 병합) 재매핑 패치 — todo #11")
    print("=" * 55)
    if not os.path.exists(DB_PATH):
        print(f"  ⚠ DB 없음: {DB_PATH}")
        sys.exit(1)
    conn = sqlite3.connect(DB_PATH)
    try:
        run(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
