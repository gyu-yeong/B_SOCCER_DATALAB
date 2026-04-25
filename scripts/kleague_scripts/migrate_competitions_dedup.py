"""
migrate_competitions_dedup.py — competitions 중복 entry 통합 일회성 마이그레이션

문제:
    같은 시즌·리그가 두 개의 cid로 중복 존재해 schedule ↔ matches 조인 단절.
    예) 2024 K리그2: 'K리그2 2024'(33115, schedule) vs '하나은행 K리그2'(34786, matches)

원인:
    - load_schedule.py: '{keyword} {year}' 형식으로 INSERT
    - ETL_ver4.insert_dataframe: 포털 그대로 '하나은행 K리그X' 형식으로 INSERT
    - schedule이 먼저 적재된 시즌은 LIKE 매칭이 안 돼서 신규 cid 생성됨

동작:
    1. (year, K리그1/K리그2) 그룹화 → 중복 cid 검출
    2. 그룹 내 matches 보유 cid를 canonical로 채택
    3. 다른 cid의 schedule·player_match_stats·matches를 canonical로 UPDATE
    4. 비워진 중복 cid DELETE
    5. update_schedule_match_ids() 실행해 schedule.match_id 연결

idempotent: 재실행 시 중복이 없으면 NOOP
"""
import sys
import os
import sqlite3
from collections import defaultdict

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "..", "..", "database", "kleague.db")


def base_keyword(name: str) -> str | None:
    """competition_name에서 정규화 키워드 추출."""
    if "K리그1" in name:
        return "K리그1"
    if "K리그2" in name:
        return "K리그2"
    return None  # 슈퍼컵 등은 제외


def find_duplicate_groups(cur):
    """(year, base) 키로 cid를 그룹화 → 2개 이상인 그룹만 반환."""
    rows = cur.execute(
        "SELECT competition_id, year, competition_name FROM competitions"
    ).fetchall()

    groups = defaultdict(list)
    for cid, year, name in rows:
        base = base_keyword(name)
        if base is None:
            continue
        groups[(year, base)].append((cid, name))

    return {k: v for k, v in groups.items() if len(v) > 1}


def pick_canonical(cur, candidates):
    """matches 보유량이 가장 많은 cid를 canonical로 선택. 동률 시 cid 큰 것."""
    scored = []
    for cid, name in candidates:
        m_cnt = cur.execute(
            "SELECT COUNT(*) FROM matches WHERE competition_id = ?", (cid,)
        ).fetchone()[0]
        scored.append((m_cnt, cid, name))
    scored.sort(reverse=True)  # matches 많은 것, 그다음 cid 큰 것
    return scored[0][1], scored[0][2]  # canonical_cid, canonical_name


def merge_cid(cur, src_cid, dst_cid):
    """src_cid → dst_cid로 모든 참조 이동."""
    counts = {}

    # schedule 이동
    res = cur.execute(
        "UPDATE schedule SET competition_id = ? WHERE competition_id = ?",
        (dst_cid, src_cid),
    )
    counts["schedule"] = res.rowcount

    # matches 이동 (혹시라도 양쪽에 있으면)
    res = cur.execute(
        "UPDATE matches SET competition_id = ? WHERE competition_id = ?",
        (dst_cid, src_cid),
    )
    counts["matches"] = res.rowcount

    # 빈 competition row 삭제
    cur.execute("DELETE FROM competitions WHERE competition_id = ?", (src_cid,))
    counts["competition_deleted"] = src_cid

    return counts


def update_schedule_match_ids(cur):
    """schedule.match_id ↔ matches 조인 후 UPDATE."""
    res = cur.execute("""
        UPDATE schedule
        SET match_id = (
            SELECT m.match_id FROM matches m
            WHERE m.competition_id = schedule.competition_id
              AND m.round_number   = schedule.round_number
              AND m.home_team_id   = schedule.home_team_id
              AND m.away_team_id   = schedule.away_team_id
        )
        WHERE match_id IS NULL
          AND EXISTS (
            SELECT 1 FROM matches m
            WHERE m.competition_id = schedule.competition_id
              AND m.round_number   = schedule.round_number
              AND m.home_team_id   = schedule.home_team_id
              AND m.away_team_id   = schedule.away_team_id
          )
    """)
    return res.rowcount


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    print("=" * 60)
    print("competitions 중복 통합 마이그레이션")
    print("=" * 60)

    dups = find_duplicate_groups(cur)
    if not dups:
        print("\n✅ 중복 그룹 없음. NOOP.")
    else:
        print(f"\n[검출] 중복 그룹 {len(dups)}개")
        for (year, base), candidates in dups.items():
            print(f"\n  ▶ {year} {base}")
            for cid, name in candidates:
                m = cur.execute("SELECT COUNT(*) FROM matches WHERE competition_id=?", (cid,)).fetchone()[0]
                s = cur.execute("SELECT COUNT(*) FROM schedule WHERE competition_id=?", (cid,)).fetchone()[0]
                print(f"      cid={cid} | {name} | matches={m} schedule={s}")

            canonical_cid, canonical_name = pick_canonical(cur, candidates)
            print(f"      → canonical: cid={canonical_cid} ({canonical_name})")

            for cid, name in candidates:
                if cid == canonical_cid:
                    continue
                counts = merge_cid(cur, cid, canonical_cid)
                print(f"      [merge] cid={cid} → cid={canonical_cid}: "
                      f"schedule={counts['schedule']}, matches={counts['matches']}, "
                      f"competition deleted")

    # ----------------------------------------------------------
    # round_number 형식 통일: 'R1','R10' (schedule) → '1R','10R' (matches)
    # 'PO' 등 비숫자 라운드는 보존
    # ----------------------------------------------------------
    print("\n[round_number 정규화]")
    res = cur.execute("""
        UPDATE schedule
        SET round_number =
            substr(round_number, 2) || 'R'
        WHERE round_number GLOB 'R[0-9]*'
    """)
    print(f"  schedule.round_number 변환: {res.rowcount}건 (R{{N}} → {{N}}R)")

    print("\n[update_schedule_match_ids 실행]")
    updated = update_schedule_match_ids(cur)
    print(f"  schedule.match_id 연결: {updated}건")

    conn.commit()
    conn.close()
    print("\n[완료]")


if __name__ == "__main__":
    main()
