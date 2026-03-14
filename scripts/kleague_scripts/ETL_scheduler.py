"""
ETL_scheduler.py — K리그 배치 스케줄러

실행 방법:
    python ETL_scheduler.py

동작:
    1. schedule 테이블에서 오늘 이전 경기 중 match_id IS NULL인 미적재 경기 탐색
    2. 대회별 최소 미적재 라운드(from_round) 계산 → 불필요한 재수집 방지
    3. ETL_backpill_stable.py의 scrape_match_data() 호출
    4. insert_dataframe()으로 DB 적재
    5. schedule.match_id UPDATE (조인키: competition_id + round_number + home/away team_id)
"""

import sys
import sqlite3
import os

# 경로 설정 (이 스크립트 위치에서 DB 참조)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "..", "..", "database", "kleague1.db")

# 인코딩 설정 (Windows cp949 환경)
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# --------------------------------------------------
# 포털 대회 코드 매핑
# selectMeetSeq 드롭다운의 value 값
# 포털에서 직접 확인한 값 — 변경 시 이 dict만 수정
# --------------------------------------------------
COMPETITION_MEET_MAP = {
    "K리그1": 1,
    "K리그2": 2,
    # "K리그 슈퍼컵": 3,  # 포털 지원 여부 확인 필요
}


def get_db():
    return sqlite3.connect(DB_PATH)


def fetch_unscraped_targets():
    """
    오늘 이전 경기 중 match_id가 없는 대회+연도 조합 목록 반환.
    각 항목에 from_round (최소 미적재 라운드 번호) 포함.

    Returns:
        list of dict: [{"year": 2026, "competition_name": "K리그1", "from_round": 1}, ...]
    """
    with get_db() as conn:
        rows = conn.execute("""
            SELECT
                CAST(strftime('%Y', match_date) AS INTEGER) AS year,
                competition_name,
                MIN(CAST(round_number AS INTEGER)) AS min_round_int
            FROM schedule
            WHERE date(match_date) <= date('now')
              AND match_id IS NULL
              AND competition_name IN ({})
            GROUP BY year, competition_name
            ORDER BY year, competition_name
        """.format(",".join("?" * len(COMPETITION_MEET_MAP))),
        list(COMPETITION_MEET_MAP.keys())).fetchall()
        # SQLite: CAST('34R' AS INTEGER) = 34, CAST('슈퍼컵' AS INTEGER) = 0 → fallback to 1

    targets = []
    for year, competition_name, min_round_int in rows:
        from_round = max(1, min_round_int or 1)

        targets.append({
            "year": year,
            "competition_name": competition_name,
            "meet_value": COMPETITION_MEET_MAP[competition_name],
            "from_round": from_round,
            "min_round_text": f"{from_round}R",
        })

    return targets


def update_schedule_match_ids():
    """
    matches 테이블에 적재된 경기와 schedule 테이블을 조인하여
    schedule.match_id를 업데이트.
    조인키: (competition_id, round_number, home_team_id, away_team_id)
    """
    with get_db() as conn:
        result = conn.execute("""
            UPDATE schedule
            SET match_id = (
                SELECT m.match_id
                FROM matches m
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
        updated = result.rowcount
        conn.commit()

    print(f"[schedule UPDATE] match_id 연결 완료: {updated}건")
    return updated


def print_status():
    """현재 schedule 테이블 적재 상태 출력."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT
                competition_name,
                CAST(strftime('%Y', match_date) AS INTEGER) AS year,
                COUNT(*) AS total,
                SUM(CASE WHEN match_id IS NOT NULL THEN 1 ELSE 0 END) AS linked,
                SUM(CASE WHEN match_id IS NULL AND date(match_date) <= date('now') THEN 1 ELSE 0 END) AS pending
            FROM schedule
            GROUP BY competition_name, year
            ORDER BY year, competition_name
        """).fetchall()

    print("\n[schedule 현황]")
    print(f"{'대회':<15} {'시즌':<6} {'전체':>6} {'연결됨':>8} {'미적재(과거)':>12}")
    print("-" * 52)
    for comp, year, total, linked, pending in rows:
        print(f"{comp:<15} {year:<6} {total:>6} {linked:>8} {pending:>12}")
    print()


# --------------------------------------------------
# main
# --------------------------------------------------
if __name__ == "__main__":

    # 현재 상태 출력
    print_status()

    # 미적재 대상 탐색
    targets = fetch_unscraped_targets()

    if not targets:
        print("[완료] 미적재 경기 없음. 스크래핑 불필요.")
        sys.exit(0)

    print(f"[대상] 스크래핑 필요 조합: {len(targets)}개")
    for t in targets:
        print(f"  - {t['year']} {t['competition_name']} (meet={t['meet_value']}, from_round={t['from_round']}R)")

    print()

    # 스크래핑 실행
    from ETL_backpill_stable import create_driver, scrape_match_data
    from ETL_ver4 import insert_dataframe

    driver = create_driver()

    try:
        for t in targets:
            print(f"\n[스크래핑 시작] {t['year']} {t['competition_name']} (from {t['min_round_text']})")

            df = scrape_match_data(
                driver,
                year_value=t["year"],
                meet_value=t["meet_value"],
                from_round=t["from_round"],
            )

            if df.empty:
                print("  → 수집 데이터 없음")
                continue

            print(f"  → 수집 {len(df)}행, DB 적재 중...")
            insert_dataframe(df)
            print("  → 적재 완료")

            # 적재 후 schedule.match_id 즉시 업데이트
            update_schedule_match_ids()

    finally:
        driver.quit()

    # 최종 상태 출력
    print_status()
    print("[배치 완료]")
