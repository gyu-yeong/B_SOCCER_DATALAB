# scripts/db_setup_v4.py
"""
ETL_ver4
- 컬럼 mismatch 완전 방지
- CSV 컬럼 fallback 지원
- stats mapping 구조화
- placeholder 자동 생성
"""

import sqlite3
import pandas as pd
import os


DB_PATH = "database/kleague1.db"


# -------------------------
# 유틸
# -------------------------
def clean_numeric_value(value):
    if pd.isna(value) or value in ["", "-"]:
        return 0
    try:
        return int(float(value))
    except:
        return 0


def safe_get_column(row, names):
    for n in names:
        if n in row.index:
            return row[n]
    return 0


# -------------------------
# Stats 매핑 (핵심)
# -------------------------
STAT_MAPPING = {
    "minutes_played": ["출전시간(분)", "출전시간"],
    "goals": ["득점"],
    "assists": ["도움"],
    "shots": ["슈팅"],
    "shots_on_target": ["유효슈팅", "유효 슈팅"],
    "blocked_shots": ["차단된슈팅", "차단된 슈팅"],
    "missed_shots": ["벗어난슈팅", "벗어난 슈팅"],
    "shots_in_pa": ["PA내 슈팅", "PA내슈팅"],
    "shots_out_pa": ["PA외 슈팅", "PA외슈팅"],
    "offsides": ["오프사이드"],
    "freekicks": ["프리킥"],
    "corners": ["코너킥"],
    "throwins": ["스로인"],
    "dribbles_attempted": ["드리블 시도", "드리블시도"],
    "dribbles_successful": ["드리블 성공", "드리블성공"],
    "passes_attempted": ["패스 시도", "패스시도"],
    "passes_successful": ["패스 성공", "패스성공"],
    "key_passes": ["키패스"],
    "forward_passes_attempted": ["전방 패스 시도", "전방패스시도"],
    "forward_passes_successful": ["전방 패스 성공", "전방패스성공"],
    "backward_passes_attempted": ["후방 패스 시도", "후방패스시도"],
    "backward_passes_successful": ["후방 패스 성공", "후방패스성공"],
    "lateral_passes_attempted": ["횡패스 시도", "횡패스시도"],
    "lateral_passes_successful": ["횡패스 성공", "횡패스성공"],
    "attacking_third_passes_attempted": ["공격지역패스 시도"],
    "attacking_third_passes_successful": ["공격지역패스 성공"],
    "defensive_third_passes_attempted": ["수비지역패스 시도"],
    "defensive_third_passes_successful": ["수비지역패스 성공"],
    "middle_third_passes_attempted": ["중앙지역패스 시도"],
    "middle_third_passes_successful": ["중앙지역패스 성공"],
    "long_passes_attempted": ["롱패스 시도"],
    "long_passes_successful": ["롱패스 성공"],
    "medium_passes_attempted": ["중거리패스 시도"],
    "medium_passes_successful": ["중거리패스 성공"],
    "short_passes_attempted": ["숏패스 시도"],
    "short_passes_successful": ["숏패스 성공"],
    "crosses_attempted": ["크로스 시도"],
    "crosses_successful": ["크로스 성공"],
    "ground_duels_attempted": ["경합 지상 시도"],
    "ground_duels_won": ["경합 지상 성공"],
    "aerial_duels_attempted": ["경합 공중 시도"],
    "aerial_duels_won": ["경합 공중 성공"],
    "tackles_attempted": ["태클 시도"],
    "tackles_successful": ["태클 성공"],
    "clearances": ["클리어링"],
    "interceptions": ["인터셉트"],
    "blocks": ["차단"],
    "recoveries": ["획득"],
    "ball_losses": ["볼미스"],
    "fouls_committed": ["파울"],
    "fouls_won": ["피파울"],
    "yellow_cards": ["경고"],
    "red_cards": ["퇴장"],
}


STAT_COLUMNS = list(STAT_MAPPING.keys())


# -------------------------
# 메인 ETL
# -------------------------
def import_csv_to_db(csv_path):

    if not os.path.exists(csv_path):
        print("CSV 없음")
        return

    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for idx, row in df.iterrows():

        try:

            # ----------------
            # competition
            # ----------------
            cursor.execute(
                "INSERT OR IGNORE INTO competitions (year, competition_name) VALUES (?, ?)",
                (clean_numeric_value(row["대회년도"]), row["대회명"]),
            )

            cursor.execute(
                "SELECT competition_id FROM competitions WHERE year=? AND competition_name=?",
                (clean_numeric_value(row["대회년도"]), row["대회명"]),
            )
            competition_id = cursor.fetchone()[0]

            # ----------------
            # teams
            # ----------------
            cursor.execute(
                "INSERT OR IGNORE INTO teams (team_name) VALUES (?)", (row["팀명"],)
            )
            cursor.execute(
                "SELECT team_id FROM teams WHERE team_name=?", (row["팀명"],)
            )
            team_id = cursor.fetchone()[0]

            opponent = row["상대팀명"]
            cursor.execute(
                "INSERT OR IGNORE INTO teams (team_name) VALUES (?)", (opponent,)
            )
            cursor.execute(
                "SELECT team_id FROM teams WHERE team_name=?", (opponent,)
            )
            opponent_team_id = cursor.fetchone()[0]

            # ----------------
            # match
            # ----------------
            round_number = row["라운드"]

            cursor.execute(
                """
                INSERT OR IGNORE INTO matches
                (competition_id, round_number, home_team_id, away_team_id)
                VALUES (?, ?, ?, ?)
                """,
                (competition_id, round_number, team_id, opponent_team_id),
            )

            cursor.execute(
                """
                SELECT match_id FROM matches
                WHERE competition_id=? AND round_number=?
                AND (
                    (home_team_id=? AND away_team_id=?)
                    OR
                    (home_team_id=? AND away_team_id=?)
                )
                """,
                (
                    competition_id,
                    round_number,
                    team_id,
                    opponent_team_id,
                    opponent_team_id,
                    team_id,
                ),
            )

            match_id = cursor.fetchone()[0]

            # ----------------
            # player
            # ----------------
            player_name = row["선수명"]

            cursor.execute(
                """
                INSERT OR IGNORE INTO players
                (player_name, position, back_number, team_name)
                VALUES (?, ?, ?, ?)
                """,
                (
                    player_name,
                    row["포지션"],
                    clean_numeric_value(row["등번호"]),
                    row["팀명"],
                ),
            )

            cursor.execute(
                "SELECT player_id FROM players WHERE player_name=? AND team_name=?",
                (player_name, row["팀명"]),
            )
            player_id = cursor.fetchone()[0]

            # ----------------
            # stats 생성
            # ----------------
            stat_values = []

            for col in STAT_COLUMNS:
                value = clean_numeric_value(
                    safe_get_column(row, STAT_MAPPING[col])
                )
                stat_values.append(value)

            # ----------------
            # INSERT 자동 생성
            # ----------------
            columns = ["match_id", "player_id", "team_id"] + STAT_COLUMNS
            values = [match_id, player_id, team_id] + stat_values

            placeholders = ",".join(["?"] * len(values))
            col_string = ",".join(columns)

            sql = f"""
            INSERT OR REPLACE INTO player_match_stats
            ({col_string})
            VALUES ({placeholders})
            """

            cursor.execute(sql, values)

        except Exception as e:
            print(
                f"❌ 오류 idx={idx} 선수={row.get('선수명')} 라운드={row.get('라운드')}"
            )
            print(e)

    conn.commit()
    conn.close()
    print("✅ import 완료")


# -------------------------
# 실행
# -------------------------
import glob

if __name__ == "__main__":

    csv_files = glob.glob("data/raw/2025_KLEAGUE1/*.csv")

    for file in sorted(csv_files):
        print(f"\n📂 적재 시작: {file}")
        import_csv_to_db(file)
