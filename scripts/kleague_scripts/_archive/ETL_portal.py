# kleague_pipeline.py

import sqlite3
import pandas as pd
import time
import os

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup


DB_PATH = "database/kleague1.db"

# -------------------------
# UTIL
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
# STAT MAPPING
# -------------------------
STAT_MAPPING = {
    "minutes_played": ["출전시간(분)", "출전시간"],
    "goals": ["득점"],
    "assists": ["도움"],
    "shots": ["슈팅"],
    "shots_on_target": ["유효 슈팅", "유효슈팅"],
    "blocked_shots": ["차단된슈팅", "차단된 슈팅"],
    "missed_shots": ["벗어난슈팅", "벗어난 슈팅"],
    "shots_in_pa": ["PA내 슈팅"],
    "shots_out_pa": ["PA외 슈팅"],
    "offsides": ["오프사이드"],
    "freekicks": ["프리킥"],
    "corners": ["코너킥"],
    "throwins": ["스로인"],
    "dribbles_attempted": ["드리블 시도"],
    "dribbles_successful": ["드리블 성공"],
    "passes_attempted": ["패스 시도"],
    "passes_successful": ["패스 성공"],
    "key_passes": ["키패스"],
    "forward_passes_attempted": ["전방 패스 시도"],
    "forward_passes_successful": ["전방 패스 성공"],
    "backward_passes_attempted": ["후방 패스 시도"],
    "backward_passes_successful": ["후방 패스 성공"],
    "lateral_passes_attempted": ["횡패스 시도"],
    "lateral_passes_successful": ["횡패스 성공"],
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
# DB LOADER
# -------------------------
def insert_dataframe(df):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for _, row in df.iterrows():

        # competition
        cursor.execute(
            "INSERT OR IGNORE INTO competitions (year, competition_name) VALUES (?, ?)",
            (clean_numeric_value(row["대회년도"]), row["대회명"]),
        )

        cursor.execute(
            "SELECT competition_id FROM competitions WHERE year=? AND competition_name=?",
            (clean_numeric_value(row["대회년도"]), row["대회명"]),
        )
        competition_id = cursor.fetchone()[0]

        # teams
        cursor.execute("INSERT OR IGNORE INTO teams (team_name) VALUES (?)", (row["팀명"],))
        cursor.execute("SELECT team_id FROM teams WHERE team_name=?", (row["팀명"],))
        team_id = cursor.fetchone()[0]

        opponent = row["상대팀명"]
        cursor.execute("INSERT OR IGNORE INTO teams (team_name) VALUES (?)", (opponent,))
        cursor.execute("SELECT team_id FROM teams WHERE team_name=?", (opponent,))
        opponent_team_id = cursor.fetchone()[0]

        # match
        round_number = row["라운드"]

        cursor.execute("""
            INSERT OR IGNORE INTO matches
            (competition_id, round_number, home_team_id, away_team_id)
            VALUES (?, ?, ?, ?)
        """, (competition_id, round_number, team_id, opponent_team_id))

        cursor.execute("""
            SELECT match_id FROM matches
            WHERE competition_id=? AND round_number=?
        """, (competition_id, round_number))

        match_id = cursor.fetchone()[0]

        # player
        cursor.execute("""
            INSERT OR IGNORE INTO players
            (player_name, position, back_number, team_name)
            VALUES (?, ?, ?, ?)
        """, (
            row["선수명"],
            row["포지션"],
            clean_numeric_value(row["등번호"]),
            row["팀명"]
        ))

        cursor.execute("""
            SELECT player_id FROM players
            WHERE player_name=? AND team_name=?
        """, (row["선수명"], row["팀명"]))

        player_id = cursor.fetchone()[0]

        # stats
        stat_values = [
            clean_numeric_value(
                safe_get_column(row, STAT_MAPPING[col])
            )
            for col in STAT_COLUMNS
        ]

        columns = ["match_id", "player_id", "team_id"] + STAT_COLUMNS
        values = [match_id, player_id, team_id] + stat_values

        placeholders = ",".join(["?"] * len(values))
        col_string = ",".join(columns)

        cursor.execute(
            f"INSERT OR REPLACE INTO player_match_stats ({col_string}) VALUES ({placeholders})",
            values
        )

    conn.commit()
    conn.close()

    print("✅ DB 적재 완료")


# -------------------------
# CSV MODE
# -------------------------
def load_from_csv(path):
    df = pd.read_csv(path, encoding="utf-8-sig")
    insert_dataframe(df)


# -------------------------
# DRIVER 생성
# -------------------------
def create_driver():

    options = Options()
    options.page_load_strategy = "eager"

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    driver.get("https://data.kleague.com")

    frames = driver.find_elements("tag name", "frame")
    for frame in frames:
        if "portal.kleague.com" in frame.get_attribute("src"):
            driver.get(frame.get_attribute("src"))

    driver.execute_script("javascript:moveMainFrame('0208');")

    return driver


# -------------------------
# SCRAPER
# -------------------------
def scrape_round(driver, year, meet, start_idx, end_idx):

    all_table_data = []

    year_select = Select(driver.find_element(By.ID, "selectYear"))
    year_select.select_by_value(str(year))
    time.sleep(1)

    meet_select = Select(driver.find_element(By.ID, "selectMeetSeq"))
    meet_select.select_by_value(str(meet))
    time.sleep(1)

    team_select = Select(driver.find_element(By.ID, "selectTeamId"))
    team_values = [o.get_attribute("value") for o in team_select.options[1:]]

    for team_value in team_values:

        team_select.select_by_value(team_value)
        time.sleep(1)

        game_select = Select(driver.find_element(By.ID, "selectGameId"))
        game_values = [o.get_attribute("value") for o in game_select.options[1:]]

        for game_value in game_values[start_idx:end_idx]:

            game_select.select_by_value(game_value)
            time.sleep(1)

            driver.find_element(By.ID, "btnSearch").click()
            time.sleep(2)

            year_text = year_select.first_selected_option.text
            meet_text = meet_select.first_selected_option.text
            team_text = team_select.first_selected_option.text
            game_text = game_select.first_selected_option.text

            soup = BeautifulSoup(driver.page_source, "html.parser")
            table = soup.find("table")

            if table:
                rows = table.find_all("tr")
                for row in rows[2:-1]:
                    cols = [c.text.strip() for c in row.find_all("td")]
                    cols.extend([year_text, meet_text, team_text, game_text])
                    all_table_data.append(cols)

    columns = [
        "No","선수명","포지션","등번호","출전시간(분)","득점","도움","슈팅","유효 슈팅",
        "차단된슈팅","벗어난슈팅","PA내 슈팅","PA외 슈팅","오프사이드","프리킥","코너킥",
        "스로인","드리블 시도","드리블 성공","드리블 성공%","패스 시도","패스 성공",
        "패스 성공%","키패스","전방 패스 시도","전방 패스 성공","전방 패스 성공%",
        "후방 패스 시도","후방 패스 성공","후방 패스 성공%","횡패스 시도","횡패스 성공",
        "횡패스 성공%","공격지역패스 시도","공격지역패스 성공","공격지역패스 성공%",
        "수비지역패스 시도","수비지역패스 성공","수비지역패스 성공%","중앙지역패스 시도",
        "중앙지역패스 성공","중앙지역패스 성공%","롱패스 시도","롱패스 성공","롱패스 성공%",
        "중거리패스 시도","중거리패스 성공","중거리패스 성공%","숏패스 시도","숏패스 성공",
        "숏패스 성공%","크로스 시도","크로스 성공","크로스 성공%","경합 지상 시도",
        "경합 지상 성공","경합 지상 성공%","경합 공중 시도","경합 공중 성공","경합 공중 성공%",
        "태클 시도","태클 성공","태클 성공%","클리어링","인터셉트","차단","획득","블락",
        "볼미스","파울","피파울","경고","퇴장","대회년도","대회명","팀명","경기명"
    ]

    df = pd.DataFrame(all_table_data, columns=columns)

    df[["라운드","상대팀명"]] = df["경기명"].str.split("/", expand=True)
    df.drop(columns=[c for c in df.columns if "%" in c], inplace=True)

    return df