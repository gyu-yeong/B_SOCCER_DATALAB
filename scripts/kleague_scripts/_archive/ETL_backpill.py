import time
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# ⭐ ETL Loader import
from ETL_ver4 import insert_dataframe

# --------------------------------------------------
# DRIVER 생성
# --------------------------------------------------
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


# --------------------------------------------------
# ETL 스키마 정규화
# --------------------------------------------------
def normalize_to_etl_schema(df):

    # 1. % 컬럼 제거
    df = df.drop(columns=[c for c in df.columns if '%' in c], errors="ignore")

    # 2. 경기명 → 라운드 / 상대팀명
    if "경기명" in df.columns:

        split_cols = df["경기명"].str.split("/", expand=True)

        if split_cols.shape[1] >= 2:
            df["라운드"] = split_cols[0].str.strip()
            df["상대팀명"] = split_cols[1].str.strip()

    # 3. 문자열 trim
    for col in ["선수명", "팀명", "상대팀명", "포지션"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # 4. 필수 컬럼 보장
    required_columns = [
        "대회년도",
        "대회명",
        "팀명",
        "선수명",
        "포지션",
        "등번호",
        "라운드",
        "상대팀명"
    ]

    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    # 5. 중복 컬럼 제거
    df = df.loc[:, ~df.columns.duplicated()]

    return df


# --------------------------------------------------
# SCRAPER
# --------------------------------------------------
def scrape_match_data(driver, year_value, meet_value, start_game_index, end_game_index):

    all_table_data = []

    year_select = Select(
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.ID, "selectYear"))
    ))
    year_select.select_by_value(str(year_value))
    time.sleep(1)

    meet_select = Select(
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "selectMeetSeq"))
    ))
    meet_select.select_by_value(str(meet_value))
    time.sleep(1)

    team_select = Select(
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "selectTeamId"))
    ))
    team_values = [o.get_attribute("value") for o in team_select.options[1:]]

    for team_value in team_values:

        team_select.select_by_value(team_value)
        time.sleep(1)

        game_select = Select(driver.find_element(By.ID, "selectGameId"))
        game_values = [o.get_attribute("value") for o in game_select.options[1:]]

        for game_value in game_values[start_game_index:end_game_index]:

            game_select.select_by_value(game_value)
            time.sleep(1)

            WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "btnSearch"))
            ).click()
            time.sleep(2)

            year_text = year_select.first_selected_option.text
            meet_text = meet_select.first_selected_option.text
            team_text = team_select.first_selected_option.text
            game_text = game_select.first_selected_option.text

            soup = BeautifulSoup(driver.page_source, "html.parser")
            table = soup.find("table")

            if not table:
                continue

            rows = table.find_all("tr")

            for row in rows[2:-1]:

                cols = [c.text.strip() for c in row.find_all("td")]

                cols.extend([year_text, meet_text, team_text, game_text])
                all_table_data.append(cols)

    # --------------------------------------------------
    # 포털 컬럼 정의
    # --------------------------------------------------
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

    # ⭐ ETL 정규화
    df = normalize_to_etl_schema(df)

    return df


# --------------------------------------------------
# 실행부
# --------------------------------------------------
if __name__ == "__main__":

    driver = create_driver()

    df = scrape_match_data(
        driver,
        year_value=2025,
        meet_value=1,
        start_game_index=29,
        end_game_index=34
    )

    print("수집 row:", len(df))

    insert_dataframe(df)

    driver.quit()