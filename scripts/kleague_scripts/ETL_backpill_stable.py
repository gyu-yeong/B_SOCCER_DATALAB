import re
import time
import random
import pandas as pd
import undetected_chromedriver as uc

from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from selenium import webdriver
from bs4 import BeautifulSoup
from ETL_ver4 import insert_dataframe


# --------------------------------------------------
# human sleep
# --------------------------------------------------
def human_sleep(a=2, b=5):
    time.sleep(random.uniform(a, b))


# --------------------------------------------------
# driver 생성
# --------------------------------------------------
def create_driver():

    options = Options()
    options.page_load_strategy = "eager"

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    driver.get("https://data.kleague.com")

    human_sleep(3, 6)

    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )

    # frame 이동
    frames = driver.find_elements(By.TAG_NAME, "frame")

    for frame in frames:
        src = frame.get_attribute("src")
        if src and "portal.kleague.com" in src:
            driver.get(src)
            break

    human_sleep(2, 4)
    driver.execute_script("javascript:moveMainFrame('0208');")
    human_sleep(3, 5)

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
            raw_opponent = split_cols[1].str.strip()
            # (H) = 현재 행의 팀이 홈=1, (A) = 어웨이=0
            df["홈여부"] = raw_opponent.str.contains(r'\(H\)$').astype(int)
            df["상대팀명"] = raw_opponent.str.replace(r'\s*\([HA]\)$', '', regex=True).str.strip()

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
        "상대팀명",
        "홈여부"
    ]

    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    # 5. 중복 컬럼 제거
    df = df.loc[:, ~df.columns.duplicated()]

    return df

# --------------------------------------------------
# safe select
# --------------------------------------------------
def safe_select(driver, select_id, value):

    for _ in range(3):
        try:
            select = Select(
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.ID, select_id))
                )
            )
            select.select_by_value(value)
            return select

        except StaleElementReferenceException:
            human_sleep(1, 2)

    raise Exception(f"{select_id} select 실패")


# --------------------------------------------------
# restart 후 상태 복구
# --------------------------------------------------
def restore_state(driver, year_value, meet_value, team_value):

    safe_select(driver, "selectYear", str(year_value))
    human_sleep()

    safe_select(driver, "selectMeetSeq", str(meet_value))
    human_sleep()

    safe_select(driver, "selectTeamId", team_value)
    human_sleep(2, 4)


# --------------------------------------------------
# scraper
# --------------------------------------------------
def scrape_match_data(driver, year_value, meet_value, from_round=1):

    all_table_data = []
    game_counter = 0

    # year / meet
    safe_select(driver, "selectYear", str(year_value))
    human_sleep()

    safe_select(driver, "selectMeetSeq", str(meet_value))
    human_sleep()

    # 팀 목록
    team_select = Select(
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "selectTeamId"))
        )
    )

    team_values = [o.get_attribute("value") for o in team_select.options[1:]]

    # --------------------------------------------------
    # team loop
    # --------------------------------------------------
    for team_value in team_values:

        safe_select(driver, "selectTeamId", team_value)
        human_sleep(3, 6)

        game_select = Select(
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "selectGameId"))
            )
        )

        game_options = [
            (o.get_attribute("value"), o.text.strip())
            for o in game_select.options[1:]
        ]

        # --------------------------------------------------
        # game loop
        # --------------------------------------------------
        for game_value, game_label in game_options:

            # from_round 필터: 드롭다운 텍스트에서 라운드 번호 파싱 후 skip
            if from_round > 1:
                try:
                    round_num = int(re.search(r'(\d+)R', game_label).group(1))
                    if round_num < from_round:
                        continue
                except (AttributeError, ValueError):
                    pass  # 파싱 실패 시 수집 진행

            # ⭐ driver restart (루프 유지형)
            if game_counter != 0 and game_counter % 20 == 0:

                print("♻ driver restart")

                driver.quit()
                driver = create_driver()

                # 상태 복구
                restore_state(driver, year_value, meet_value, team_value)

            try:

                safe_select(driver, "selectGameId", game_value)
                human_sleep(2, 4)

                WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "btnSearch"))
                ).click()

                human_sleep(4, 7)

                soup = BeautifulSoup(driver.page_source, "html.parser")
                table = soup.find("table")

                if not table:
                    continue

                # 선택 텍스트 확보
                year_text = Select(driver.find_element(By.ID, "selectYear")).first_selected_option.text
                meet_text = Select(driver.find_element(By.ID, "selectMeetSeq")).first_selected_option.text
                team_text = Select(driver.find_element(By.ID, "selectTeamId")).first_selected_option.text
                game_text = Select(driver.find_element(By.ID, "selectGameId")).first_selected_option.text

                rows = table.find_all("tr")

                # --------------------------------------------------
                # [DOM 탐색] 첫 번째 경기에서만 선수명 셀 HTML attr 확인
                # → 포털 고유 player ID 존재 여부 파악 후 이 블록 제거
                # --------------------------------------------------
                if game_counter == 0 and rows[2:3]:
                    print("[DOM 탐색] 선수명 셀 href/onclick 확인:")
                    for cell in rows[2].find_all("td"):
                        link = cell.find("a")
                        if link:
                            print(f"  text={cell.text.strip()!r}  href={link.get('href')!r}  onclick={link.get('onclick')!r}")
                    print("[DOM 탐색 끝]")

                for row in rows[2:-1]:
                    cols = [c.text.strip() for c in row.find_all("td")]
                    cols.extend([year_text, meet_text, team_text, game_text])
                    all_table_data.append(cols)

                game_counter += 1
                human_sleep(3, 6)

            except Exception as e:
                print("⚠ 경기 수집 실패 → skip", e)
                continue

    # --------------------------------------------------
    # dataframe 생성
    # --------------------------------------------------
    columns = [
        "No","선수명","포지션","등번호","출전시간(분)","득점","도움","슈팅","유효 슈팅",
        "차단된슈팅","벗어난슈팅","PA내 슈팅","PA외 슈팅","오프사이드","프리킥","코너킥",
        "스로인","드리블 시도","드리블 성공","드리블 성공%",
        "패스 시도","패스 성공","패스 성공%","키패스",
        "전방 패스 시도","전방 패스 성공","전방 패스 성공%",
        "후방 패스 시도","후방 패스 성공","후방 패스 성공%",
        "횡패스 시도","횡패스 성공","횡패스 성공%",
        "공격지역패스 시도","공격지역패스 성공","공격지역패스 성공%",
        "수비지역패스 시도","수비지역패스 성공","수비지역패스 성공%",
        "중앙지역패스 시도","중앙지역패스 성공","중앙지역패스 성공%",
        "롱패스 시도","롱패스 성공","롱패스 성공%",
        "중거리패스 시도","중거리패스 성공","중거리패스 성공%",
        "숏패스 시도","숏패스 성공","숏패스 성공%",
        "크로스 시도","크로스 성공","크로스 성공%",
        "경합 지상 시도","경합 지상 성공","경합 지상 성공%",
        "경합 공중 시도","경합 공중 성공","경합 공중 성공%",
        "태클 시도","태클 성공","태클 성공%",
        "클리어링","인터셉트","차단","획득","블락",
        "볼미스","파울","피파울","경고","퇴장",
        "대회년도","대회명","팀명","경기명"
    ]

    df = pd.DataFrame(all_table_data, columns=columns)

    df = normalize_to_etl_schema(df)

    return df


# --------------------------------------------------
# main
# --------------------------------------------------
if __name__ == "__main__":

    driver = create_driver()

    # from_round: 이 라운드부터 수집 (기본값=1, 즉 전체)
    # 예) 2025시즌 34R부터 수집: scrape_match_data(driver, 2025, 1, from_round=34)
    df = scrape_match_data(driver, 2025, 1, from_round=34)

    print("수집 row:", len(df))

    insert_dataframe(df)

    driver.quit()
