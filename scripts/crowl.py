from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import Select

import pandas as pd
import time
import os 

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException

data = [
    "No,선수명,포지션,등번호,출전시간(분),"
    "득점,도움,슈팅,유효 슈팅,차단된슈팅,벗어난슈팅,PA내 슈팅,PA외 슈팅,"
    "오프사이드,프리킥,코너킥,스로인,"
    "드리블 시도,드리블 성공,드리블 성공%,"
    "패스 시도,패스 성공,패스 성공%,키패스,전방 패스 시도,전방 패스 성공,전방 패스 성공%,후방 패스 시도,후방 패스 성공,후방 패스 성공%,"
    "횡패스 시도,횡패스 성공,횡패스 성공%,"
    "공격지역패스 시도,공격지역패스 성공,공격지역패스 성공%,수비지역패스 시도,수비지역패스 성공,수비지역패스 성공%,"
    "중앙지역패스 시도,중앙지역패스 성공,중앙지역패스 성공%,롱패스 시도,롱패스 성공,롱패스 성공%,중거리패스 시도,중거리패스 성공,중거리패스 성공%,"
    "숏패스 시도,숏패스 성공,숏패스 성공%,크로스 시도,크로스 성공,크로스 성공%,"
    "경합 지상 시도,경합 지상 성공,경합 지상 성공%,경합 공중 시도,경합 공중 성공,경합 공중 성공%,태클 시도,태클 성공,태클 성공%,"
    "클리어링,인터셉트,차단,획득,블락,볼미스,파울,피파울,경고,퇴장,"
    "대회년도,대회명,팀명,경기명"
]

columns = data[0].split(",")

# 크롬 드라이버 실행 및 사이트 이동

options = Options()
chrome_options = webdriver.ChromeOptions()
options.page_load_strategy = 'eager'
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# 웹사이트에 접속합니다.
driver.get('https://data.kleague.com')

# Find frameset element and get its children
frames = driver.find_elements("tag name", 'frame')

for frame in frames:
    if 'https://portal.kleague.com' in frame.get_attribute('src'):
        # redirect the browser to the frame url
        driver.get(frame.get_attribute('src'))
    else:
        # do nothing
        pass

driver.execute_script("javascript:moveMainFrame('0208');")  # 경기별 선수기록

def scrape_match_data(driver, year_value, meet_value, start_game_index, end_game_index):
    # 수집한 데이터를 저장할 리스트
    all_table_data = []

    # 대회년도 필터 객체 생성 및 선택
    year_select_element = driver.find_element(By.ID, 'selectYear')
    year_select = Select(year_select_element)
    year_select.select_by_value(str(year_value))
    time.sleep(1)

    # 대회명 필터 객체 생성 및 선택
    meet_select = Select(driver.find_element(By.ID, 'selectMeetSeq'))
    meet_select.select_by_value(str(meet_value))
    time.sleep(1)

    # 팀 필터 객체 생성
    team_select_element = driver.find_element(By.ID, 'selectTeamId')
    team_select = Select(team_select_element)
    team_values = [option.get_attribute('value') for option in team_select.options[1:]]

    for team_value in team_values:
        team_select.select_by_value(team_value)
        time.sleep(1)

        # 경기 필터 객체 생성
        game_select_element = driver.find_element(By.ID, 'selectGameId')
        game_select = Select(game_select_element)
        game_values = [option.get_attribute('value') for option in game_select.options[1:]]

        for i, game_value in enumerate(game_values[start_game_index:end_game_index], start=start_game_index):
            try:
                game_select.select_by_value(game_value)
            except NoSuchElementException as e:
                print(f"경기 선택 실패 (value: {game_value}): {e}")
                continue

            time.sleep(1)

            try:
                WebDriverWait(driver, 10).until(EC.invisibility_of_element((By.ID, "loading_goal")))
            except Exception as e:
                print("로딩 화면이 사라지지 않음:", e)

            search_button = driver.find_element(By.ID, 'btnSearch')
            search_button.click()

            try:
                WebDriverWait(driver, 10).until(EC.invisibility_of_element((By.ID, "loading_goal")))
            except Exception as e:
                print("로딩 화면이 사라지지 않음:", e)

            time.sleep(3)

            # 최신 필터 값 가져오기
            year_select = Select(driver.find_element(By.ID, 'selectYear'))
            meet_select = Select(driver.find_element(By.ID, 'selectMeetSeq'))
            team_select = Select(driver.find_element(By.ID, 'selectTeamId'))
            game_select = Select(driver.find_element(By.ID, 'selectGameId'))

            selected_year = year_select.first_selected_option.text
            selected_meet = meet_select.first_selected_option.text
            selected_team = team_select.first_selected_option.text
            selected_game = game_select.first_selected_option.text

            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            table = soup.find('table')
            table_data = []

            if table:
                rows = table.find_all('tr')
                for row in rows[2:-1]:
                    cols = row.find_all('td')
                    cols = [ele.text.strip() for ele in cols]
                    cols.append(selected_year)
                    cols.append(selected_meet)
                    cols.append(selected_team)
                    cols.append(selected_game)
                    table_data.append(cols)

            all_table_data.extend(table_data)

    # 데이터프레임 변환
    df = pd.DataFrame(all_table_data, columns=columns)

    # 경기명 분해
    df[['라운드', '상대팀명']] = df['경기명'].str.split('/', expand=True)

    # % 포함 컬럼 제거
    df.drop(columns=[col for col in df.columns if '%' in col], inplace=True)

    return df

# 실제 선택한 파라미터에 대한 값만 스크래핑
league1 = scrape_match_data(driver, year_value=2025, meet_value=1, start_game_index=29, end_game_index=34)