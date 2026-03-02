# scripts/db_setup.py

import sqlite3
import pandas as pd
import os
from datetime import datetime

def create_database():
    """데이터베이스와 테이블 생성"""
    
    db_path = 'database/soccer.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. 선수 마스터 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS players (
            player_id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name TEXT NOT NULL,
            position TEXT,
            back_number INTEGER,
            team_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(player_name, team_name)
        )
    ''')
    
    # 2. 팀 마스터 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS teams (
            team_id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_name TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 3. 대회 마스터 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS competitions (
            competition_id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            competition_name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(year, competition_name)
        )
    ''')
    
    # 4. 경기 마스터 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            match_id INTEGER PRIMARY KEY AUTOINCREMENT,
            competition_id INTEGER,
            round_number TEXT,
            home_team_id INTEGER,
            away_team_id INTEGER,
            match_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (competition_id) REFERENCES competitions(competition_id),
            FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
            FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
        )
    ''')
    
    # 5. 선수 경기 기록 메인 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_match_stats (
            stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER NOT NULL,
            player_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            
            -- 기본 정보
            minutes_played INTEGER DEFAULT 0,
            
            -- 공격 지표
            goals INTEGER DEFAULT 0,
            assists INTEGER DEFAULT 0,
            shots INTEGER DEFAULT 0,
            shots_on_target INTEGER DEFAULT 0,
            blocked_shots INTEGER DEFAULT 0,
            missed_shots INTEGER DEFAULT 0,
            shots_in_pa INTEGER DEFAULT 0,
            shots_out_pa INTEGER DEFAULT 0,
            
            -- 세트피스
            offsides INTEGER DEFAULT 0,
            freekicks INTEGER DEFAULT 0,
            corners INTEGER DEFAULT 0,
            throwins INTEGER DEFAULT 0,
            
            -- 드리블
            dribbles_attempted INTEGER DEFAULT 0,
            dribbles_successful INTEGER DEFAULT 0,
            
            -- 패스
            passes_attempted INTEGER DEFAULT 0,
            passes_successful INTEGER DEFAULT 0,
            key_passes INTEGER DEFAULT 0,
            forward_passes_attempted INTEGER DEFAULT 0,
            forward_passes_successful INTEGER DEFAULT 0,
            backward_passes_attempted INTEGER DEFAULT 0,
            backward_passes_successful INTEGER DEFAULT 0,
            lateral_passes_attempted INTEGER DEFAULT 0,
            lateral_passes_successful INTEGER DEFAULT 0,
            
            -- 지역별 패스
            attacking_third_passes_attempted INTEGER DEFAULT 0,
            attacking_third_passes_successful INTEGER DEFAULT 0,
            defensive_third_passes_attempted INTEGER DEFAULT 0,
            defensive_third_passes_successful INTEGER DEFAULT 0,
            middle_third_passes_attempted INTEGER DEFAULT 0,
            middle_third_passes_successful INTEGER DEFAULT 0,
            
            -- 거리별 패스
            long_passes_attempted INTEGER DEFAULT 0,
            long_passes_successful INTEGER DEFAULT 0,
            medium_passes_attempted INTEGER DEFAULT 0,
            medium_passes_successful INTEGER DEFAULT 0,
            short_passes_attempted INTEGER DEFAULT 0,
            short_passes_successful INTEGER DEFAULT 0,
            
            -- 크로스
            crosses_attempted INTEGER DEFAULT 0,
            crosses_successful INTEGER DEFAULT 0,
            
            -- 수비/경합
            ground_duels_attempted INTEGER DEFAULT 0,
            ground_duels_won INTEGER DEFAULT 0,
            aerial_duels_attempted INTEGER DEFAULT 0,
            aerial_duels_won INTEGER DEFAULT 0,
            tackles_attempted INTEGER DEFAULT 0,
            tackles_successful INTEGER DEFAULT 0,
            
            -- 수비 액션
            clearances INTEGER DEFAULT 0,
            interceptions INTEGER DEFAULT 0,
            blocks INTEGER DEFAULT 0,
            recoveries INTEGER DEFAULT 0,
            
            -- 징계/실책
            ball_losses INTEGER DEFAULT 0,
            fouls_committed INTEGER DEFAULT 0,
            fouls_won INTEGER DEFAULT 0,
            yellow_cards INTEGER DEFAULT 0,
            red_cards INTEGER DEFAULT 0,
            
            -- 메타
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (match_id) REFERENCES matches(match_id),
            FOREIGN KEY (player_id) REFERENCES players(player_id),
            FOREIGN KEY (team_id) REFERENCES teams(team_id),
            UNIQUE(match_id, player_id)
        )
    ''')
    
    # 인덱스 생성
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_match ON player_match_stats(player_id, match_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_match_date ON matches(match_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_name ON players(player_name)')
    
    conn.commit()
    conn.close()
    print(f"✅ 데이터베이스 생성 완료: {db_path}")


def clean_numeric_value(value):
    """숫자 데이터 정제"""
    if pd.isna(value) or value == '' or value == '-':
        return 0
    try:
        return int(float(value))
    except:
        return 0


def safe_get_column(row, possible_names):
    """
    여러 가능한 컬럼명 중 하나라도 있으면 값 반환
    
    Args:
        row: DataFrame row
        possible_names: 가능한 컬럼명 리스트 ['유효슈팅', '유효 슈팅']
    
    Returns:
        값 (없으면 0)
    """
    for name in possible_names:
        if name in row.index:
            return row[name]
    return 0


def import_csv_to_db(csv_path):
    """CSV 파일을 DB에 임포트 (컬럼명 자동 매칭)"""
    
    if not os.path.exists(csv_path):
        print(f"❌ CSV 파일을 찾을 수 없습니다: {csv_path}")
        return
    
    print(f"📂 CSV 파일 로드 중: {csv_path}")
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    print(f"📊 총 {len(df)}개 레코드 발견")
    
    # 컬럼명 출력 (디버깅용)
    print(f"\n📋 CSV 컬럼 (처음 10개): {list(df.columns[:10])}")
    
    conn = sqlite3.connect('database/soccer.db')
    cursor = conn.cursor()
    
    inserted_count = 0
    skipped_count = 0
    error_count = 0
    
    for idx, row in df.iterrows():
        try:
            # 1. 대회 정보
            cursor.execute('''
                INSERT OR IGNORE INTO competitions (year, competition_name)
                VALUES (?, ?)
            ''', (clean_numeric_value(row['대회년도']), row['대회명']))
            
            cursor.execute('''
                SELECT competition_id FROM competitions 
                WHERE year = ? AND competition_name = ?
            ''', (clean_numeric_value(row['대회년도']), row['대회명']))
            competition_id = cursor.fetchone()[0]
            
            # 2. 팀 정보
            cursor.execute('INSERT OR IGNORE INTO teams (team_name) VALUES (?)', (row['팀명'],))
            cursor.execute('SELECT team_id FROM teams WHERE team_name = ?', (row['팀명'],))
            team_id = cursor.fetchone()[0]
            
            # 상대팀
            opponent_team = row['상대팀명']
            cursor.execute('INSERT OR IGNORE INTO teams (team_name) VALUES (?)', (opponent_team,))
            cursor.execute('SELECT team_id FROM teams WHERE team_name = ?', (opponent_team,))
            opponent_team_id = cursor.fetchone()[0]
            
            # 3. 경기 정보
            round_number = row['라운드']
            
            cursor.execute('''
                INSERT OR IGNORE INTO matches (
                    competition_id, round_number, home_team_id, away_team_id
                ) VALUES (?, ?, ?, ?)
            ''', (competition_id, round_number, team_id, opponent_team_id))
            
            cursor.execute('''
                SELECT match_id FROM matches 
                WHERE competition_id = ? 
                AND round_number = ? 
                AND (
                    (home_team_id = ? AND away_team_id = ?) OR
                    (home_team_id = ? AND away_team_id = ?)
                )
            ''', (competition_id, round_number, team_id, opponent_team_id, 
                  opponent_team_id, team_id))
            
            match_result = cursor.fetchone()
            if match_result:
                match_id = match_result[0]
            else:
                error_count += 1
                continue
            
            # 4. 선수 정보
            player_name = row['선수명']
            position = row['포지션']
            back_number = clean_numeric_value(row['등번호'])
            
            cursor.execute('''
                INSERT OR REPLACE INTO players (player_name, position, back_number, team_name)
                VALUES (?, ?, ?, ?)
            ''', (player_name, position, back_number, row['팀명']))
            
            cursor.execute('''
                SELECT player_id FROM players 
                WHERE player_name = ? AND team_name = ?
            ''', (player_name, row['팀명']))
            player_id = cursor.fetchone()[0]
            
            # 5. 선수 경기 기록 (안전한 컬럼 접근)
            cursor.execute('''
                INSERT OR REPLACE INTO player_match_stats (
                    match_id, player_id, team_id,
                    minutes_played,
                    goals, assists,
                    shots, shots_on_target, blocked_shots, missed_shots,
                    shots_in_pa, shots_out_pa,
                    offsides, freekicks, corners, throwins,
                    dribbles_attempted, dribbles_successful,
                    passes_attempted, passes_successful, key_passes,
                    forward_passes_attempted, forward_passes_successful,
                    backward_passes_attempted, backward_passes_successful,
                    lateral_passes_attempted, lateral_passes_successful,
                    attacking_third_passes_attempted, attacking_third_passes_successful,
                    defensive_third_passes_attempted, defensive_third_passes_successful,
                    middle_third_passes_attempted, middle_third_passes_successful,
                    long_passes_attempted, long_passes_successful,
                    medium_passes_attempted, medium_passes_successful,
                    short_passes_attempted, short_passes_successful,
                    crosses_attempted, crosses_successful,
                    ground_duels_attempted, ground_duels_won,
                    aerial_duels_attempted, aerial_duels_won,
                    tackles_attempted, tackles_successful,
                    clearances, interceptions, blocks, recoveries,
                    ball_losses, fouls_committed, fouls_won,
                    yellow_cards, red_cards
                ) VALUES (
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?,
                    ?, ?, ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?
                )
            ''', (
                match_id, player_id, team_id,
                clean_numeric_value(safe_get_column(row, ['출전시간(분)', '출전시간'])),
                clean_numeric_value(safe_get_column(row, ['득점'])),
                clean_numeric_value(safe_get_column(row, ['도움'])),
                clean_numeric_value(safe_get_column(row, ['슈팅'])),
                clean_numeric_value(safe_get_column(row, ['유효슈팅', '유효 슈팅'])),
                clean_numeric_value(safe_get_column(row, ['차단된슈팅', '차단된 슈팅'])),
                clean_numeric_value(safe_get_column(row, ['벗어난슈팅', '벗어난 슈팅'])),
                clean_numeric_value(safe_get_column(row, ['PA내 슈팅', 'PA내슈팅'])),
                clean_numeric_value(safe_get_column(row, ['PA외 슈팅', 'PA외슈팅'])),
                clean_numeric_value(safe_get_column(row, ['오프사이드'])),
                clean_numeric_value(safe_get_column(row, ['프리킥'])),
                clean_numeric_value(safe_get_column(row, ['코너킥'])),
                clean_numeric_value(safe_get_column(row, ['스로인'])),
                clean_numeric_value(safe_get_column(row, ['드리블 시도', '드리블시도'])),
                clean_numeric_value(safe_get_column(row, ['드리블 성공', '드리블성공'])),
                clean_numeric_value(safe_get_column(row, ['패스 시도', '패스시도'])),
                clean_numeric_value(safe_get_column(row, ['패스 성공', '패스성공'])),
                clean_numeric_value(safe_get_column(row, ['키패스'])),
                clean_numeric_value(safe_get_column(row, ['전방 패스 시도', '전방패스시도'])),
                clean_numeric_value(safe_get_column(row, ['전방 패스 성공', '전방패스성공'])),
                clean_numeric_value(safe_get_column(row, ['후방 패스 시도', '후방패스시도'])),
                clean_numeric_value(safe_get_column(row, ['후방 패스 성공', '후방패스성공'])),
                clean_numeric_value(safe_get_column(row, ['횡패스 시도', '횡패스시도'])),
                clean_numeric_value(safe_get_column(row, ['횡패스 성공', '횡패스성공'])),
                clean_numeric_value(safe_get_column(row, ['공격지역패스 시도', '공격지역패스시도'])),
                clean_numeric_value(safe_get_column(row, ['공격지역패스 성공', '공격지역패스성공'])),
                clean_numeric_value(safe_get_column(row, ['수비지역패스 시도', '수비지역패스시도'])),
                clean_numeric_value(safe_get_column(row, ['수비지역패스 성공', '수비지역패스성공'])),
                clean_numeric_value(safe_get_column(row, ['중앙지역패스 시도', '중앙지역패스시도'])),
                clean_numeric_value(safe_get_column(row, ['중앙지역패스 성공', '중앙지역패스성공'])),
                clean_numeric_value(safe_get_column(row, ['롱패스 시도', '롱패스시도'])),
                clean_numeric_value(safe_get_column(row, ['롱패스 성공', '롱패스성공'])),
                clean_numeric_value(safe_get_column(row, ['중거리패스 시도', '중거리패스시도'])),
                clean_numeric_value(safe_get_column(row, ['중거리패스 성공', '중거리패스성공'])),
                clean_numeric_value(safe_get_column(row, ['숏패스 시도', '숏패스시도'])),
                clean_numeric_value(safe_get_column(row, ['숏패스 성공', '숏패스성공'])),
                clean_numeric_value(safe_get_column(row, ['크로스 시도', '크로스시도'])),
                clean_numeric_value(safe_get_column(row, ['크로스 성공', '크로스성공'])),
                clean_numeric_value(safe_get_column(row, ['경합 지상 시도', '경합지상시도'])),
                clean_numeric_value(safe_get_column(row, ['경합 지상 성공', '경합지상성공'])),
                clean_numeric_value(safe_get_column(row, ['경합 공중 시도', '경합공중시도'])),
                clean_numeric_value(safe_get_column(row, ['경합 공중 성공', '경합공중성공'])),
                clean_numeric_value(safe_get_column(row, ['태클 시도', '태클시도'])),
                clean_numeric_value(safe_get_column(row, ['태클 성공', '태클성공'])),
                clean_numeric_value(safe_get_column(row, ['클리어링'])),
                clean_numeric_value(safe_get_column(row, ['인터셉트'])),
                clean_numeric_value(safe_get_column(row, ['블록'])),
                clean_numeric_value(safe_get_column(row, ['횟득'])),
                clean_numeric_value(safe_get_column(row, ['볼미스'])),
                clean_numeric_value(safe_get_column(row, ['파울'])),
                clean_numeric_value(safe_get_column(row, ['피파울'])),
                clean_numeric_value(safe_get_column(row, ['경고'])),
                clean_numeric_value(safe_get_column(row, ['퇴장']))
            ))
            
            inserted_count += 1
            
            if (idx + 1) % 100 == 0:
                print(f"   진행: {idx + 1}/{len(df)} 행 처리 중...")
            
        except Exception as e:
            error_count += 1
            print(f"❌ 행 {idx} 처리 중 오류: {e}")
            print(f"   선수: {row.get('선수명', 'Unknown')}, 경기: {row.get('경기명', 'Unknown')}")
            # 첫 10개 오류만 상세히 출력
            if error_count <= 10:
                print(f"   사용 가능한 컬럼: {list(row.index[:5])}...")
            continue
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ 데이터 임포트 완료!")
    print(f"   - 성공: {inserted_count}건")
    print(f"   - 건너뜀: {skipped_count}건")
    print(f"   - 오류: {error_count}건")


def get_db_stats():
    """데이터베이스 통계 조회"""
    conn = sqlite3.connect('database/soccer.db')
    cursor = conn.cursor()
    
    print("\n📊 데이터베이스 통계")
    print("=" * 50)
    
    cursor.execute("SELECT COUNT(*) FROM players")
    print(f"총 선수 수: {cursor.fetchone()[0]}명")
    
    cursor.execute("SELECT COUNT(*) FROM teams")
    print(f"총 팀 수: {cursor.fetchone()[0]}팀")
    
    cursor.execute("SELECT COUNT(*) FROM matches")
    print(f"총 경기 수: {cursor.fetchone()[0]}경기")
    
    cursor.execute("SELECT COUNT(*) FROM player_match_stats")
    print(f"총 선수 경기 기록: {cursor.fetchone()[0]}건")
    
    cursor.execute("""
        SELECT c.year, c.competition_name, m.round_number
        FROM matches m
        JOIN competitions c ON m.competition_id = c.competition_id
        ORDER BY m.match_id DESC
        LIMIT 1
    """)
    result = cursor.fetchone()
    if result:
        print(f"최근 경기: {result[0]}년 {result[1]} {result[2]}")
    
    conn.close()


if __name__ == '__main__':
    # 1. DB 생성
    create_database()
    
    # 2. CSV 임포트
    csv_file = 'data/raw/2025_KLEAGUE1/2025K리그1_경기기록_R29.csv'
    if os.path.exists(csv_file):
        import_csv_to_db(csv_file)
        get_db_stats()
    else:
        print(f"⚠️  CSV 파일을 찾을 수 없습니다: {csv_file}")
        print("   data/raw/ 폴더에 스크래핑한 CSV 파일을 넣어주세요.")