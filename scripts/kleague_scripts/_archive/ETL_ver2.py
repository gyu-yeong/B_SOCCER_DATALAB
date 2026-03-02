# scripts/db_setup_v2.py
"""
중복 방지 개선 ETL 코드
- 이미 임포트된 라운드 자동 감지
- 중복 데이터 방지
- 기존 중복 데이터 정리 기능 추가
"""

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
    
    # 4. 경기 마스터 테이블 (UNIQUE 제약 강화!)
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
            FOREIGN KEY (away_team_id) REFERENCES teams(team_id),
            UNIQUE(competition_id, round_number, home_team_id, away_team_id)
        )
    ''')
    
    # 5. 선수 경기 기록 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_match_stats (
            stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER NOT NULL,
            player_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            
            minutes_played INTEGER DEFAULT 0,
            goals INTEGER DEFAULT 0,
            assists INTEGER DEFAULT 0,
            shots INTEGER DEFAULT 0,
            shots_on_target INTEGER DEFAULT 0,
            blocked_shots INTEGER DEFAULT 0,
            missed_shots INTEGER DEFAULT 0,
            shots_in_pa INTEGER DEFAULT 0,
            shots_out_pa INTEGER DEFAULT 0,
            offsides INTEGER DEFAULT 0,
            freekicks INTEGER DEFAULT 0,
            corners INTEGER DEFAULT 0,
            throwins INTEGER DEFAULT 0,
            dribbles_attempted INTEGER DEFAULT 0,
            dribbles_successful INTEGER DEFAULT 0,
            passes_attempted INTEGER DEFAULT 0,
            passes_successful INTEGER DEFAULT 0,
            key_passes INTEGER DEFAULT 0,
            forward_passes_attempted INTEGER DEFAULT 0,
            forward_passes_successful INTEGER DEFAULT 0,
            backward_passes_attempted INTEGER DEFAULT 0,
            backward_passes_successful INTEGER DEFAULT 0,
            lateral_passes_attempted INTEGER DEFAULT 0,
            lateral_passes_successful INTEGER DEFAULT 0,
            attacking_third_passes_attempted INTEGER DEFAULT 0,
            attacking_third_passes_successful INTEGER DEFAULT 0,
            defensive_third_passes_attempted INTEGER DEFAULT 0,
            defensive_third_passes_successful INTEGER DEFAULT 0,
            middle_third_passes_attempted INTEGER DEFAULT 0,
            middle_third_passes_successful INTEGER DEFAULT 0,
            long_passes_attempted INTEGER DEFAULT 0,
            long_passes_successful INTEGER DEFAULT 0,
            medium_passes_attempted INTEGER DEFAULT 0,
            medium_passes_successful INTEGER DEFAULT 0,
            short_passes_attempted INTEGER DEFAULT 0,
            short_passes_successful INTEGER DEFAULT 0,
            crosses_attempted INTEGER DEFAULT 0,
            crosses_successful INTEGER DEFAULT 0,
            ground_duels_attempted INTEGER DEFAULT 0,
            ground_duels_won INTEGER DEFAULT 0,
            aerial_duels_attempted INTEGER DEFAULT 0,
            aerial_duels_won INTEGER DEFAULT 0,
            tackles_attempted INTEGER DEFAULT 0,
            tackles_successful INTEGER DEFAULT 0,
            clearances INTEGER DEFAULT 0,
            interceptions INTEGER DEFAULT 0,
            blocks INTEGER DEFAULT 0,
            recoveries INTEGER DEFAULT 0,
            ball_losses INTEGER DEFAULT 0,
            fouls_committed INTEGER DEFAULT 0,
            fouls_won INTEGER DEFAULT 0,
            yellow_cards INTEGER DEFAULT 0,
            red_cards INTEGER DEFAULT 0,
            
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
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_round ON matches(competition_id, round_number)')
    
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


def check_already_imported(csv_path):
    """
    CSV 파일이 이미 임포트되었는지 확인
    
    Returns:
        dict: {
            'is_imported': bool,
            'competition': str,
            'year': int,
            'rounds': list
        }
    """
    if not os.path.exists(csv_path):
        return {'is_imported': False}
    
    # CSV 메타 정보 추출
    df = pd.read_csv(csv_path, encoding='utf-8-sig', nrows=10)
    
    if len(df) == 0:
        return {'is_imported': False}
    
    year = clean_numeric_value(df['대회년도'].iloc[0])
    competition = df['대회명'].iloc[0]
    
    # 라운드 목록 추출
    df_full = pd.read_csv(csv_path, encoding='utf-8-sig')
    rounds = df_full['라운드'].unique().tolist()
    
    # DB 확인
    conn = sqlite3.connect('database/soccer.db')
    cursor = conn.cursor()
    
    # 대회 ID 조회
    cursor.execute('''
        SELECT competition_id FROM competitions 
        WHERE year = ? AND competition_name = ?
    ''', (year, competition))
    
    comp_result = cursor.fetchone()
    
    if not comp_result:
        conn.close()
        return {
            'is_imported': False,
            'competition': competition,
            'year': year,
            'rounds': rounds
        }
    
    competition_id = comp_result[0]
    
    # 이미 임포트된 라운드 확인
    imported_rounds = []
    for round_num in rounds:
        cursor.execute('''
            SELECT COUNT(*) FROM matches 
            WHERE competition_id = ? AND round_number = ?
        ''', (competition_id, round_num))
        
        if cursor.fetchone()[0] > 0:
            imported_rounds.append(round_num)
    
    conn.close()
    
    return {
        'is_imported': len(imported_rounds) > 0,
        'competition': competition,
        'year': year,
        'rounds': rounds,
        'imported_rounds': imported_rounds,
        'new_rounds': [r for r in rounds if r not in imported_rounds]
    }


def import_csv_to_db(csv_path, force=False, skip_duplicates=True):
    """
    CSV 파일을 DB에 임포트 (중복 방지)
    
    Args:
        csv_path: CSV 파일 경로
        force: True면 중복 체크 무시하고 강제 임포트
        skip_duplicates: True면 중복 라운드 건너뛰기, False면 에러
    """
    
    if not os.path.exists(csv_path):
        print(f"❌ CSV 파일을 찾을 수 없습니다: {csv_path}")
        return
    
    # 중복 체크
    if not force:
        check_result = check_already_imported(csv_path)
        
        if check_result['is_imported']:
            print(f"\n⚠️  중복 데이터 감지!")
            print(f"   대회: {check_result['year']}년 {check_result['competition']}")
            print(f"   이미 임포트된 라운드: {check_result['imported_rounds']}")
            
            if len(check_result['new_rounds']) > 0:
                print(f"   새로운 라운드: {check_result['new_rounds']}")
                
                if skip_duplicates:
                    print(f"\n✅ 새로운 라운드만 임포트합니다...")
                    # 새 라운드만 필터링
                    df = pd.read_csv(csv_path, encoding='utf-8-sig')
                    df = df[df['라운드'].isin(check_result['new_rounds'])]
                    
                    if len(df) == 0:
                        print("⚠️  임포트할 새 데이터가 없습니다.")
                        return
                else:
                    print("\n❌ 중복된 라운드가 있어 임포트를 중단합니다.")
                    print("   force=True 옵션으로 강제 임포트하거나")
                    print("   clean_duplicates() 함수로 기존 데이터를 먼저 정리하세요.")
                    return
            else:
                print("\n⚠️  모든 라운드가 이미 임포트되었습니다.")
                print("   force=True 옵션으로 재임포트하거나")
                print("   다른 CSV 파일을 선택하세요.")
                return
    
    print(f"\n📂 CSV 파일 로드 중: {csv_path}")
    
    if 'df' not in locals():
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
    
    print(f"📊 총 {len(df)}개 레코드 발견")
    
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
            
            opponent_team = row['상대팀명']
            cursor.execute('INSERT OR IGNORE INTO teams (team_name) VALUES (?)', (opponent_team,))
            cursor.execute('SELECT team_id FROM teams WHERE team_name = ?', (opponent_team,))
            opponent_team_id = cursor.fetchone()[0]
            
            # 3. 경기 정보 (INSERT OR IGNORE로 중복 방지)
            round_number = row['라운드']
            
            cursor.execute('''
                INSERT OR IGNORE INTO matches (
                    competition_id, round_number, home_team_id, away_team_id
                ) VALUES (?, ?, ?, ?)
            ''', (competition_id, round_number, team_id, opponent_team_id))
            
            # 경기 ID 조회 (이미 있으면 기존 ID 사용)
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
                INSERT OR IGNORE INTO players (player_name, position, back_number, team_name)
                VALUES (?, ?, ?, ?)
            ''', (player_name, position, back_number, row['팀명']))
            
            cursor.execute('''
                SELECT player_id FROM players 
                WHERE player_name = ? AND team_name = ?
            ''', (player_name, row['팀명']))
            player_id = cursor.fetchone()[0]
            
            # 5. 선수 경기 기록 (INSERT OR REPLACE)
            cursor.execute('''
                INSERT OR REPLACE INTO player_match_stats (
                    match_id, player_id, team_id,
                    minutes_played, goals, assists,
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
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?
                )
            ''', (
                match_id, player_id, team_id,
                clean_numeric_value(row['출전시간(분)']),
                clean_numeric_value(row['득점']),
                clean_numeric_value(row['도움']),
                clean_numeric_value(row['슈팅']),
                clean_numeric_value(row['유효 슈팅']),
                clean_numeric_value(row['차단된슈팅']),
                clean_numeric_value(row['벗어난슈팅']),
                clean_numeric_value(row['PA내 슈팅']),
                clean_numeric_value(row['PA외 슈팅']),
                clean_numeric_value(row['오프사이드']),
                clean_numeric_value(row['프리킥']),
                clean_numeric_value(row['코너킥']),
                clean_numeric_value(row['스로인']),
                clean_numeric_value(row['드리블 시도']),
                clean_numeric_value(row['드리블 성공']),
                clean_numeric_value(row['패스 시도']),
                clean_numeric_value(row['패스 성공']),
                clean_numeric_value(row['키패스']),
                clean_numeric_value(row['전방 패스 시도']),
                clean_numeric_value(row['전방 패스 성공']),
                clean_numeric_value(row['후방 패스 시도']),
                clean_numeric_value(row['후방 패스 성공']),
                clean_numeric_value(row['횡패스 시도']),
                clean_numeric_value(row['횡패스 성공']),
                clean_numeric_value(row['공격지역패스 시도']),
                clean_numeric_value(row['공격지역패스 성공']),
                clean_numeric_value(row['수비지역패스 시도']),
                clean_numeric_value(row['수비지역패스 성공']),
                clean_numeric_value(row['중앙지역패스 시도']),
                clean_numeric_value(row['중앙지역패스 성공']),
                clean_numeric_value(row['롱패스 시도']),
                clean_numeric_value(row['롱패스 성공']),
                clean_numeric_value(row['중거리패스 시도']),
                clean_numeric_value(row['중거리패스 성공']),
                clean_numeric_value(row['숏패스 시도']),
                clean_numeric_value(row['숏패스 성공']),
                clean_numeric_value(row['크로스 시도']),
                clean_numeric_value(row['크로스 성공']),
                clean_numeric_value(row['경합 지상 시도']),
                clean_numeric_value(row['경합 지상 성공']),
                clean_numeric_value(row['경합 공중 시도']),
                clean_numeric_value(row['경합 공중 성공']),
                clean_numeric_value(row['태클 시도']),
                clean_numeric_value(row['태클 성공']),
                clean_numeric_value(row['클리어링']),
                clean_numeric_value(row['인터셉트']),
                clean_numeric_value(row['차단']),
                clean_numeric_value(row['획득']),
                clean_numeric_value(row['볼미스']),
                clean_numeric_value(row['파울']),
                clean_numeric_value(row['피파울']),
                clean_numeric_value(row['경고']),
                clean_numeric_value(row['퇴장'])
            ))
            
            inserted_count += 1
            
            if (idx + 1) % 100 == 0:
                print(f"   진행: {idx + 1}/{len(df)} 행 처리 중...")
            
        except Exception as e:
            error_count += 1
            if error_count <= 5:
                print(f"❌ 행 {idx} 처리 중 오류: {e}")
                print(f"   선수: {row.get('선수명', 'Unknown')}, 경기: {row.get('경기명', 'Unknown')}")
            continue
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ 데이터 임포트 완료!")
    print(f"   - 성공: {inserted_count}건")
    print(f"   - 건너뜀: {skipped_count}건")
    print(f"   - 오류: {error_count}건")


def clean_duplicates():
    """기존 중복 데이터 정리"""
    
    print("\n🧹 중복 데이터 정리 시작...")
    
    conn = sqlite3.connect('database/soccer.db')
    cursor = conn.cursor()
    
    # 중복 확인
    cursor.execute('''
        SELECT match_id, player_id, COUNT(*) as cnt
        FROM player_match_stats
        GROUP BY match_id, player_id
        HAVING cnt > 1
    ''')
    
    duplicates = cursor.fetchall()
    
    if len(duplicates) == 0:
        print("✅ 중복 데이터가 없습니다.")
        conn.close()
        return
    
    print(f"⚠️  {len(duplicates)}개 중복 발견")
    
    # 중복 제거 (stat_id가 작은 것만 남김)
    cursor.execute('''
        DELETE FROM player_match_stats 
        WHERE stat_id NOT IN (
            SELECT MIN(stat_id) 
            FROM player_match_stats 
            GROUP BY match_id, player_id
        )
    ''')
    
    deleted_count = cursor.rowcount
    conn.commit()
    conn.close()
    
    print(f"✅ {deleted_count}개 중복 데이터 삭제 완료")


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
    
    # 대회별 라운드 통계
    cursor.execute('''
        SELECT c.year, c.competition_name, 
               GROUP_CONCAT(DISTINCT m.round_number ORDER BY m.round_number) as rounds
        FROM matches m
        JOIN competitions c ON m.competition_id = c.competition_id
        GROUP BY c.competition_id
    ''')
    
    print("\n📋 대회별 라운드:")
    for row in cursor.fetchall():
        year, comp, rounds = row
        round_list = rounds.split(',') if rounds else []
        print(f"  {year}년 {comp}: {len(round_list)}개 라운드 ({rounds})")
    
    conn.close()


if __name__ == '__main__':
    # 1. DB 생성
    create_database()
    
    # 2. 기존 중복 데이터 정리 (선택사항)
    # clean_duplicates()
    
    # 3. CSV 임포트 (중복 자동 감지)
    csv_file = 'data/raw/2025_KLEAGUE1/2025K리그1_경기기록_R30_34.csv'
    
    if os.path.exists(csv_file):
        import_csv_to_db(csv_file, force=False, skip_duplicates=True)
        get_db_stats()
    else:
        print(f"⚠️  CSV 파일을 찾을 수 없습니다: {csv_file}")