import os
import sqlite3
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'database', 'kleague1.db')


def _conn():
    return sqlite3.connect(DB_PATH)


def get_seasons() -> list[int]:
    with _conn() as conn:
        df = pd.read_sql(
            "SELECT DISTINCT year FROM competitions ORDER BY year DESC", conn
        )
    return df['year'].tolist()


def get_teams(year: int) -> pd.DataFrame:
    with _conn() as conn:
        df = pd.read_sql("""
            SELECT DISTINCT t.team_id, t.team_name
            FROM teams t
            JOIN player_match_stats pms ON pms.team_id = t.team_id
            JOIN matches m ON pms.match_id = m.match_id
            JOIN competitions c ON m.competition_id = c.competition_id
            WHERE c.year = ?
            ORDER BY t.team_name
        """, conn, params=(year,))
    return df


def get_players(year: int, team_id: int) -> pd.DataFrame:
    with _conn() as conn:
        df = pd.read_sql("""
            SELECT DISTINCT p.player_id, p.player_name, p.position, p.back_number
            FROM players p
            JOIN player_match_stats pms ON pms.player_id = p.player_id
            JOIN matches m ON pms.match_id = m.match_id
            JOIN competitions c ON m.competition_id = c.competition_id
            WHERE c.year = ? AND pms.team_id = ?
            ORDER BY p.back_number
        """, conn, params=(year, team_id))
    return df


def get_player_season_stats(year: int, player_id: int) -> pd.DataFrame:
    """선수의 시즌 전체 경기 기록 반환"""
    with _conn() as conn:
        df = pd.read_sql("""
            SELECT pms.*
            FROM player_match_stats pms
            JOIN matches m ON pms.match_id = m.match_id
            JOIN competitions c ON m.competition_id = c.competition_id
            WHERE c.year = ? AND pms.player_id = ?
        """, conn, params=(year, player_id))
    return df


def get_season_all_stats(year: int) -> pd.DataFrame:
    """시즌 전체 선수 합산 기록 (퍼센타일 계산용)"""
    with _conn() as conn:
        df = pd.read_sql("""
            SELECT pms.player_id, SUM(pms.minutes_played) AS minutes_played,
                   SUM(pms.goals) AS goals, SUM(pms.assists) AS assists,
                   SUM(pms.shots) AS shots, SUM(pms.shots_on_target) AS shots_on_target,
                   SUM(pms.key_passes) AS key_passes,
                   SUM(pms.passes_attempted) AS passes_attempted,
                   SUM(pms.passes_successful) AS passes_successful,
                   SUM(pms.dribbles_attempted) AS dribbles_attempted,
                   SUM(pms.dribbles_successful) AS dribbles_successful,
                   SUM(pms.tackles_attempted) AS tackles_attempted,
                   SUM(pms.tackles_successful) AS tackles_successful,
                   SUM(pms.interceptions) AS interceptions,
                   SUM(pms.clearances) AS clearances,
                   SUM(pms.fouls_committed) AS fouls_committed,
                   SUM(pms.yellow_cards) AS yellow_cards
            FROM player_match_stats pms
            JOIN matches m ON pms.match_id = m.match_id
            JOIN competitions c ON m.competition_id = c.competition_id
            WHERE c.year = ?
            GROUP BY pms.player_id
            HAVING SUM(pms.minutes_played) >= 90
        """, conn, params=(year,))
    return df
