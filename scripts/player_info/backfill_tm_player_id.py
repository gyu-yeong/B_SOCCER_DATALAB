"""
Transfermarkt Player ID 백필 스크립트
기존 player_master 레코드(tm_player_id IS NULL)에
Transfermarkt player ID를 역으로 채운다.

매칭 전략 (이름 표기 차이에 의존하지 않음):
  1순위: birth_date 정확 일치 + DB 내 유일 → 자동 UPDATE
  2순위: birth_date 일치 + 복수 후보 → current_club 유사도로 추가 필터
  3순위: 매칭 실패 → unmatched_{season}.csv 출력 (수동 처리)

Usage:
    python scripts/player_info/backfill_tm_player_id.py --season 2026
    python scripts/player_info/backfill_tm_player_id.py --season 2026 --league kl1
"""

import argparse
import re
import sys
import time
import random
import sqlite3
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup

import requests

sys.stdout.reconfigure(encoding="utf-8")

# ─────────────────────────────────────────────
# 경로 상수
# ─────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH      = PROJECT_ROOT / "database" / "kleague.db"
OUTPUT_DIR   = PROJECT_ROOT / "data" / "raw"

BASE_URL = "https://www.transfermarkt.com"

LEAGUES = {
    "kl1": {"label": "K League 1", "tm_code": "RSK1", "slug": "k-league-1"},
    "kl2": {"label": "K League 2", "tm_code": "RSK2", "slug": "k-league-2"},
}

TEAM_LINK_PATTERN   = re.compile(r"^/(.+)/startseite/verein/(\d+)")
PLAYER_ID_PATTERN   = re.compile(r"/spieler/(\d+)")
TABLE_SELECTOR      = "table.items"
ROW_SELECTOR        = "tr.odd, tr.even"


# ─────────────────────────────────────────────
# 파싱 유틸
# ─────────────────────────────────────────────

def parse_birth_date(val: str) -> str | None:
    """'11/12/2003 (22)' → '2003-12-11'"""
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", str(val))
    if m:
        day, month, year = m.groups()
        return f"{year}-{month}-{day}"
    return None


def human_sleep(a: float = 3, b: float = 7) -> None:
    time.sleep(random.uniform(a, b))


# ─────────────────────────────────────────────
# HTTP 세션 (requests 기반 — 팝업 없음)
# ─────────────────────────────────────────────

def make_session() -> requests.Session:
    """노트북과 동일한 User-Agent로 requests 세션 생성."""
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    })
    return sess


def tm_saison_id(season: int) -> int:
    """실제 시즌 → Transfermarkt saison_id 변환 (season - 2)."""
    return season - 2


# ─────────────────────────────────────────────
# DB: 기존 레코드 로드 & 업데이트
# ─────────────────────────────────────────────

def add_tm_player_id_column(conn: sqlite3.Connection) -> None:
    """tm_player_id 컬럼이 없으면 추가 (멱등)"""
    existing = {
        row[1]
        for row in conn.execute("PRAGMA table_info(player_master)").fetchall()
    }
    if "tm_player_id" not in existing:
        conn.execute("ALTER TABLE player_master ADD COLUMN tm_player_id TEXT")
        conn.commit()
        print("  [DB] 컬럼 추가: player_master.tm_player_id")


def load_unmatched_players(conn: sqlite3.Connection) -> pd.DataFrame:
    """tm_player_id가 NULL인 레코드를 로드한다."""
    df = pd.read_sql(
        """
        SELECT master_id, name_original, player_name, birth_date, current_club
        FROM player_master
        WHERE tm_player_id IS NULL
        ORDER BY current_club, name_original
        """,
        conn,
    )
    return df


def update_tm_player_id(conn: sqlite3.Connection, matches: list[dict]) -> int:
    """
    매칭된 결과를 DB에 반영한다.
    matches: [{"master_id": int, "tm_player_id": str}]
    """
    updated = 0
    for m in matches:
        conn.execute(
            "UPDATE player_master SET tm_player_id = ? WHERE master_id = ?",
            (m["tm_player_id"], m["master_id"]),
        )
        updated += 1
    conn.commit()
    return updated


# ─────────────────────────────────────────────
# 스크래핑: kader 페이지에서 (birth_date, tm_player_id) 수집
# ─────────────────────────────────────────────

def get_team_list(sess: requests.Session, season: int, league: dict) -> list[dict]:
    saison_id = tm_saison_id(season)
    url = (
        f"{BASE_URL}/{league['slug']}/startseite/wettbewerb/"
        f"{league['tm_code']}/plus/?saison_id={saison_id}"
    )
    print(f"  [팀 목록] {league['label']} (saison_id={saison_id}) 접속 중...")
    resp = sess.get(url, timeout=30)
    soup = BeautifulSoup(resp.text, "html.parser")

    teams, seen = [], set()
    for a in soup.select("table.items td.hauptlink a"):
        href = a.get("href", "")
        m = TEAM_LINK_PATTERN.match(href)
        if not m:
            continue
        slug, tm_id = m.group(1), m.group(2)
        if tm_id in seen:
            continue
        seen.add(tm_id)
        name = a.get_text(strip=True)
        if name:
            teams.append({"slug": slug, "tm_id": tm_id, "team_name": name})

    print(f"  → {len(teams)}개 팀")
    return teams


def scrape_kader_for_ids(
    sess: requests.Session, team: dict, season: int, max_retries: int = 2,
) -> list[dict]:
    """
    kader 페이지에서 각 선수의 (tm_player_id, birth_date, team_name)을 수집한다.
    이름은 수집하지 않아도 됨 — birth_date + team으로 매칭하기 때문.
    실패 시 max_retries 횟수만큼 재시도.
    """
    saison_id = tm_saison_id(season)
    url = (
        f"{BASE_URL}/{team['slug']}/kader/verein/{team['tm_id']}"
        f"/plus/1?saison_id={saison_id}"
    )

    for attempt in range(1, max_retries + 2):
        try:
            resp = sess.get(url, timeout=30)
            soup = BeautifulSoup(resp.text, "html.parser")
            table = soup.select_one(TABLE_SELECTOR)
            if not table:
                raise ValueError("table.items 없음")
            human_sleep(1, 2)
            break
        except Exception as e:
            if attempt <= max_retries:
                print(f"    ⚠ {team['team_name']} 시도 {attempt} 실패 ({e}) — 재시도 중...")
                human_sleep(3, 6)
            else:
                print(f"    ✗ {team['team_name']} 최종 실패 — 스킵")
                return []

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.select_one(TABLE_SELECTOR)
    if not table:
        return []

    results = []
    for tr in table.select(ROW_SELECTOR):
        cells = tr.find_all("td", recursive=False)
        if len(cells) < 3:
            continue

        # tm_player_id
        name_el = cells[1].select_one("td.hauptlink a")
        if not name_el:
            continue
        href = name_el.get("href", "")
        id_m = PLAYER_ID_PATTERN.search(href)
        if not id_m:
            continue
        tm_player_id = id_m.group(1)

        # 선수 표시명 (매칭 보조용)
        scraped_name = name_el.get_text(strip=True)

        # birth_date
        dob_raw = cells[2].get_text(strip=True)
        birth_date = parse_birth_date(dob_raw)
        if not birth_date:
            continue

        results.append({
            "tm_player_id":  tm_player_id,
            "birth_date":    birth_date,
            "scraped_name":  scraped_name,
            "team_name":     team["team_name"],
        })

    return results


# ─────────────────────────────────────────────
# 매칭 로직
# ─────────────────────────────────────────────

def _club_similarity(db_club: str, scraped_team: str) -> bool:
    """
    current_club(DB) vs team_name(Transfermarkt) 대략 일치 여부.
    완전 일치를 기대하기 어려우므로 단어 포함 여부로 판단.
    """
    if not db_club or not scraped_team:
        return False
    db_lower = db_club.lower().strip()
    sc_lower = scraped_team.lower().strip()
    # 한쪽이 다른쪽을 포함하면 일치로 간주
    return db_lower in sc_lower or sc_lower in db_lower


def match_players(
    db_df: pd.DataFrame,
    scraped: list[dict],
) -> tuple[list[dict], list[dict]]:
    """
    DB 레코드 ↔ 스크래핑 결과를 birth_date 기준으로 매칭한다.

    반환:
        confirmed  — [{"master_id": int, "tm_player_id": str, ...}]
        unmatched  — db_df 중 매칭 실패한 행 목록
    """
    # birth_date → DB 후보 매핑
    from collections import defaultdict
    db_by_bdate: dict[str, list] = defaultdict(list)
    for _, row in db_df.iterrows():
        if row["birth_date"]:
            db_by_bdate[row["birth_date"]].append(row)

    confirmed: list[dict] = []
    used_master_ids: set[int] = set()
    used_tm_ids: set[str] = set()

    for item in scraped:
        bdate = item["birth_date"]
        tm_id = item["tm_player_id"]

        if tm_id in used_tm_ids:
            continue

        candidates = [r for r in db_by_bdate.get(bdate, [])
                      if r["master_id"] not in used_master_ids]

        if not candidates:
            continue

        if len(candidates) == 1:
            # ✅ 1순위: birth_date 유일 매칭
            matched = candidates[0]
        else:
            # 2순위: current_club 유사도로 추가 필터
            club_matches = [
                r for r in candidates
                if _club_similarity(r["current_club"], item["team_name"])
            ]
            if len(club_matches) == 1:
                matched = club_matches[0]
            else:
                # 해소 불가 → 스킵 (unmatched로 남김)
                continue

        confirmed.append({
            "master_id":    matched["master_id"],
            "tm_player_id": tm_id,
            "name_original": matched["name_original"],
            "scraped_name":  item["scraped_name"],
            "birth_date":    bdate,
        })
        used_master_ids.add(matched["master_id"])
        used_tm_ids.add(tm_id)

    matched_ids = {c["master_id"] for c in confirmed}
    unmatched_rows = db_df[~db_df["master_id"].isin(matched_ids)].to_dict("records")

    return confirmed, unmatched_rows


# ─────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="player_master.tm_player_id 백필 스크립트"
    )
    parser.add_argument("--season", type=int, required=True,
                        help="스크래핑할 시즌 연도 (예: 2026)")
    parser.add_argument("--league", choices=["kl1", "kl2", "all"],
                        default="all",
                        help="대상 리그 (기본값: all)")
    args = parser.parse_args()

    target_leagues = list(LEAGUES.keys()) if args.league == "all" else [args.league]

    conn = sqlite3.connect(DB_PATH)
    add_tm_player_id_column(conn)

    # 백필 대상 로드
    db_df = load_unmatched_players(conn)
    print(f"\n[대상] tm_player_id IS NULL 선수: {len(db_df)}명")
    if db_df.empty:
        print("  → 백필 대상 없음. 종료.")
        conn.close()
        return

    sess = make_session()
    all_scraped: list[dict] = []

    for league_key in target_leagues:
        info = LEAGUES[league_key]
        print(f"\n[{info['label']}] 스크래핑 시작")

        teams = get_team_list(sess, args.season, info)
        for i, team in enumerate(teams, 1):
            print(f"  ({i}/{len(teams)}) {team['team_name']}", end=" ")
            rows = scrape_kader_for_ids(sess, team, args.season)
            all_scraped.extend(rows)
            print(f"→ {len(rows)}명")
            if i < len(teams):
                human_sleep(4, 8)

    print(f"\n[매칭] 수집된 선수: {len(all_scraped)}명 / DB 미매칭 대상: {len(db_df)}명")

    confirmed, unmatched = match_players(db_df, all_scraped)

    print(f"  → 자동 매칭 성공: {len(confirmed)}명")
    print(f"  → 매칭 실패 (수동 처리 필요): {len(unmatched)}명")

    # DB 업데이트
    if confirmed:
        updated = update_tm_player_id(conn, confirmed)
        print(f"  [DB] tm_player_id 업데이트 완료: {updated}건")

    # 매칭 실패 → CSV 저장
    if unmatched:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        out_path = OUTPUT_DIR / f"unmatched_tm_id_{args.season}.csv"
        pd.DataFrame(unmatched).to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"  [CSV] 미매칭 선수 저장 → {out_path}")
        print(f"        → 해당 파일에서 tm_player_id를 수동으로 확인 후 DB UPDATE 필요")

    # 최종 현황 출력
    total     = conn.execute("SELECT COUNT(*) FROM player_master").fetchone()[0]
    filled    = conn.execute("SELECT COUNT(*) FROM player_master WHERE tm_player_id IS NOT NULL").fetchone()[0]
    remaining = total - filled
    print(f"\n[최종 현황] player_master {total}명 중 tm_player_id 확보: {filled}명 / 미확보: {remaining}명")

    conn.close()
    print("\n완료.")


if __name__ == "__main__":
    main()
