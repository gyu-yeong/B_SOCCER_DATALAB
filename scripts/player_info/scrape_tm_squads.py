"""
Transfermarkt K리그 스쿼드 자동 스크래핑 스크립트
Source : transfermarkt.com → /kader/ (Detailed 뷰, /plus/1)
Target : data/raw/TM_squads_{season}_{KL1|KL2}.csv
         database/kleague.db → player_master 테이블

Usage:
    python scripts/player_info/scrape_tm_squads.py --season 2024
    python scripts/player_info/scrape_tm_squads.py --season 2025
    python scripts/player_info/scrape_tm_squads.py --season 2026 --league kl1

컬럼 확정 (2026-03-22):
    등번호, 선수명, tm_player_id, 포지션, 생년월일,
    국적1, 신장, 주발, 합류일, 이적출처, 계약만료, 시장가치
    * position_detail 수집 안 함
    * thead 기반 동적 컬럼 매핑 (고정 인덱스 사용 안 함)
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
OUTPUT_DIR   = PROJECT_ROOT / "data" / "raw" / "TM_squads"

# ─────────────────────────────────────────────
# Transfermarkt 설정
# ─────────────────────────────────────────────

BASE_URL = "https://www.transfermarkt.com"

LEAGUES = {
    "kl1": {"label": "K League 1", "tm_code": "RSK1", "slug": "k-league-1"},
    "kl2": {"label": "K League 2", "tm_code": "RSK2", "slug": "k-league-2"},
}

TABLE_SELECTOR    = "table.items"
ROW_SELECTOR      = "tr.odd, tr.even"
TEAM_LINK_PATTERN = re.compile(r"^/(.+)/startseite/verein/(\d+)")

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


def parse_joined(val: str) -> str | None:
    """'04/06/2025' → '2025-06-04'"""
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", str(val))
    if m:
        day, month, year = m.groups()
        return f"{year}-{month}-{day}"
    return None


def parse_height(val: str) -> int | None:
    """'1,73 m' → 173"""
    m = re.match(r"(\d+),(\d+)", str(val))
    if m:
        return int(m.group(1)) * 100 + int(m.group(2))
    return None


def parse_value(val: str) -> int | None:
    """'€150k' → 150000 / '€1.50m' → 1500000 / '-' → None"""
    val = str(val).strip()
    if val in ("-", ""):
        return None
    mk = re.match(r"€([\d.]+)k$", val)
    mm = re.match(r"€([\d.]+)m$", val)
    if mk:
        return int(float(mk.group(1)) * 1_000)
    if mm:
        return int(float(mm.group(1)) * 1_000_000)
    return None


def parse_position(val: str) -> str | None:
    """'Attack - Right Winger' → 'Attack'  (position_detail 수집 안 함)"""
    if not val or str(val).strip() == "":
        return None
    return str(val).strip().split(" - ")[0].strip() or None


# ─────────────────────────────────────────────
# HTTP 세션
# ─────────────────────────────────────────────

def human_sleep(a: float = 4, b: float = 8) -> None:
    time.sleep(random.uniform(a, b))


def make_session() -> requests.Session:
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
    """실제 시즌 → Transfermarkt saison_id
    TM은 K리그 캘린더 시즌을 (year-1)로 인덱싱: 2024시즌 → saison_id=2023
    """
    return season - 1


# ─────────────────────────────────────────────
# 헤더 기반 컬럼 매핑
# ─────────────────────────────────────────────

def get_col_map(table) -> dict[str, int]:
    """
    thead > th 텍스트 → 셀 인덱스 매핑을 반환한다.
    예: {'#': 0, 'Player': 1, 'Date of birth/Age': 2, 'Nat.': 3,
         'Height': 4, 'Foot': 5, 'Joined': 6, ...}

    시즌·팀마다 컬럼 순서가 다를 수 있으므로 인덱스 고정 사용 금지.
    """
    col_map = {}
    for i, th in enumerate(table.select("thead th")):
        text = th.get_text(strip=True)
        if text:
            col_map[text] = i
    return col_map


def _cell_text(cells: list, col_map: dict, key: str) -> str:
    """col_map에서 key의 인덱스를 찾아 해당 셀 텍스트를 반환."""
    idx = col_map.get(key)
    if idx is None or idx >= len(cells):
        return ""
    return cells[idx].get_text(strip=True)


# ─────────────────────────────────────────────
# STEP 1: 대회 페이지 → 팀 목록 수집
# ─────────────────────────────────────────────

def get_team_list(sess: requests.Session, season: int, league: dict) -> list[dict]:
    saison_id = tm_saison_id(season)
    url = (
        f"{BASE_URL}/{league['slug']}/startseite/wettbewerb/"
        f"{league['tm_code']}/plus/?saison_id={saison_id}"
    )
    print(f"  [팀 목록] {league['label']} (saison_id={saison_id}) 접속 중...")

    for attempt in range(1, 4):
        try:
            resp = sess.get(url, timeout=60)
            break
        except Exception as e:
            if attempt < 3:
                print(f"  ⚠ 팀목록 타임아웃 (시도 {attempt}) — 재시도 중...")
                human_sleep(5, 10)
            else:
                print(f"  ✗ 팀목록 최종 실패 ({e})")
                return []

    soup = BeautifulSoup(resp.text, "html.parser")
    teams, seen_ids = [], set()

    for a in soup.select("table.items td.hauptlink a"):
        href = a.get("href", "")
        m = TEAM_LINK_PATTERN.match(href)
        if not m:
            continue
        slug, tm_id = m.group(1), m.group(2)
        if tm_id in seen_ids:
            continue
        seen_ids.add(tm_id)
        team_name = a.get_text(strip=True)
        if team_name:
            teams.append({"slug": slug, "tm_id": tm_id, "team_name": team_name})

    print(f"  → {len(teams)}개 팀: {[t['team_name'] for t in teams]}")
    return teams


# ─────────────────────────────────────────────
# STEP 2: 팀 스쿼드 페이지 파싱
# ─────────────────────────────────────────────

def scrape_squad(
    sess: requests.Session,
    team: dict,
    season: int,
    league_label: str,
    max_retries: int = 2,
) -> list[dict]:
    saison_id = tm_saison_id(season)
    url = (
        f"{BASE_URL}/{team['slug']}/kader/verein/{team['tm_id']}"
        f"/plus/1?saison_id={saison_id}"
    )
    print(f"    스크래핑: {team['team_name']}")

    for attempt in range(1, max_retries + 2):
        try:
            resp = sess.get(url, timeout=60)
            soup = BeautifulSoup(resp.text, "html.parser")

            tables = soup.select(TABLE_SELECTOR)
            if not tables:
                raise ValueError("table.items 없음")

            results = []
            for table in tables:
                col_map = get_col_map(table)
                results += [
                    row for tr in table.select(ROW_SELECTOR)
                    if (row := _parse_player_row(tr, col_map, team["team_name"], league_label, season))
                ]

            print(f"    → {len(results)}명")
            human_sleep(1, 2)
            return results

        except Exception as e:
            if attempt <= max_retries:
                print(f"    ⚠ 시도 {attempt} 실패 ({e}) — 재시도 중...")
                human_sleep(3, 6)
            else:
                print(f"    ✗ 최종 실패 ({e}) — 스킵")
                return []

    return []


def _parse_player_row(
    tr,
    col_map: dict[str, int],
    team_name: str,
    league_label: str,
    season: int,
) -> dict | None:
    """
    thead 기반 col_map을 사용해 <tr> 1행을 파싱한다.
    인덱스 고정 방식 사용 안 함 → 컬럼 순서 변경에 안전.

    Player 셀(index=1)은 항상 고정 위치로 가정 (TM 구조상 불변).
    나머지 컬럼은 모두 col_map 조회.
    """
    cells = tr.find_all("td", recursive=False)
    if len(cells) < 3:
        return None

    # ── 선수명 + tm_player_id (Player 셀: col_map['Player'] 또는 index 1)
    player_idx = col_map.get("Player", 1)
    if player_idx >= len(cells):
        return None

    name_el = cells[player_idx].select_one("td.hauptlink a")
    if not name_el:
        return None

    name_orig    = name_el.get_text(strip=True)
    player_href  = name_el.get("href", "")
    tm_id_m      = re.search(r"/spieler/(\d+)", player_href)
    tm_player_id = tm_id_m.group(1) if tm_id_m else None

    # ── 포지션 (inner table 두 번째 <tr>) — position_detail 수집 안 함
    inner_rows   = cells[player_idx].select("tr")
    position_raw = inner_rows[1].get_text(strip=True) if len(inner_rows) >= 2 else ""
    position     = parse_position(position_raw)

    # ── 생년월일
    birth_date = parse_birth_date(_cell_text(cells, col_map, "Date of birth/Age"))

    # ── 국적 (img title 우선, alt fallback)
    nat_idx = col_map.get("Nat.")
    citizenship = ""
    if nat_idx is not None and nat_idx < len(cells):
        nat_img = cells[nat_idx].find("img")
        if nat_img:
            citizenship = nat_img.get("title") or nat_img.get("alt") or ""

    # ── 신장
    height_cm = parse_height(_cell_text(cells, col_map, "Height"))

    # ── 주발
    foot = _cell_text(cells, col_map, "Foot")

    # ── 합류일
    joined = parse_joined(_cell_text(cells, col_map, "Joined"))

    # ── 이적출처 (링크 텍스트 우선)
    signed_from = ""
    sf_idx = col_map.get("Signed from")
    if sf_idx is not None and sf_idx < len(cells):
        el = cells[sf_idx].find("a") or cells[sf_idx]
        signed_from = el.get_text(strip=True)

    # ── 계약만료
    contract_until = _cell_text(cells, col_map, "Contract")

    # ── 시장가치
    market_value_eur = parse_value(_cell_text(cells, col_map, "Market value"))

    return {
        "jersey_number":    _cell_text(cells, col_map, "#"),
        "name_original":    name_orig,
        "tm_player_id":     tm_player_id,
        "position":         position,
        "birth_date":       birth_date,
        "citizenship":      citizenship,
        "height_cm":        height_cm,
        "foot":             foot,
        "joined":           joined,
        "signed_from":      signed_from,
        "contract_until":   contract_until,
        "market_value_eur": market_value_eur,
        "is_korean":        1 if "Korea" in citizenship else 0,
        "player_name":      None,   # 한국어명: 후처리에서 채움
        "team_name":        team_name,
        "league":           league_label,
        "season":           season,
    }


# ─────────────────────────────────────────────
# CSV 저장
# ─────────────────────────────────────────────

def save_csv(rows: list[dict], season: int, league_key: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"TM_squads_{season}_{league_key.upper()}.csv"
    pd.DataFrame(rows).to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  [CSV] → {out_path}  ({len(rows)}행)")
    return out_path


# ─────────────────────────────────────────────
# DB 적재
# ─────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS player_master (
    master_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    player_name      TEXT,
    name_original    TEXT NOT NULL,
    tm_player_id     TEXT,
    birth_date       TEXT,
    height_cm        INTEGER,
    citizenship      TEXT,
    is_korean        INTEGER,
    position         TEXT,
    foot             TEXT,
    joined           TEXT,
    signed_from      TEXT,
    contract_until   TEXT,
    market_value_eur INTEGER,
    UNIQUE (name_original, birth_date)
)
"""

# player_name: UPDATE 제외 (기존 한국어명 보존)
# tm_player_id: COALESCE (기존값 우선)
UPSERT_SQL = """
INSERT INTO player_master
    (player_name, name_original, tm_player_id, birth_date, height_cm,
     citizenship, is_korean, position, foot,
     joined, signed_from, contract_until, market_value_eur)
VALUES
    (?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(name_original, birth_date) DO UPDATE SET
    height_cm        = excluded.height_cm,
    citizenship      = excluded.citizenship,
    is_korean        = excluded.is_korean,
    position         = excluded.position,
    foot             = excluded.foot,
    joined           = excluded.joined,
    signed_from      = excluded.signed_from,
    contract_until   = excluded.contract_until,
    market_value_eur = excluded.market_value_eur,
    tm_player_id     = COALESCE(player_master.tm_player_id, excluded.tm_player_id)
"""


def load_to_db(rows: list[dict]) -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()

    count = 0
    for row in rows:
        if not row.get("name_original") or not row.get("birth_date"):
            continue
        try:
            conn.execute(
                UPSERT_SQL,
                (
                    row["player_name"],
                    row["name_original"],
                    row.get("tm_player_id"),
                    row["birth_date"],
                    row["height_cm"],
                    row["citizenship"],
                    row["is_korean"],
                    row["position"],
                    row["foot"],
                    row["joined"],
                    row["signed_from"],
                    row["contract_until"],
                    row["market_value_eur"],
                ),
            )
            count += 1
        except sqlite3.IntegrityError as e:
            print(f"  ⚠ 적재 오류 ({row.get('name_original')}): {e}")

    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM player_master").fetchone()[0]
    print(f"  [DB] {count}건 처리 / player_master 총 {total}명")
    conn.close()


# ─────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Transfermarkt K리그 스쿼드 스크래핑")
    parser.add_argument("--season", type=int, required=True, help="시즌 연도 (예: 2025)")
    parser.add_argument("--league", choices=["kl1", "kl2", "all"], default="all")
    args = parser.parse_args()

    target_leagues = list(LEAGUES.keys()) if args.league == "all" else [args.league]

    print(f"=== Transfermarkt 스쿼드 스크래핑 ===")
    print(f"시즌: {args.season} | 리그: {', '.join(target_leagues)}")

    sess = make_session()

    for league_key in target_leagues:
        info = LEAGUES[league_key]
        print(f"\n[{info['label']}]")

        teams = get_team_list(sess, args.season, info)
        if not teams:
            print(f"  ⚠ 팀 목록 없음 — 스킵")
            continue

        all_rows: list[dict] = []
        for i, team in enumerate(teams, 1):
            print(f"  ({i}/{len(teams)})", end=" ")
            rows = scrape_squad(sess, team, args.season, info["label"])
            all_rows.extend(rows)
            if i < len(teams):
                human_sleep(4, 8)

        if not all_rows:
            print(f"  ⚠ 수집 결과 없음")
            continue

        print(f"\n  [합계] {len(all_rows)}명")
        save_csv(all_rows, args.season, league_key)
        # DB 적재는 build_db.py Phase 1a·2에서 처리 (이 스크립트에서는 CSV만 저장)

    print("\n=== 완료 ===")


if __name__ == "__main__":
    main()
