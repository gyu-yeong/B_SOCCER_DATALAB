"""
Transfermarkt K리그 스쿼드 자동 스크래핑 스크립트
Source : transfermarkt.com → /kader/ (Detailed 뷰)
Target : data/raw/TM_squads_{season}_{KL1|KL2}.csv
         database/kleague1.db → player_master 테이블

Usage:
    python scripts/player_info/scrape_tm_squads.py --season 2025
    python scripts/player_info/scrape_tm_squads.py --season 2024 --league kl1
    python scripts/player_info/scrape_tm_squads.py --season 2025 --league kl2
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
DB_PATH      = PROJECT_ROOT / "database" / "kleague1.db"
OUTPUT_DIR   = PROJECT_ROOT / "data" / "raw"

# ─────────────────────────────────────────────
# Transfermarkt 설정
# ─────────────────────────────────────────────

BASE_URL = "https://www.transfermarkt.com"

LEAGUES = {
    "kl1": {"label": "K League 1", "tm_code": "RSK1", "slug": "k-league-1"},
    "kl2": {"label": "K League 2", "tm_code": "RSK2", "slug": "k-league-2"},
}

# HTML 파싱 셀렉터 (구조 변경 시 여기만 수정)
TABLE_SELECTOR       = "table.items"
ROW_SELECTOR         = "tr.odd, tr.even"
TEAM_LINK_PATTERN    = re.compile(r"^/(.+)/startseite/verein/(\d+)")

# ─────────────────────────────────────────────
# 파싱 유틸 (build_player_master.py 와 동일 로직)
# ─────────────────────────────────────────────

def parse_birth_date(val: str) -> str | None:
    """'11/12/2003 (22)' → '2003-12-11' (DD/MM/YYYY 형식)"""
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", str(val))
    if m:
        day, month, year = m.groups()
        return f"{year}-{month}-{day}"
    return None


def parse_joined(val: str) -> str | None:
    """'04/06/2025' → '2025-06-04' (DD/MM/YYYY 형식)"""
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


def parse_position(val: str) -> tuple[str | None, str | None]:
    """'Attack - Right Winger' → ('Attack', 'Right Winger')"""
    if not val or str(val).strip() == "":
        return None, None
    parts = str(val).strip().split(" - ", 1)
    pos    = parts[0].strip() or None
    detail = parts[1].strip() if len(parts) > 1 else None
    return pos, detail


# ─────────────────────────────────────────────
# HTTP 세션 (requests 기반 — 팝업 없음)
# ─────────────────────────────────────────────

def human_sleep(a: float = 4, b: float = 8) -> None:
    time.sleep(random.uniform(a, b))


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
    """실제 시즌 → Transfermarkt saison_id 변환.
    노트북 확인: REAL_SEASON=2025 → saison_id=2023 (season - 2)
    """
    return season - 2


# ─────────────────────────────────────────────
# STEP 1: 대회 페이지 → 팀 목록 수집
# ─────────────────────────────────────────────

def get_team_list(sess: requests.Session, season: int, league: dict) -> list[dict]:
    """
    대회 페이지에서 팀 slug + tm_id 목록을 추출한다.
    반환값: [{"slug": "...", "tm_id": "...", "team_name": "..."}]
    """
    saison_id = tm_saison_id(season)
    url = (
        f"{BASE_URL}/{league['slug']}/startseite/wettbewerb/"
        f"{league['tm_code']}/plus/?saison_id={saison_id}"
    )
    print(f"  [팀 목록] {league['label']} (saison_id={saison_id}) 접속 중...")
    resp = sess.get(url, timeout=30)
    soup = BeautifulSoup(resp.text, "html.parser")

    teams = []
    seen_ids = set()

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

    print(f"  → {len(teams)}개 팀 발견: {[t['team_name'] for t in teams]}")
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
    """
    팀의 /kader/ 페이지(Detailed 뷰)에서 선수 행을 파싱한다.
    /plus/1 → Detailed 뷰, ?saison_id= 쿼리 파라미터 방식(노트북 확인).
    실패 시 max_retries 횟수만큼 재시도.
    """
    saison_id = tm_saison_id(season)
    url = (
        f"{BASE_URL}/{team['slug']}/kader/verein/{team['tm_id']}"
        f"/plus/1?saison_id={saison_id}"
    )
    print(f"    스크래핑: {team['team_name']}")

    for attempt in range(1, max_retries + 2):
        try:
            resp = sess.get(url, timeout=30)
            soup = BeautifulSoup(resp.text, "html.parser")

            table = soup.select_one(TABLE_SELECTOR)
            if not table:
                raise ValueError("table.items 없음")

            results = [
                row for tr in table.select(ROW_SELECTOR)
                if (row := _parse_player_row(tr, team["team_name"], league_label, season))
            ]

            print(f"    → {len(results)}명 파싱 완료")
            human_sleep(1, 2)
            return results

        except Exception as e:
            if attempt <= max_retries:
                print(f"    ⚠ {team['team_name']} 시도 {attempt} 실패 ({e}) — 재시도 중...")
                human_sleep(3, 6)
            else:
                print(f"    ✗ {team['team_name']} 최종 실패 ({e}) — 스킵")
                return []

    return []


def _parse_player_row(tr, team_name: str, league_label: str, season: int) -> dict | None:
    """
    <tr> 1행 파싱 → dict 반환.
    선수명 + 포지션은 같은 셀에 2줄로 표시되므로 분리한다.

    컬럼 순서 (Detailed 뷰):
      0: 등번호(rn_nummer)
      1: 선수명+포지션(posrela)
      2: 생년월일/Age
      3: 국적 flag
      4: 신장
      5: 주발
      6: 합류일
      7: Signed from
      8: 계약 만료
      9: 시장가치
    """
    cells = tr.find_all("td", recursive=False)
    if len(cells) < 9:
        return None

    # 0. 등번호
    jersey = cells[0].get_text(strip=True)

    # 1. 선수명 + 포지션 (2줄 구조)
    # HTML: <td class="posrela"><table><tr><td class="hauptlink"><a href="...">Name</a></td></tr>
    name_el = cells[1].select_one("td.hauptlink a")
    if not name_el:
        return None
    name_orig = name_el.get_text(strip=True)

    # tm_player_id: 선수 링크 href에서 추출 (/player-name/profil/spieler/317720)
    player_href = name_el.get("href", "")
    tm_id_match = re.search(r"/spieler/(\d+)", player_href)
    tm_player_id = tm_id_match.group(1) if tm_id_match else None

    # 포지션: posrela 셀 내부 두 번째 <tr> 텍스트
    inner_rows = cells[1].select("tr")
    position_raw = ""
    if len(inner_rows) >= 2:
        position_raw = inner_rows[1].get_text(strip=True)
    else:
        # 구조가 다를 경우 fallback: hauptlink 다음 텍스트 노드
        pos_span = cells[1].find("td", class_=lambda c: c and "zentriert" in c)
        if pos_span:
            position_raw = pos_span.get_text(strip=True)

    position, position_detail = parse_position(position_raw)

    # 2. 생년월일
    dob_raw = cells[2].get_text(strip=True)

    # 3. 국적 (img title 우선, alt fallback)
    nat_img = cells[3].find("img")
    nat = ""
    if nat_img:
        nat = nat_img.get("title") or nat_img.get("alt") or ""

    # 4. 신장
    height_raw = cells[4].get_text(strip=True)

    # 5. 주발
    foot = cells[5].get_text(strip=True)

    # 6. 합류일
    joined_raw = cells[6].get_text(strip=True)

    # 7. Signed from
    signed_from_el = cells[7].find("a") or cells[7]
    signed_from = signed_from_el.get_text(strip=True)

    # 8. 계약 만료
    contract = cells[8].get_text(strip=True)

    # 9. 시장가치
    value_raw = cells[9].get_text(strip=True) if len(cells) > 9 else "-"

    return {
        "jersey_number":    jersey,
        "name_original":    name_orig,
        "tm_player_id":     tm_player_id,
        "position":         position,
        "position_detail":  position_detail,
        "birth_date":       parse_birth_date(dob_raw),
        "citizenship":      nat,
        "height_cm":        parse_height(height_raw),
        "foot":             foot,
        "joined":           parse_joined(joined_raw),
        "signed_from":      signed_from,
        "contract_until":   contract,
        "market_value_eur": parse_value(value_raw),
        "is_korean":        1 if "Korea" in nat else 0,
        "player_name":      None,  # 한국인 포함 전원 None → map_foreign_korean_names.py 로 채움
        "current_club":     team_name,
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
    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  [CSV] 저장 완료 → {out_path}  ({len(df)}행)")
    return out_path


# ─────────────────────────────────────────────
# DB 적재
# ─────────────────────────────────────────────

def alter_table_if_needed(conn: sqlite3.Connection) -> None:
    """신규 컬럼이 없으면 추가한다 (멱등 실행 가능)."""
    new_cols = [
        ("foot",           "TEXT"),
        ("signed_from",    "TEXT"),
        ("contract_until", "TEXT"),
        ("tm_player_id",   "TEXT"),
    ]
    cursor = conn.execute("PRAGMA table_info(player_master)")
    existing = {row[1] for row in cursor.fetchall()}

    for col_name, col_type in new_cols:
        if col_name not in existing:
            conn.execute(
                f"ALTER TABLE player_master ADD COLUMN {col_name} {col_type}"
            )
            print(f"  [DB] 컬럼 추가: player_master.{col_name}")

    conn.commit()


# player_name 은 UPDATE 제외: 기존에 입력된 한국어명을 덮어쓰지 않음
# tm_player_id 는 COALESCE: 기존값 우선, NULL일 때만 새값으로 채움
UPSERT_SQL = """
INSERT INTO player_master
    (player_name, name_original, birth_date, height_cm,
     citizenship, is_korean, position, position_detail,
     current_club, joined, market_value_eur,
     foot, signed_from, contract_until, tm_player_id)
VALUES
    (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(name_original, birth_date) DO UPDATE SET
    height_cm        = excluded.height_cm,
    citizenship      = excluded.citizenship,
    position         = excluded.position,
    position_detail  = excluded.position_detail,
    current_club     = excluded.current_club,
    joined           = excluded.joined,
    market_value_eur = excluded.market_value_eur,
    foot             = excluded.foot,
    signed_from      = excluded.signed_from,
    contract_until   = excluded.contract_until,
    tm_player_id     = COALESCE(player_master.tm_player_id, excluded.tm_player_id)
"""


def load_to_db(rows: list[dict]) -> None:
    conn = sqlite3.connect(DB_PATH)

    # 테이블이 없을 경우 DDL 실행
    conn.execute("""
        CREATE TABLE IF NOT EXISTS player_master (
            master_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name      TEXT,
            name_original    TEXT NOT NULL,
            birth_date       TEXT,
            height_cm        INTEGER,
            citizenship      TEXT,
            citizenship_2    TEXT,
            is_korean        INTEGER,
            position         TEXT,
            position_detail  TEXT,
            current_club     TEXT,
            joined           TEXT,
            market_value_eur INTEGER,
            foot             TEXT,
            signed_from      TEXT,
            contract_until   TEXT,
            UNIQUE (name_original, birth_date)
        )
    """)
    conn.commit()

    alter_table_if_needed(conn)

    inserted = updated = 0
    for row in rows:
        # birth_date가 None이면 UNIQUE 제약 동작 불안정 → 스킵
        if not row.get("name_original") or not row.get("birth_date"):
            continue
        try:
            conn.execute(
                UPSERT_SQL,
                (
                    row["player_name"],
                    row["name_original"],
                    row["birth_date"],
                    row["height_cm"],
                    row["citizenship"],
                    row["is_korean"],
                    row["position"],
                    row["position_detail"],
                    row["current_club"],
                    row["joined"],
                    row["market_value_eur"],
                    row["foot"],
                    row["signed_from"],
                    row["contract_until"],
                    row.get("tm_player_id"),
                ),
            )
            # rowcount 1 = INSERT, 0 = UPDATE(변경없음)도 포함해 추적
            inserted += 1
        except sqlite3.IntegrityError as e:
            print(f"  ⚠ 적재 오류 ({row.get('name_original')}): {e}")

    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM player_master").fetchone()[0]
    print(f"  [DB] 적재 완료: {inserted}건 처리 / player_master 총 {total}명")
    conn.close()


# ─────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Transfermarkt K리그 스쿼드 자동 스크래핑"
    )
    parser.add_argument(
        "--season",
        type=int,
        required=True,
        help="수집할 시즌 연도 (예: 2025)",
    )
    parser.add_argument(
        "--league",
        choices=["kl1", "kl2", "all"],
        default="all",
        help="대상 리그 (기본값: all)",
    )
    args = parser.parse_args()

    target_leagues = (
        list(LEAGUES.keys()) if args.league == "all" else [args.league]
    )

    print(f"=== Transfermarkt 스쿼드 스크래핑 시작 ===")
    print(f"시즌: {args.season} | 리그: {', '.join(target_leagues)}")

    sess = make_session()

    for league_key in target_leagues:
        info = LEAGUES[league_key]
        print(f"\n[{info['label']}] 처리 시작")

        # STEP 1: 팀 목록
        teams = get_team_list(sess, args.season, info)
        if not teams:
            print(f"  ⚠ 팀 목록 없음 — {info['label']} 스킵")
            continue

        # STEP 2: 팀별 스쿼드 수집
        all_rows: list[dict] = []
        for i, team in enumerate(teams, 1):
            print(f"  ({i}/{len(teams)})", end=" ")
            rows = scrape_squad(sess, team, args.season, info["label"])
            all_rows.extend(rows)
            if i < len(teams):
                human_sleep(4, 8)

        if not all_rows:
            print(f"  ⚠ 수집된 데이터 없음")
            continue

        print(f"\n  [합계] {info['label']} 총 {len(all_rows)}명")

        # CSV 저장
        save_csv(all_rows, args.season, league_key)

        # DB 적재
        load_to_db(all_rows)

    print("\n=== 완료 ===")


if __name__ == "__main__":
    main()
