"""
외국인 선수 한국명 자동 매핑 스크립트
- 선수인적정보.xlsx (K리그 데이터포털, 2026시즌) 기준
- 매핑 전략: 생년월일 + 소속클럽 (1순위) → 생년월일 단독 고유매칭 (2순위)
- 결과: 외국인선수_한국명매핑.csv (korean_name 컬럼 채워진 버전)
"""

import sqlite3
import pandas as pd
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

XLSX_PATH = Path("data/raw/2026_KLEAGUE/선수인적정보.xlsx")
DB_PATH   = Path("database/kleague1.db")
OUT_PATH  = Path("C:/Users/koaro/Downloads/외국인선수_한국명매핑.csv")

# 영문 클럽명(Transfermarkt) → 한국 클럽명(K리그 데이터포털)
CLUB_MAP = {
    "Ansan Greeners":          "안산 그리너스 축구단",
    "Bucheon FC 1995":         "부천 FC 1995",
    "Busan IPark":             "부산 아이파크",
    "Cheonan City":            "천안시티FC",
    "Chungbuk Cheongju FC":    "충북 청주FC",
    "Chungnam Asan":           "충남아산 프로축구단",
    "Daegu FC":                "대구FC",
    "Daejeon Hana Citizen":    "대전 하나 시티즌",
    "FC Anyang":               "FC안양",
    "FC Seoul":                "FC서울",
    "Gangwon FC":              "강원FC",
    "Gimhae FC 2008":          "김해FC2008",
    "Gimpo FC":                "김포FC",
    "Gwangju FC":              "광주FC",
    "Gyeongnam FC":            "경남FC",
    "Hwaseong FC":             "화성FC",
    "Incheon United":          "인천 유나이티드",
    "Jeju SK":                 "제주SK FC",
    "Jeonbuk Hyundai Motors":  "전북 현대 모터스",
    "Jeonnam Dragons":         "전남 드래곤즈",
    "Paju Frontier":           "파주프런티어FC",
    "Pohang Steelers":         "포항 스틸러스",
    "Seongnam FC":             "성남FC",
    "Seoul E-Land":            "서울 이랜드 FC",
    "Suwon FC":                "수원FC",
    "Suwon Samsung Bluewings": "수원 삼성 블루윙즈",
    "Ulsan HD FC":             "울산 HD FC",
    "Yongin FC":               "용인FC",
}


def fuzzy_date_match(birth_date_str: str, club_kr: str, xlsx: pd.DataFrame, days: int = 30):
    """생년월일 ±days일 + 같은 클럽으로 퍼지 매칭. 유일 결과만 반환."""
    if not birth_date_str or not club_kr:
        return None
    try:
        target = pd.to_datetime(birth_date_str)
    except Exception:
        return None
    lower = (target - pd.Timedelta(days=days)).strftime("%Y-%m-%d")
    upper = (target + pd.Timedelta(days=days)).strftime("%Y-%m-%d")
    candidates = xlsx[
        (xlsx["birth_date"] >= lower) &
        (xlsx["birth_date"] <= upper) &
        (xlsx["club_kr"] == club_kr)
    ]
    if len(candidates) == 1:
        return candidates.iloc[0]
    return None


def main():
    # ── 데이터 로드 ───────────────────────────────────────────
    print("[1/5] 데이터 로드")
    xlsx = pd.read_excel(XLSX_PATH)
    xlsx["birth_date"] = pd.to_datetime(xlsx["생년월일"]).dt.strftime("%Y-%m-%d")
    xlsx = xlsx.rename(columns={"소속 클럽": "club_kr", "성명": "korean_name", "등번": "back_number"})
    print(f"      선수인적정보: {len(xlsx)}명")

    conn = sqlite3.connect(DB_PATH)
    master = pd.read_sql(
        "SELECT master_id, name_original, birth_date, citizenship, "
        "position, position_detail, current_club FROM player_master WHERE is_korean=0",
        conn,
    )
    # DB players 테이블에서 한국 음차명 fallback용으로 로드
    db_players = pd.read_sql(
        "SELECT DISTINCT player_name, team_name FROM players", conn
    )
    conn.close()
    print(f"      외국인 player_master: {len(master)}명")

    # ── 클럽명 한글 변환 ──────────────────────────────────────
    print("[2/5] 클럽명 매핑")
    master["club_kr"] = master["current_club"].map(CLUB_MAP)
    unmapped_clubs = master[master["club_kr"].isna()]["current_club"].unique()
    if len(unmapped_clubs):
        print(f"      ⚠ 클럽 매핑 누락: {list(unmapped_clubs)}")

    # ── 1순위: 생년월일 + 클럽 정확 매핑 ────────────────────
    print("[3/5] 매핑 시도")
    merged = master.merge(
        xlsx[["korean_name", "birth_date", "club_kr", "back_number"]],
        on=["birth_date", "club_kr"],
        how="left",
    )
    hit1  = merged[merged["korean_name"].notna()].copy()
    miss1 = merged[merged["korean_name"].isna()].copy()
    print(f"      [1순위] 생년월일+클럽 정확: {len(hit1)}명 매핑 / {len(miss1)}명 잔여")

    # ── 2순위: 생년월일 단독 고유 매핑 ──────────────────────
    bd_counts = xlsx["birth_date"].value_counts()
    unique_bd = set(bd_counts[bd_counts == 1].index)

    tier2_rows = []
    for _, row in miss1.iterrows():
        if row["birth_date"] in unique_bd:
            m = xlsx[xlsx["birth_date"] == row["birth_date"]].iloc[0]
            r = row.copy()
            r["korean_name"] = m["korean_name"]
            r["back_number"] = m["back_number"]
            tier2_rows.append(r)
        else:
            tier2_rows.append(row)

    tier2_df = pd.DataFrame(tier2_rows) if tier2_rows else pd.DataFrame(columns=miss1.columns)
    hit2  = tier2_df[tier2_df["korean_name"].notna()]
    miss2 = tier2_df[tier2_df["korean_name"].isna()]
    print(f"      [2순위] 생년월일 단독 고유: +{len(hit2)}명 / {len(miss2)}명 잔여")

    # ── 3순위: 퍼지 날짜 + 클럽 매핑 (±30일) ─────────────────
    tier3_rows = []
    for _, row in miss2.iterrows():
        m = fuzzy_date_match(row["birth_date"], row["club_kr"], xlsx, days=30)
        if m is not None:
            r = row.copy()
            r["korean_name"] = m["korean_name"]
            r["back_number"] = m["back_number"]
            r["_fuzzy"]      = True
            tier3_rows.append(r)
        else:
            tier3_rows.append(row)

    tier3_df = pd.DataFrame(tier3_rows) if tier3_rows else pd.DataFrame(columns=miss2.columns)
    hit3  = tier3_df[tier3_df["korean_name"].notna()]
    miss3 = tier3_df[tier3_df["korean_name"].isna()]
    if len(hit3):
        print(f"      [3순위] 퍼지 날짜±30일+클럽: +{len(hit3)}명")
        for _, r in hit3.iterrows():
            print(f"             {r['name_original']} → {r['korean_name']} (birth: {r['birth_date']})")
    print(f"      {len(miss3)}명 미매핑 → DB players 테이블 fallback 시도")

    # ── 4순위: DB players 테이블 한국 음차명 fallback (K리그1 한정) ──
    # K리그1 팀 영문→한국 약칭 매핑 (players 테이블 team_name 형식)
    K1_CLUB_MAP = {
        "FC Seoul":               "서울",
        "Jeonbuk Hyundai Motors": "전북",
        "Pohang Steelers":        "포항",
        "Ulsan HD FC":            "울산",
        "Daegu FC":               "대구",
        "Gwangju FC":             "광주",
        "Daejeon Hana Citizen":   "대전",
        "Suwon FC":               "수원FC",
        "Gangwon FC":             "강원",
        "Jeju SK":                "제주",
        "Incheon United":         "인천",
    }

    # players 테이블에서 각 K1 팀 선수 이름 목록 구성
    k1_players_by_team = {}
    for club_en, team_kr in K1_CLUB_MAP.items():
        names = db_players[db_players["team_name"] == team_kr]["player_name"].tolist()
        k1_players_by_team[club_en] = names

    # 이미 매핑된 한국명 집합
    already_mapped = set(
        pd.concat([hit1, hit2, hit3])["korean_name"].dropna()
    )

    tier4_rows = []
    for _, row in miss3.iterrows():
        club_en = row["current_club"]
        if club_en in K1_CLUB_MAP:
            team_players = k1_players_by_team[club_en]
            # 해당 팀에 있는 이름 중 아직 매핑 안 된 것 = 외국인 음차 후보
            candidates = [n for n in team_players if n not in already_mapped]
            if len(candidates) == 1:
                r = row.copy()
                r["korean_name"] = candidates[0]
                r["_db_fallback"] = True
                tier4_rows.append(r)
                already_mapped.add(candidates[0])
                print(f"      [4순위] DB fallback: {row['name_original']} → {candidates[0]}")
                continue
        # 5순위: same year + same day-of-month + same club (월만 다른 경우, 가르시아 등)
        if row["birth_date"] and row["club_kr"]:
            year = row["birth_date"][:4]
            day  = row["birth_date"][8:10]
            same_day = xlsx[
                (xlsx["birth_date"].str[:4] == year) &
                (xlsx["birth_date"].str[8:10] == day) &
                (xlsx["club_kr"] == row["club_kr"])
            ]
            if len(same_day) == 1:
                m = same_day.iloc[0]
                r = row.copy()
                r["korean_name"] = m["korean_name"]
                r["back_number"] = m["back_number"]
                r["_same_day"]   = True
                tier4_rows.append(r)
                print(f"      [5순위] 같은연도·일+클럽: {row['name_original']} → {m['korean_name']} "
                      f"(TM: {row['birth_date']} / KLP: {m['birth_date']})")
                continue
        tier4_rows.append(row)

    # ── 6순위: 알려진 예외 처리 (소스 간 생년월일 불일치 + DB에 음차명 확인된 선수) ──
    # master_id: 한국명 (근거: DB players 테이블 확인)
    KNOWN_EXCEPTIONS = {
        888: "이탈로",      # Italo (Brazil, Jeju SK) - TM:1997-08-03 / KLP:1997-08-23 (20일 차이, ±30일 내 3명으로 퍼지 실패)
        205: "팔로세비치",  # Александар Палочевић (Serbia, FC Seoul) - 2026 K리그 포털 미등록
    }
    final_rows = []
    for _, row in pd.DataFrame(tier4_rows).iterrows() if tier4_rows else []:
        mid = int(row["master_id"])
        if row["korean_name"] is None or pd.isna(row["korean_name"]):
            if mid in KNOWN_EXCEPTIONS:
                r = row.copy()
                r["korean_name"] = KNOWN_EXCEPTIONS[mid]
                final_rows.append(r)
                print(f"      [6순위] 알려진예외: {row['name_original']} → {KNOWN_EXCEPTIONS[mid]}")
                continue
        final_rows.append(row)

    tier4_df = pd.DataFrame(final_rows) if final_rows else (
        pd.DataFrame(tier4_rows) if tier4_rows else pd.DataFrame(columns=miss3.columns)
    )
    miss_final = tier4_df[tier4_df["korean_name"].isna()]

    # ── 결과 통합 ─────────────────────────────────────────────
    print(f"\n[4/5] 결과 통합")
    result = pd.concat([hit1, hit2, tier3_df, tier4_df], ignore_index=True)
    result = result.sort_values("korean_name", na_position="last")
    result = result.drop_duplicates(subset="master_id", keep="first")

    total_matched = result["korean_name"].notna().sum()
    total_miss    = result["korean_name"].isna().sum()
    print(f"      전체 {len(result)}명 | 매핑 완료: {total_matched}명 | 수동 필요: {total_miss}명")

    # ── CSV 출력 ──────────────────────────────────────────────
    print(f"\n[5/5] CSV 출력")
    keep_cols = ["master_id", "name_original", "birth_date",
                 "korean_name", "citizenship", "position", "position_detail", "current_club"]
    out = result[[c for c in keep_cols if c in result.columns]].sort_values(
        ["current_club", "name_original"]
    )
    out.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
    print(f"      저장됨 → {OUT_PATH}")

    if total_miss > 0:
        print(f"\n=== 수동 입력 필요 ({total_miss}명) ===")
        for _, r in out[out["korean_name"].isna()].iterrows():
            print(f"  [{int(r['master_id'])}] {r['name_original']} ({r['citizenship']}) - {r['current_club']}  birth: {r['birth_date']}")

    print("\n완료.")


if __name__ == "__main__":
    main()
