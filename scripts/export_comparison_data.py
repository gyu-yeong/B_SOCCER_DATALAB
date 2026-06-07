#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
export_comparison_data.py
-------------------------
kleague.db -> 선수 비교 시안용 정적 JSON 추출.

- player_match_stats 를 (시즌, 리그, 선수) 단위로 집계(합계)
- 대회명 정규화: '하나은행 K리그1'/'K리그2 2025' 등 -> 'K리그1'/'K리그2', 슈퍼컵 제외
- count measure 원본 합계만 저장(파생 비율은 프런트에서 num/den 으로 계산)
- 출력: demo html/players.generated.json

추후 FastAPI 전환 시: 이 집계 로직을 그대로 endpoint 로 옮기면 됨(프런트 인터페이스 동일).

실행:  python scripts/export_comparison_data.py
"""
import sqlite3, json, datetime, os

DB = "database/kleague.db"
OUT = "demo html/players.generated.json"
MIN_MINUTES_FOR_PCT = 450  # 백분위 모집단 최소 출전시간(분)

# ---- 저장할 count measure (player_match_stats 원본 컬럼) ----
COUNT_MEASURES = [
    # 슈팅
    "goals", "shots", "shots_on_target", "shots_in_pa", "shots_out_pa", "blocked_shots", "missed_shots",
    # 패스
    "assists", "passes_attempted", "passes_successful", "key_passes",
    "crosses_attempted", "crosses_successful",
    "long_passes_attempted", "long_passes_successful",
    "medium_passes_attempted", "medium_passes_successful",
    "short_passes_attempted", "short_passes_successful",
    "forward_passes_attempted", "forward_passes_successful",
    "backward_passes_attempted", "backward_passes_successful",
    "lateral_passes_attempted", "lateral_passes_successful",
    "attacking_third_passes_attempted", "attacking_third_passes_successful",
    "middle_third_passes_attempted", "middle_third_passes_successful",
    "defensive_third_passes_attempted", "defensive_third_passes_successful",
    # 수비
    "tackles_attempted", "tackles_successful", "interceptions", "clearances", "blocks", "recoveries",
    "ground_duels_attempted", "ground_duels_won", "aerial_duels_attempted", "aerial_duels_won",
    # 드리블·세트피스
    "dribbles_attempted", "dribbles_successful", "freekicks", "corners", "throwins", "offsides",
    # 규율·기타
    "fouls_committed", "fouls_won", "yellow_cards", "red_cards", "ball_losses",
]

# ---- measure 메타(카테고리/라벨/타입). 순서 = 드롭다운/기본선택 순서 ----
def m(key, label, cat, type="count", num=None, den=None, minDen=0):
    return {"key": key, "label": label, "cat": cat, "type": type,
            **({"num": num, "den": den, "minDen": minDen} if type == "rate" else {})}

MEASURES = [
    # ===== 슈팅 =====
    m("goals", "득점", "슈팅"), m("shots", "슈팅", "슈팅"), m("shots_on_target", "유효슈팅", "슈팅"),
    m("shots_in_pa", "PA내 슈팅", "슈팅"), m("shots_out_pa", "PA외 슈팅", "슈팅"),
    m("blocked_shots", "차단된 슈팅", "슈팅"), m("missed_shots", "벗어난 슈팅", "슈팅"),
    m("sot_rate", "유효슈팅률", "슈팅", "rate", "shots_on_target", "shots", 20),
    m("conv_rate", "득점 전환율", "슈팅", "rate", "goals", "shots", 20),
    m("inpa_rate", "PA내 슈팅 비중", "슈팅", "rate", "shots_in_pa", "shots", 20),
    # ===== 패스 =====
    m("passes_attempted", "패스 시도", "패스"), m("passes_successful", "패스 성공", "패스"),
    m("key_passes", "키패스", "패스"), m("assists", "도움", "패스"),
    m("crosses_attempted", "크로스 시도", "패스"), m("crosses_successful", "크로스 성공", "패스"),
    m("long_passes_attempted", "롱패스 시도", "패스"), m("long_passes_successful", "롱패스 성공", "패스"),
    m("medium_passes_attempted", "중거리패스 시도", "패스"), m("medium_passes_successful", "중거리패스 성공", "패스"),
    m("short_passes_attempted", "숏패스 시도", "패스"), m("short_passes_successful", "숏패스 성공", "패스"),
    m("forward_passes_attempted", "전방패스 시도", "패스"), m("forward_passes_successful", "전방패스 성공", "패스"),
    m("backward_passes_attempted", "후방패스 시도", "패스"), m("backward_passes_successful", "후방패스 성공", "패스"),
    m("lateral_passes_attempted", "횡패스 시도", "패스"), m("lateral_passes_successful", "횡패스 성공", "패스"),
    m("attacking_third_passes_attempted", "공격지역 패스 시도", "패스"), m("attacking_third_passes_successful", "공격지역 패스 성공", "패스"),
    m("middle_third_passes_attempted", "중앙지역 패스 시도", "패스"), m("middle_third_passes_successful", "중앙지역 패스 성공", "패스"),
    m("defensive_third_passes_attempted", "수비지역 패스 시도", "패스"), m("defensive_third_passes_successful", "수비지역 패스 성공", "패스"),
    m("pass_rate", "패스 성공률", "패스", "rate", "passes_successful", "passes_attempted", 100),
    m("long_rate", "롱패스 성공률", "패스", "rate", "long_passes_successful", "long_passes_attempted", 15),
    m("cross_rate", "크로스 성공률", "패스", "rate", "crosses_successful", "crosses_attempted", 10),
    m("fwd_rate", "전방패스 성공률", "패스", "rate", "forward_passes_successful", "forward_passes_attempted", 50),
    # ===== 수비 =====
    m("tackles_attempted", "태클 시도", "수비"), m("tackles_successful", "태클 성공", "수비"),
    m("interceptions", "인터셉트", "수비"), m("clearances", "클리어링", "수비"),
    m("blocks", "차단", "수비"), m("recoveries", "볼 획득", "수비"),
    m("ground_duels_attempted", "지상경합 시도", "수비"), m("ground_duels_won", "지상경합 성공", "수비"),
    m("aerial_duels_attempted", "공중경합 시도", "수비"), m("aerial_duels_won", "공중경합 성공", "수비"),
    m("tackle_rate", "태클 성공률", "수비", "rate", "tackles_successful", "tackles_attempted", 10),
    m("ground_rate", "지상경합 승률", "수비", "rate", "ground_duels_won", "ground_duels_attempted", 15),
    m("aerial_rate", "공중경합 승률", "수비", "rate", "aerial_duels_won", "aerial_duels_attempted", 15),
    # ===== 드리블·세트피스 =====
    m("dribbles_attempted", "드리블 시도", "드리블·세트피스"), m("dribbles_successful", "드리블 성공", "드리블·세트피스"),
    m("freekicks", "프리킥", "드리블·세트피스"), m("corners", "코너킥", "드리블·세트피스"),
    m("throwins", "스로인", "드리블·세트피스"), m("offsides", "오프사이드", "드리블·세트피스"),
    m("dribble_rate", "드리블 성공률", "드리블·세트피스", "rate", "dribbles_successful", "dribbles_attempted", 10),
    # ===== 규율·기타 =====
    m("fouls_committed", "파울", "규율·기타"), m("fouls_won", "피파울", "규율·기타"),
    m("yellow_cards", "경고", "규율·기타"), m("red_cards", "퇴장", "규율·기타"),
    m("ball_losses", "볼 미스", "규율·기타"),
]
CATEGORIES = ["슈팅", "패스", "수비", "드리블·세트피스", "규율·기타"]


def norm_league(name: str):
    if not name:
        return None
    if "K리그1" in name:
        return "K리그1"
    if "K리그2" in name:
        return "K리그2"
    return None  # 슈퍼컵 등 제외


def map_pos(p: str):
    if not p:
        return "-"
    if "Goalkeeper" in p:
        return "GK"
    if "Back" in p or p == "Defender":
        return "DF"
    if "Midfield" in p or p == "Midfielder":
        return "MF"
    if "Forward" in p or "Winger" in p or "Striker" in p:
        return "FW"
    return "-"


def main():
    con = sqlite3.connect(DB)
    cur = con.cursor()
    raw_cols = ",".join(f"COALESCE(pms.{c},0) AS {c}" for c in COUNT_MEASURES)
    # 경기별 단일 행으로 추출(집계는 Python). (master,match) 중복은 dedup 처리.
    q = f"""
        SELECT c.year, c.competition_name, pms.master_id, pms.match_id, t.team_name,
               pm.name_kor, pm.position, pm.birth_date,
               COALESCE(pms.minutes_played,0) AS minutes,
               {raw_cols}
        FROM player_match_stats pms
        JOIN matches m       ON pms.match_id = m.match_id
        JOIN competitions c  ON m.competition_id = c.competition_id
        JOIN player_master pm ON pms.master_id = pm.master_id
        LEFT JOIN teams t    ON pms.team_id = t.team_id
        WHERE pms.master_id IS NOT NULL
    """
    rows = cur.execute(q).fetchall()
    colnames = [d[0] for d in cur.description]
    idx = {name: i for i, name in enumerate(colnames)}

    # 1) (year, league, master, match) 단위 dedup — 최다 출전 행 1개만 채택
    seen = {}  # (year,league,master,match) -> row (max minutes)
    for r in rows:
        league = norm_league(r[idx["competition_name"]])
        if not league:
            continue
        k = (r[idx["year"]], league, r[idx["master_id"]], r[idx["match_id"]])
        prev = seen.get(k)
        if prev is None or r[idx["minutes"]] > prev[idx["minutes"]]:
            seen[k] = r

    # 2) (year, league, master) 단위 집계
    agg = {}
    for (year, league, mid, _match), r in seen.items():
        key = (year, league, mid)
        rec = agg.get(key)
        if rec is None:
            rec = {
                "season": year, "league": league, "id": mid,
                "name": r[idx["name_kor"]], "pos": map_pos(r[idx["position"]]),
                "birth": r[idx["birth_date"]],
                "minutes": 0, "games": 0,
                "stats": {c: 0 for c in COUNT_MEASURES},
                "_team_minutes": {},
            }
            agg[key] = rec
        mins = r[idx["minutes"]]
        rec["minutes"] += mins
        rec["games"] += 1
        for c in COUNT_MEASURES:
            rec["stats"][c] += r[idx[c]]
        tname = r[idx["team_name"]] or "미상"
        rec["_team_minutes"][tname] = rec["_team_minutes"].get(tname, 0) + mins

    # 리그 라운드 상한(+승강PO 여유 2). 초과 시 master_id 충돌(동명이인 병합)로 간주해 드롭.
    ROUND_CAP = {"K리그1": 38, "K리그2": 41}
    players, dropped, borderline = [], [], []
    for rec in agg.values():
        team = max(rec["_team_minutes"].items(), key=lambda kv: kv[1])[0] if rec["_team_minutes"] else "미상"
        del rec["_team_minutes"]
        rec["team"] = team
        cap = ROUND_CAP.get(rec["league"], 41)
        tag = (rec["season"], rec["league"], rec["name"], rec["id"], rec["games"], rec["minutes"])
        if rec["games"] > cap + 2:          # 명백한 충돌 → 드롭
            dropped.append(tag)
            continue
        if rec["games"] > cap:              # 경계(승강PO 가능) → 보존하되 검토 목록
            borderline.append(tag)
        players.append(rec)

    players.sort(key=lambda p: (-p["minutes"],))

    out = {
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "source": DB,
        "min_minutes_for_pct": MIN_MINUTES_FOR_PCT,
        "categories": CATEGORIES,
        "measures": MEASURES,
        "players": players,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))

    # 요약 출력
    from collections import Counter
    combo = Counter((p["season"], p["league"]) for p in players)
    print(f"[OK] {OUT}")
    print(f"  players: {len(players)}  measures: {len(MEASURES)}  categories: {len(CATEGORIES)}")
    for (yr, lg), n in sorted(combo.items()):
        print(f"  {yr} {lg}: {n}명")
    if dropped:
        print(f"  [DROP] master_id 충돌 추정(출전>상한+2) {len(dropped)}건 제외:")
        for s in sorted(dropped, key=lambda x: -x[4]):
            print(f"    - {s[0]} {s[1]} {s[2]}(id={s[3]}) games={s[4]} min={s[5]}")
    if borderline:
        print(f"  [KEEP/검토] 경계(상한<출전<=상한+2, 승강PO 가능) {len(borderline)}건 보존:")
        for s in sorted(borderline, key=lambda x: -x[4]):
            print(f"    - {s[0]} {s[1]} {s[2]}(id={s[3]}) games={s[4]}")
    con.close()


if __name__ == "__main__":
    main()
