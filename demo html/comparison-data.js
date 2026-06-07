/* comparison-data.js
 * v1.0 | 2026-06-03 | 레퍼런스: docs/player_comparison_spec.md
 * 선수 비교 시안(1안 히트맵 / 2안 바차트) 공용 데이터·로직.
 * 샘플 로스터 + 결정적(seeded) 스탯 생성 + 백분위(시즌·리그 전체 선수 기준) 계산.
 * 실제 DB 연동 시 ROSTER/profile()만 API 호출로 교체하면 됨.
 */

/* ===== 스탯 카테고리 (1레벨 → 2레벨) ===== */
const CAT_STATS = {
  "슈팅": ["슈팅", "유효슈팅", "득점", "PA내 슈팅", "PA외 슈팅"],
  "패스": ["패스 시도", "패스 성공", "키패스", "롱패스 시도", "크로스"],
  "수비": ["태클", "인터셉트", "클리어링", "차단", "공중경합 승리"]
};

/* 스탯별 총합 범위(POC용) */
const STAT_RANGE = {
  "슈팅": [20, 80], "유효슈팅": [8, 40], "득점": [2, 18], "PA내 슈팅": [15, 65], "PA외 슈팅": [5, 25],
  "패스 시도": [600, 2000], "패스 성공": [480, 1800], "키패스": [8, 70], "롱패스 시도": [30, 180], "크로스": [8, 120],
  "태클": [10, 90], "인터셉트": [5, 60], "클리어링": [10, 150], "차단": [5, 50], "공중경합 승리": [10, 120]
};

/* 포지션별 카테고리 가중치 */
const POS_BIAS = {
  FW: { "슈팅": 1.4, "패스": 0.75, "수비": 0.45 },
  MF: { "슈팅": 0.9, "패스": 1.4, "수비": 1.0 },
  DF: { "슈팅": 0.4, "패스": 1.05, "수비": 1.55 }
};

/* stat → category 역매핑 */
const STAT_CAT = {};
for (const c in CAT_STATS) CAT_STATS[c].forEach(s => STAT_CAT[s] = c);

/* ===== 로스터: 시즌 → 리그 → 팀 → [선수, 포지션] ===== */
const ROSTER = {
  "2025": {
    "K리그1": {
      "울산 HD": [["주민규", "FW"], ["엄원상", "FW"], ["박용우", "MF"], ["김영권", "DF"]],
      "전북 현대": [["전진우", "FW"], ["이승우", "FW"], ["백승호", "MF"], ["홍정호", "DF"]],
      "포항 스틸러스": [["이호재", "FW"], ["김인성", "FW"], ["오베르단", "MF"], ["박승욱", "DF"]],
      "강원 FC": [["양민혁", "FW"], ["이상헌", "MF"], ["김영빈", "DF"], ["황문기", "MF"]]
    },
    "K리그2": {
      "인천 유나이티드": [["무고사", "FW"], ["제르소", "FW"], ["김도혁", "MF"]],
      "부산 아이파크": [["페신", "FW"], ["라마스", "MF"], ["최건주", "FW"]],
      "수원 삼성": [["안병준", "FW"], ["김주찬", "FW"], ["고무열", "MF"]],
      "충남 아산": [["강민규", "FW"], ["두아르테", "MF"], ["박세직", "MF"]]
    }
  },
  "2024": {
    "K리그1": {
      "울산 HD": [["주민규", "FW"], ["엄원상", "FW"], ["박용우", "MF"], ["김영권", "DF"]],
      "전북 현대": [["전진우", "FW"], ["이승우", "FW"], ["백승호", "MF"], ["홍정호", "DF"]],
      "포항 스틸러스": [["이호재", "FW"], ["김인성", "FW"], ["오베르단", "MF"], ["박승욱", "DF"]],
      "강원 FC": [["양민혁", "FW"], ["이상헌", "MF"], ["김영빈", "DF"], ["황문기", "MF"]]
    },
    "K리그2": {
      "인천 유나이티드": [["무고사", "FW"], ["제르소", "FW"], ["김도혁", "MF"]],
      "부산 아이파크": [["페신", "FW"], ["라마스", "MF"], ["최건주", "FW"]],
      "수원 삼성": [["안병준", "FW"], ["김주찬", "FW"], ["고무열", "MF"]],
      "충남 아산": [["강민규", "FW"], ["두아르테", "MF"], ["박세직", "MF"]]
    }
  }
};

/* ===== 결정적 난수 (FNV-1a 해시 → 0~1) ===== */
function _rnd(str) {
  let h = 2166136261;
  for (let i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = Math.imul(h, 16777619); }
  return ((h >>> 0) % 100000) / 100000;
}

/* ===== 선수 스탯 프로파일 (캐시) ===== */
const _cache = {};
function profile(p) {
  const key = p.season + "|" + p.league + "|" + p.team + "|" + p.name;
  if (_cache[key]) return _cache[key];
  const minutes = Math.round(1000 + _rnd(key + "min") * 2000);
  const games = Math.round(12 + _rnd(key + "g") * 26);
  const stats = {};
  for (const s in STAT_RANGE) {
    const [mn, mx] = STAT_RANGE[s];
    const bias = POS_BIAS[p.pos][STAT_CAT[s]];
    stats[s] = Math.max(0, Math.round((mn + _rnd(key + s) * (mx - mn)) * bias));
  }
  return (_cache[key] = { games, minutes, stats });
}

/* 특정 시즌·리그의 모든 선수 (백분위 모집단) */
function allPlayers(season, league) {
  const out = [];
  const T = ROSTER[season][league];
  for (const team in T) for (const pl of T[team]) out.push({ season, league, team, name: pl[0], pos: pl[1] });
  return out;
}

/* 백분위: 선택 시즌·리그 전체 선수 기준 (≤ 비율) */
function percentile(p, stat) {
  const pool = allPlayers(p.season, p.league).map(q => profile(q).stats[stat]);
  const v = profile(p).stats[stat];
  const below = pool.filter(x => x <= v).length;
  return Math.round(below / pool.length * 100);
}

/* 단위별 값: total / per90 / pct */
function val(p, stat, unit) {
  const pr = profile(p);
  if (unit === "total") return pr.stats[stat];
  if (unit === "pct") return percentile(p, stat);
  return pr.stats[stat] / pr.minutes * 90; // per90
}

/* ===== 빠른 검색용 평탄화 로스터 (라벨 → 선수 레코드) ===== */
const ROSTER_FLAT = {};
for (const s in ROSTER)
  for (const l in ROSTER[s])
    for (const t in ROSTER[s][l])
      for (const pl of ROSTER[s][l][t])
        ROSTER_FLAT[`${pl[0]} — ${t} · ${s} ${l}`] = { season: s, league: l, team: t, name: pl[0], pos: pl[1] };

/* 선택 선수별 구분 색상 (최대 3명) */
const PCOLORS = ["#2E5FC0", "#E8A33D", "#3FBF8F"];
