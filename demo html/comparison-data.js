/* comparison-data.js
 * v2.0 | 2026-06-07 | 레퍼런스: docs/player_comparison_spec.md
 * 실제 kleague.db 추출본(players.generated.json)을 로드해 선수 비교 시안에 공급.
 * 생성: python scripts/export_comparison_data.py
 *
 * 단위(unit) 처리:
 *   - count : total=합계 / per90=합계/분*90 / pct=백분위(per90 기준, 최소출전 필터)
 *   - rate  : total·per90 = 동일 비율값(%) / pct=비율 순위 (시도 부족 시 null '–')
 * FastAPI 전환 시: loadComparisonData()의 fetch 대상만 /api/... 로 교체하면 동일 인터페이스 유지.
 */

const PCOLORS = ["#2E5FC0", "#E8A33D", "#3FBF8F", "#A45CD6"];

let MEASURES = [], CATEGORIES = [], CAT_STATS = {}, MEASURES_BY_KEY = {};
let MIN_MIN = 450;
let ROSTER = {}, ROSTER_FLAT = {};
const _PIDX = {};   // `${id}|${season}|${league}` -> profile rec
const _POOL = {};   // `${season}|${league}` -> [profile rec, ...]

async function loadComparisonData(url = "players.generated.json") {
  const data = await fetch(url).then(r => r.json());
  MEASURES = data.measures;
  CATEGORIES = data.categories;
  MIN_MIN = data.min_minutes_for_pct || 450;
  MEASURES_BY_KEY = {};
  MEASURES.forEach(m => MEASURES_BY_KEY[m.key] = m);
  CAT_STATS = {};
  CATEGORIES.forEach(c => CAT_STATS[c] = MEASURES.filter(m => m.cat === c).map(m => m.key));

  data.players.forEach(p => {
    const rec = {
      season: String(p.season), league: p.league, team: p.team,
      name: p.name, pos: p.pos, id: p.id, birth: p.birth,
      games: p.games, minutes: p.minutes, stats: p.stats
    };
    _PIDX[rec.id + "|" + rec.season + "|" + rec.league] = rec;
    const lk = rec.season + "|" + rec.league;
    (_POOL[lk] = _POOL[lk] || []).push(rec);
    const s = rec.season, l = rec.league, t = rec.team;
    ROSTER[s] = ROSTER[s] || {}; ROSTER[s][l] = ROSTER[s][l] || {}; ROSTER[s][l][t] = ROSTER[s][l][t] || [];
    ROSTER[s][l][t].push({ name: rec.name, pos: rec.pos, id: rec.id });
    ROSTER_FLAT[`${rec.name} — ${rec.team} · ${rec.season} ${rec.league}`] =
      { season: s, league: l, team: t, name: rec.name, pos: rec.pos, id: rec.id };
  });
  // 팀 내 선수 이름순 정렬
  for (const s in ROSTER) for (const l in ROSTER[s]) for (const t in ROSTER[s][l])
    ROSTER[s][l][t].sort((a, b) => a.name.localeCompare(b.name, "ko"));
  return data;
}

/* ===== 조회 헬퍼 ===== */
function profile(p) { return _PIDX[p.id + "|" + p.season + "|" + p.league]; }
function poolOf(season, league) { return _POOL[season + "|" + league] || []; }
function isRate(key) { return MEASURES_BY_KEY[key].type === "rate"; }
function label(key) { return MEASURES_BY_KEY[key].label; }

/* 표시용 비율: den>0 이면 항상 계산(minDen 무시) — 합계·90분당 모드에서 사용 */
function _rateShow(pr, m) {
  const den = pr.stats[m.den];
  if (!den) return null;
  return pr.stats[m.num] / den * 100;
}
/* 순위용 비율: 최소 시도(minDen) 미달이면 null — 백분위에서만 사용 */
function _rate(pr, m) {
  const den = pr.stats[m.den];
  if (!den || den < m.minDen) return null;
  return pr.stats[m.num] / den * 100;
}
/* profile rec 기준 단위값 (pct 제외) */
function _val(pr, key, unit) {
  const m = MEASURES_BY_KEY[key];
  if (m.type === "rate") return _rateShow(pr, m);  // 표시: den>0이면 값
  if (unit === "per90") return pr.minutes ? pr.stats[key] / pr.minutes * 90 : 0;
  return pr.stats[key];
}
function _rank(mine, vals) {
  if (mine === null || !vals.length) return null;
  return Math.round(vals.filter(v => v <= mine).length / vals.length * 100);
}
function _percentile(pr, key) {
  const m = MEASURES_BY_KEY[key];
  const pool = poolOf(pr.season, pr.league);
  if (m.type === "rate") {
    const mine = _rate(pr, m); if (mine === null) return null;
    return _rank(mine, pool.map(q => _rate(q, m)).filter(v => v !== null));
  }
  const per90 = q => q.minutes ? q.stats[key] / q.minutes * 90 : 0;
  const vals = pool.filter(q => q.minutes >= MIN_MIN).map(per90);
  return _rank(per90(pr), vals);
}

/* ===== 공개 API (선택 레코드 p = {id,season,league,team,name,pos}) ===== */
function val(p, key, unit) {
  const pr = profile(p); if (!pr) return null;
  if (unit === "pct") return _percentile(pr, key);
  return _val(pr, key, unit);
}
function percentile(p, key) { const pr = profile(p); return pr ? _percentile(pr, key) : null; }

/* 값 표기 */
function fmtVal(key, v, unit) {
  if (v === null || v === undefined) return "–";
  if (unit === "pct") return Math.round(v);
  if (isRate(key)) return (Math.round(v * 10) / 10).toFixed(1) + "%";
  if (unit === "per90") return (Math.round(v * 100) / 100).toFixed(2);
  return Math.round(v);
}

/* swarm 용: 분포 축 값(count=per90 / rate=비율%) + 모집단 적격 여부 */
function swarmVal(pr, key) { return _val(pr, key, "per90"); }
function eligible(pr, key) {
  const m = MEASURES_BY_KEY[key];
  if (m.type === "rate") { const den = pr.stats[m.den]; return den >= m.minDen; }
  return pr.minutes >= MIN_MIN;
}

/* 기본 선택 선수(출전시간 상위 n) */
function topPlayers(season, league, n) {
  return poolOf(season, league).slice().sort((a, b) => b.minutes - a.minutes).slice(0, n)
    .map(r => ({ season: r.season, league: r.league, team: r.team, name: r.name, pos: r.pos, id: r.id }));
}
