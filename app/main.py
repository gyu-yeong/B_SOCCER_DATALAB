"""K리그 선수 비교 매트릭스"""
import json, os, sys
import streamlit as st
import streamlit.components.v1 as components

sys.path.insert(0, os.path.dirname(__file__))
from db import get_seasons, get_teams, get_players, get_player_season_stats

# ── 데이터 헬퍼 ──────────────────────────────────────────────────────
def aggregate(df):
    return {c: float(df[c].sum()) for c in df.select_dtypes("number").columns}

def pct(a, b): return round(a / b * 100, 1) if b else 0.0
def p90v(v, m): return round(v / max(m, 1) * 90, 2)

def srow(key, stat, unit, col, ss, rmax):
    vals, p90s = [], []
    for s in ss:
        v = s.get(col) or 0
        vals.append(round(v, 1) if isinstance(v, float) else int(v))
        p90s.append(p90v(v, s.get('minutes_played') or 1))
    return dict(key=key, stat=stat, unit=unit, vals=vals, vals_p90=p90s, radarMax=rmax)

def prow(key, stat, nc, dc, ss, rmax):
    vs = [pct(s.get(nc) or 0, s.get(dc) or 0) for s in ss]
    return dict(key=key, stat=stat, unit='%', vals=vs, vals_p90=vs, radarMax=rmax)

def build_data(ss):
    return {
        'attack': {'label': '공격', 'rows': [
            srow('goal',   '골',           '', 'goals',               ss, 25),
            srow('assist', '어시스트',      '', 'assists',             ss, 15),
            srow('shot',   '슈팅',          '', 'shots',               ss, 110),
            srow('shotOT', '유효슈팅',      '', 'shots_on_target',     ss, 55),
            srow('drib',   '드리블 시도',   '', 'dribbles_attempted',  ss, 110),
            srow('dribS',  '드리블 성공',   '', 'dribbles_successful', ss, 70),
            prow('dribR',  '드리블 성공률', 'dribbles_successful', 'dribbles_attempted', ss, 100),
            srow('offsid', '오프사이드',    '', 'offsides',            ss, 30),
        ]},
        'pass': {'label': '패스', 'rows': [
            srow('pass',   '패스 시도',   '', 'passes_attempted',          ss, 900),
            srow('passS',  '패스 성공',   '', 'passes_successful',         ss, 780),
            prow('passR',  '패스 성공률', 'passes_successful', 'passes_attempted', ss, 100),
            srow('keyP',   '키패스',      '', 'key_passes',                ss, 70),
            srow('fwdP',   '전방 패스',   '', 'forward_passes_successful', ss, 360),
            srow('cross',  '크로스 시도', '', 'crosses_attempted',         ss, 100),
            srow('crossS', '크로스 성공', '', 'crosses_successful',        ss, 40),
        ]},
        'defense': {'label': '수비', 'rows': [
            srow('tkl',    '태클 시도',   '', 'tackles_attempted',      ss, 70),
            srow('tklS',   '태클 성공',   '', 'tackles_successful',     ss, 55),
            prow('tklR',   '태클 성공률', 'tackles_successful', 'tackles_attempted', ss, 100),
            srow('int',    '인터셉트',    '', 'interceptions',          ss, 45),
            srow('clr',    '클리어런스',  '', 'clearances',             ss, 50),
            srow('aerial', '공중볼 경합', '', 'aerial_duels_attempted', ss, 100),
            srow('aerialW','공중볼 성공', '', 'aerial_duels_won',       ss, 60),
        ]},
    }

# ── HTML 생성 ──────────────────────────────────────────────────────
def player_card(p, i):
    n = str(i + 1).zfill(2)
    av = p['name'][0]
    return (f'<div class="player-card active-{i+1}">'
            f'<div class="player-slot-label">&#9658; PLAYER {n}</div>'
            f'<div class="player-info">'
            f'<div class="player-avatar avatar-{i+1}">{av}</div>'
            f'<div><div class="player-name">{p["name"]}</div>'
            f'<div class="player-meta"><span class="player-pos pos-{i+1}">{p["position"]}</span>'
            f' {p["team"]} &middot; {p["season"]}시즌 &middot; {p["games"]}경기</div>'
            f'</div></div></div>')

def legend_items(players):
    return ''.join(
        f'<div class="legend-item"><div class="legend-dot" style="background:var(--p{i+1})"></div>'
        f' {p["name"]} &middot; {p["team"]}</div>'
        for i, p in enumerate(players)
    )

def build_html(players, data):
    cards  = '\n'.join(player_card(p, i) for i, p in enumerate(players))
    legend = legend_items(players)
    season_badge = ' · '.join(str(p['season']) for p in players)
    pj = json.dumps(players, ensure_ascii=False)
    dj = json.dumps(data,    ensure_ascii=False)

    return """<!DOCTYPE html>
<html lang="ko">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>K리그 선수 비교</title>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Noto+Sans+KR:wght@300;400;500;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
<style>
:root{--bg:#0a0d12;--surface:#111519;--surface2:#181d24;--border:#1e2530;--accent:#e8ff3c;--text:#f0f4ff;--muted:#5a6578;--muted2:#8a97a8;--p1:#e8ff3c;--p2:#3caaff;--p3:#ff8f3c;}
*{margin:0;padding:0;box-sizing:border-box;}
body{background:var(--bg);color:var(--text);font-family:'Noto Sans KR',sans-serif;min-height:100vh;}
header{display:flex;align-items:center;justify-content:space-between;padding:16px 36px;border-bottom:1px solid var(--border);background:rgba(10,13,18,.97);backdrop-filter:blur(14px);}
.logo{display:flex;align-items:baseline;gap:8px;}
.logo-k{font-family:'Bebas Neue',sans-serif;font-size:2rem;color:var(--accent);letter-spacing:2px;}
.logo-rest{font-family:'Bebas Neue',sans-serif;font-size:1.4rem;color:var(--text);letter-spacing:4px;opacity:.82;}
.season-badge{font-family:'JetBrains Mono',monospace;font-size:.7rem;color:var(--muted2);border:1px solid var(--border);padding:4px 12px;letter-spacing:1px;}
.main{max-width:1280px;margin:0 auto;padding:24px 36px 48px;}
.page-title{margin-bottom:24px;}
.page-title h1{font-family:'Bebas Neue',sans-serif;font-size:2.4rem;letter-spacing:3px;}
.page-title h1 span{color:var(--accent);}
.page-title p{margin-top:6px;font-size:.8rem;color:var(--muted2);font-weight:300;}
.player-selector{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:24px;}
.player-card{background:var(--surface);border:1px solid var(--border);padding:16px;}
.player-card.active-1{border-color:var(--p1);}.player-card.active-2{border-color:var(--p2);}.player-card.active-3{border-color:var(--p3);}
.player-slot-label{font-family:'JetBrains Mono',monospace;font-size:.6rem;color:var(--muted);letter-spacing:2px;margin-bottom:10px;}
.player-card.active-1 .player-slot-label{color:var(--p1);}.player-card.active-2 .player-slot-label{color:var(--p2);}.player-card.active-3 .player-slot-label{color:var(--p3);}
.player-info{display:flex;align-items:center;gap:12px;}
.player-avatar{width:46px;height:46px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-family:'Bebas Neue',sans-serif;font-size:1.2rem;flex-shrink:0;}
.avatar-1{background:rgba(232,255,60,.13);color:var(--p1);}.avatar-2{background:rgba(60,170,255,.13);color:var(--p2);}.avatar-3{background:rgba(255,143,60,.13);color:var(--p3);}
.player-name{font-size:.95rem;font-weight:700;}
.player-meta{font-size:.7rem;color:var(--muted2);margin-top:4px;display:flex;gap:6px;align-items:center;flex-wrap:wrap;}
.player-pos{font-family:'JetBrains Mono',monospace;font-size:.64rem;padding:1px 6px;border:1px solid;}
.pos-1{border-color:var(--p1);color:var(--p1);}.pos-2{border-color:var(--p2);color:var(--p2);}.pos-3{border-color:var(--p3);color:var(--p3);}
.controls-row{display:flex;align-items:stretch;justify-content:space-between;border-bottom:1px solid var(--border);}
.category-tabs{display:flex;}
.tab-btn{background:none;border:none;color:var(--muted);font-family:'JetBrains Mono',monospace;font-size:.68rem;letter-spacing:1.5px;text-transform:uppercase;padding:11px 20px;cursor:pointer;border-bottom:2px solid transparent;transition:color .2s,border-color .2s;position:relative;top:1px;}
.tab-btn:hover{color:var(--text);}.tab-btn.active{color:var(--accent);border-bottom-color:var(--accent);}
.view-toggle{display:flex;align-items:center;padding-bottom:1px;}
.view-btn{background:none;border:1px solid var(--border);color:var(--muted);font-family:'JetBrains Mono',monospace;font-size:.63rem;letter-spacing:1px;padding:6px 14px;cursor:pointer;transition:all .2s;display:flex;align-items:center;gap:5px;}
.view-btn:first-child{border-right:none;}.view-btn.active{background:var(--surface2);color:var(--accent);border-color:var(--accent);}
.view-btn svg{width:13px;height:13px;}
.stat-filter-panel{background:var(--surface);border:1px solid var(--border);border-top:none;padding:12px 18px;display:flex;align-items:center;gap:14px;flex-wrap:wrap;}
.stat-filter-title{font-family:'JetBrains Mono',monospace;font-size:.6rem;letter-spacing:1.5px;color:var(--muted);text-transform:uppercase;white-space:nowrap;flex-shrink:0;}
.stat-chips{display:flex;flex-wrap:wrap;gap:7px;flex:1;}
.chip{font-family:'JetBrains Mono',monospace;font-size:.65rem;letter-spacing:.8px;padding:3px 11px;border:1px solid var(--border);color:var(--muted2);cursor:pointer;transition:all .15s;user-select:none;border-radius:2px;}
.chip:hover{border-color:var(--muted);color:var(--text);}.chip.selected{border-color:var(--accent);color:var(--accent);background:rgba(232,255,60,.07);}
.chip-all.selected{border-color:var(--muted2);color:var(--text);background:var(--surface2);}
.toggle-wrap{display:flex;align-items:center;gap:8px;flex-shrink:0;}
.toggle-label{font-family:'JetBrains Mono',monospace;font-size:.63rem;letter-spacing:1px;color:var(--muted);white-space:nowrap;}
.toggle{width:34px;height:19px;background:var(--border);border-radius:10px;position:relative;cursor:pointer;transition:background .2s;flex-shrink:0;}
.toggle.on{background:#3caaff;}
.toggle::after{content:'';width:13px;height:13px;background:#fff;border-radius:50%;position:absolute;top:3px;left:3px;transition:left .2s;}
.toggle.on::after{left:18px;}
.comparison-table{width:100%;border-collapse:collapse;}
.comparison-table thead tr{background:var(--surface);}
.comparison-table th{padding:16px 14px;text-align:left;font-weight:400;border-bottom:1px solid var(--border);}
.th-stat{font-family:'JetBrains Mono',monospace;font-size:.6rem;letter-spacing:2px;color:var(--muted);text-transform:uppercase;width:200px;}
.th-player{text-align:center;min-width:140px;}
.th-player-name{font-size:.88rem;font-weight:700;display:block;}
.th-player-team{font-size:.68rem;color:var(--muted2);font-weight:300;display:block;margin-top:2px;}
.th-dot{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:5px;vertical-align:middle;}
.dot-1{background:var(--p1);}.dot-2{background:var(--p2);}.dot-3{background:var(--p3);}
.comparison-table tbody tr{border-bottom:1px solid var(--border);transition:background .15s;animation:fadeUp .35s both;}
.comparison-table tbody tr:hover{background:var(--surface);}
td.stat-label{padding:12px 14px;font-size:.78rem;color:var(--muted2);font-weight:300;}
td.stat-val{text-align:center;padding:12px 14px;}
.val-wrap{display:flex;flex-direction:column;align-items:center;gap:4px;}
.val-num{font-family:'JetBrains Mono',monospace;font-size:1.15rem;font-weight:600;}
.val-num.best{font-size:1.28rem;}
.val-bar-bg{width:68px;height:3px;background:var(--border);border-radius:2px;overflow:hidden;}
.val-bar{height:100%;border-radius:2px;transition:width .7s cubic-bezier(.16,1,.3,1);}
.bar-1{background:var(--p1);}.bar-2{background:var(--p2);}.bar-3{background:var(--p3);}
.best-badge{font-family:'JetBrains Mono',monospace;font-size:.54rem;letter-spacing:1px;padding:1px 5px;border-radius:2px;}
.best-badge-1{background:rgba(232,255,60,.13);color:var(--p1);}
.best-badge-2{background:rgba(60,170,255,.13);color:var(--p2);}
.best-badge-3{background:rgba(255,143,60,.13);color:var(--p3);}
.radar-container{background:var(--surface);border:1px solid var(--border);border-top:none;padding:36px 20px;display:flex;flex-direction:column;align-items:center;gap:20px;}
#radarCanvas{display:block;}
.radar-legend{display:flex;gap:24px;justify-content:center;}
.radar-legend-item{display:flex;align-items:center;gap:7px;font-size:.76rem;color:var(--muted2);}
.radar-legend-dot{width:9px;height:9px;border-radius:50%;}
.radar-note{font-family:'JetBrains Mono',monospace;font-size:.63rem;color:var(--muted);letter-spacing:1px;text-align:center;}
.empty-state{padding:50px 20px;text-align:center;color:var(--muted);font-size:.83rem;border:1px dashed var(--border);border-top:none;}
.legend{margin-top:20px;display:flex;align-items:center;gap:20px;padding-top:16px;border-top:1px solid var(--border);flex-wrap:wrap;}
.legend-item{display:flex;align-items:center;gap:7px;font-size:.73rem;color:var(--muted2);font-weight:300;}
.legend-dot{width:9px;height:9px;border-radius:50%;}
@keyframes fadeUp{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
</style></head>
<body>
<header>
  <div class="logo"><span class="logo-k">K</span><span class="logo-rest">LEAGUE STATS</span></div>
  <span class="season-badge">SEASON_BADGE</span>
</header>
<div class="main">
  <div class="page-title">
    <h1>선수 <span>비교</span> 매트릭스</h1>
    <p>최대 3명 선택 &middot; 카테고리별 지표 커스터마이징 &middot; 표 / 레이더차트 전환 &middot; 90분 기준 토글</p>
  </div>
  <div class="player-selector">PLAYER_CARDS</div>
  <div class="controls-row">
    <div class="category-tabs">
      <button class="tab-btn active" onclick="changeCategory(this,'attack')">공격</button>
      <button class="tab-btn" onclick="changeCategory(this,'pass')">패스</button>
      <button class="tab-btn" onclick="changeCategory(this,'defense')">수비</button>
    </div>
    <div class="view-toggle">
      <button class="view-btn active" id="viewTable" onclick="setView('table')">
        <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5">
          <rect x="1" y="1" width="14" height="3" rx=".5"/><rect x="1" y="6" width="14" height="3" rx=".5"/><rect x="1" y="11" width="14" height="3" rx=".5"/>
        </svg>표 형식
      </button>
      <button class="view-btn" id="viewRadar" onclick="setView('radar')">
        <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5">
          <polygon points="8,1 15,6 12.5,14 3.5,14 1,6"/><polygon points="8,4 12,7 10.3,12 5.7,12 4,7"/>
        </svg>레이더 차트
      </button>
    </div>
  </div>
  <div class="stat-filter-panel">
    <span class="stat-filter-title">지표 선택</span>
    <div class="stat-chips" id="statChips"></div>
    <div class="toggle-wrap">
      <span class="toggle-label">90분 기준</span>
      <div class="toggle" id="per90Toggle" onclick="this.classList.toggle('on');renderOutput()"></div>
    </div>
  </div>
  <div id="outputArea"></div>
  <div class="legend">LEGEND_ITEMS
    <div style="margin-left:auto;font-size:.68rem;color:var(--muted);font-family:'JetBrains Mono',monospace;">&#9733; = 세 선수 중 최고값</div>
  </div>
</div>
<script>
const PLAYERS = PLAYERS_JSON;
const DATA    = DATA_JSON;
const COLORS       = ['#e8ff3c','#3caaff','#ff8f3c'];
const COLORS_ALPHA = ['rgba(232,255,60,','rgba(60,170,255,','rgba(255,143,60,'];
const BAR_CLS   = ['bar-1','bar-2','bar-3'];
const BADGE_CLS = ['best-badge-1','best-badge-2','best-badge-3'];
let currentCategory = 'attack', currentView = 'table';
let selectedStats = new Set(DATA.attack.rows.map(r => r.key));

function changeCategory(btn, cat) {
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  currentCategory = cat;
  selectedStats = new Set(DATA[cat].rows.map(r => r.key));
  renderChips(); renderOutput();
}
function setView(v) {
  currentView = v;
  document.getElementById('viewTable').classList.toggle('active', v === 'table');
  document.getElementById('viewRadar').classList.toggle('active', v === 'radar');
  renderOutput();
}
function renderChips() {
  const rows = DATA[currentCategory].rows;
  const allSel = selectedStats.size === rows.length;
  let html = `<span class="chip chip-all ${allSel?'selected':''}" onclick="toggleAll()">전체</span>`;
  rows.forEach(r => { html += `<span class="chip ${selectedStats.has(r.key)?'selected':''}" onclick="toggleChip('${r.key}')">${r.stat}</span>`; });
  document.getElementById('statChips').innerHTML = html;
}
function toggleChip(key) {
  if (selectedStats.has(key)) { if (selectedStats.size===1) return; selectedStats.delete(key); }
  else selectedStats.add(key);
  renderChips(); renderOutput();
}
function toggleAll() {
  const rows = DATA[currentCategory].rows;
  selectedStats = selectedStats.size===rows.length ? new Set([rows[0].key]) : new Set(rows.map(r=>r.key));
  renderChips(); renderOutput();
}
function renderOutput() { currentView==='table' ? renderTable() : renderRadar(); }

function renderTable() {
  const rows = DATA[currentCategory].rows.filter(r => selectedStats.has(r.key));
  if (!rows.length) { document.getElementById('outputArea').innerHTML='<div class="empty-state">표시할 지표가 없습니다.</div>'; return; }
  const isPer90 = document.getElementById('per90Toggle').classList.contains('on');
  const vk = isPer90 ? 'vals_p90' : 'vals';
  const headerCols = PLAYERS.map((p,i) =>
    `<th class="th-player"><span class="th-dot dot-${i+1}"></span><span class="th-player-name">${p.name}</span><span class="th-player-team">${p.team}</span></th>`
  ).join('');
  let html = `<table class="comparison-table"><thead><tr><th class="th-stat">스탯 항목</th>${headerCols}</tr></thead><tbody>`;
  rows.forEach((row, ri) => {
    const vals = row[vk];
    const maxVal = Math.max(...vals) || 1;
    const bestIdx = vals.indexOf(Math.max(...vals));
    html += `<tr style="animation-delay:${.04+ri*.04}s"><td class="stat-label">${row.stat}${isPer90&&row.unit!=='%'?' /90':''}</td>`;
    vals.forEach((v, i) => {
      const barPct = Math.round((v/maxVal)*100);
      const isBest = i===bestIdx;
      const disp = Number.isInteger(v) ? v : parseFloat(v.toFixed(2));
      html += `<td class="stat-val"><div class="val-wrap">
        <span class="val-num${isBest?' best':''}" style="color:${isBest?COLORS[i]:'var(--text)'}">${disp}${row.unit}</span>
        <div class="val-bar-bg"><div class="val-bar ${BAR_CLS[i]}" style="width:${barPct}%"></div></div>
        ${isBest?`<span class="best-badge ${BADGE_CLS[i]}">&#9733; 최고</span>`:''}
      </div></td>`;
    });
    html += '</tr>';
  });
  html += '</tbody></table>';
  document.getElementById('outputArea').innerHTML = html;
}

function renderRadar() {
  const rows = DATA[currentCategory].rows.filter(r => selectedStats.has(r.key));
  const isPer90 = document.getElementById('per90Toggle').classList.contains('on');
  const vk = isPer90 ? 'vals_p90' : 'vals';
  if (rows.length < 3) {
    document.getElementById('outputArea').innerHTML='<div class="empty-state">레이더 차트는 지표를 3개 이상 선택해야 합니다.</div>'; return;
  }
  const legendHtml = PLAYERS.map((p,i) =>
    `<div class="radar-legend-item"><div class="radar-legend-dot" style="background:${COLORS[i]}"></div>${p.name}</div>`
  ).join('');
  document.getElementById('outputArea').innerHTML = `
    <div class="radar-container">
      <canvas id="radarCanvas" width="500" height="460"></canvas>
      <div class="radar-legend">${legendHtml}</div>
      <div class="radar-note">각 축: radarMax 기준 비율 &middot; 선택된 지표: ${rows.length}개</div>
    </div>`;
  drawRadar(rows, vk);
}

function drawRadar(rows, vk) {
  const canvas = document.getElementById('radarCanvas');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const W=canvas.width, H=canvas.height, cx=W/2, cy=H/2-10;
  const R = Math.min(W,H)*0.35;
  const n = rows.length;
  const angles = rows.map((_,i) => (Math.PI*2*i/n) - Math.PI/2);
  ctx.clearRect(0,0,W,H);
  const norm = (val,row) => Math.min(val/row.radarMax, 1);
  for (let lv=1; lv<=5; lv++) {
    const r = R*(lv/5);
    ctx.beginPath();
    angles.forEach((a,i) => { const x=cx+r*Math.cos(a),y=cy+r*Math.sin(a); i===0?ctx.moveTo(x,y):ctx.lineTo(x,y); });
    ctx.closePath();
    ctx.strokeStyle=lv===5?'#2a3340':'#1a2030'; ctx.lineWidth=lv===5?1.5:1; ctx.stroke();
    if (lv%2===0) {
      ctx.font='10px JetBrains Mono,monospace'; ctx.fillStyle='#3a4555'; ctx.textAlign='center';
      ctx.fillText((lv/5*100).toFixed(0)+'%', cx+4, cy-r+12);
    }
  }
  angles.forEach((a,i) => {
    const x2=cx+R*Math.cos(a), y2=cy+R*Math.sin(a);
    ctx.beginPath(); ctx.moveTo(cx,cy); ctx.lineTo(x2,y2);
    ctx.strokeStyle='#1e2530'; ctx.lineWidth=1; ctx.stroke();
    const lx=cx+(R+30)*Math.cos(a), ly=cy+(R+30)*Math.sin(a);
    ctx.font='11px Noto Sans KR,sans-serif'; ctx.fillStyle='#8a97a8';
    ctx.textAlign=Math.abs(Math.cos(a))<.1?'center':(Math.cos(a)>0?'left':'right');
    ctx.textBaseline=ly<cy-4?'bottom':(ly>cy+4?'top':'middle');
    ctx.fillText(rows[i].stat, lx, ly);
  });
  [0,1,2].slice().reverse().forEach(pi => {
    const pts = rows.map((row,i) => {
      const n2 = norm(row[vk][pi], row);
      return {x:cx+R*n2*Math.cos(angles[i]), y:cy+R*n2*Math.sin(angles[i])};
    });
    ctx.beginPath();
    pts.forEach((p,i) => i===0?ctx.moveTo(p.x,p.y):ctx.lineTo(p.x,p.y));
    ctx.closePath();
    ctx.fillStyle=COLORS_ALPHA[pi]+'0.13)'; ctx.fill();
    ctx.strokeStyle=COLORS[pi]; ctx.lineWidth=2; ctx.stroke();
    pts.forEach(p => {
      ctx.beginPath(); ctx.arc(p.x,p.y,4,0,Math.PI*2);
      ctx.fillStyle=COLORS[pi]; ctx.fill();
      ctx.strokeStyle='#0a0d12'; ctx.lineWidth=1.5; ctx.stroke();
    });
  });
}
renderChips(); renderOutput();
</script>
</body></html>""".replace('SEASON_BADGE', season_badge)\
              .replace('PLAYER_CARDS', cards)\
              .replace('LEGEND_ITEMS', legend)\
              .replace('PLAYERS_JSON', pj)\
              .replace('DATA_JSON', dj)

# ── Streamlit UI ──────────────────────────────────────────────────────
st.set_page_config(page_title="K리그 선수 비교", layout="wide", initial_sidebar_state="collapsed")
st.markdown("""<style>
.stApp,[data-testid="stAppViewContainer"]{background:#0a0d12;}
[data-testid="stHeader"]{display:none;}
.block-container{padding-top:1.2rem;padding-bottom:0;}
label,[data-testid="stWidgetLabel"]{color:#8a97a8!important;}
</style>""", unsafe_allow_html=True)

seasons = get_seasons()

st.markdown('<p style="color:#5a6578;font-family:monospace;font-size:.75rem;letter-spacing:2px;">SELECT PLAYERS</p>', unsafe_allow_html=True)
c1, c2, c3 = st.columns(3)

def player_selector(col, key):
    with col:
        season   = st.selectbox("시즌",  seasons, key=f"s{key}")
        teams    = get_teams(season)
        team_name = st.selectbox("팀",   teams['team_name'].tolist(), key=f"t{key}")
        team_id  = int(teams.loc[teams['team_name']==team_name,'team_id'].iloc[0])
        players  = get_players(season, team_id)
        if players.empty:
            st.warning("선수 없음"); return None, None
        labels = players.apply(lambda r: f"{r['player_name']} ({r['position']}, #{int(r['back_number'])})", axis=1).tolist()
        label  = st.selectbox("선수", labels, key=f"p{key}")
        row    = players.iloc[labels.index(label)]
        df     = get_player_season_stats(season, int(row['player_id']))
        info   = dict(name=row['player_name'], team=team_name, position=row['position'], games=len(df), season=season)
        return info, aggregate(df)

i1, s1 = player_selector(c1, 1)
i2, s2 = player_selector(c2, 2)
i3, s3 = player_selector(c3, 3)

if i1 and i2 and i3:
    html = build_html([i1, i2, i3], build_data([s1, s2, s3]))
    components.html(html, height=960, scrolling=False)
