"""
FC 서울 2025→2026 시즌 분석 리포트 생성기
출력: reports/seoul_analysis_2026.pdf  (블로그/문서용)
      reports/cards/card_*.png          (커뮤니티 카드뉴스용)
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
import matplotlib.patheffects as pe
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np
import os

# ── 한국어 폰트 설정 ──────────────────────────────────────────────────
plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False

# ── 색상 팔레트 ──────────────────────────────────────────────────────
RED    = '#E8002D'
PURPLE = '#5B4FCF'
DARK   = '#1A1A2E'
MID    = '#4A4A6A'
LIGHT  = '#AAAACC'
GREEN  = '#0D9B5A'
BG     = '#F5F5F0'
CARD   = '#FFFFFF'
GOLD   = '#F5A623'

RED_A   = '#E8002D22'
PURPLE_A= '#5B4FCF22'

# ── 데이터 ──────────────────────────────────────────────────────────
ATK_EFF = {
    'labels': ['슈팅 전환율', '크로스 성공률', '드리블 성공률', '유효슈팅률', 'PA 내 슈팅 비율'],
    'v25':    [8.4,  21.6, 35.6, 34.2, 55.3],
    'v26':    [19.7, 23.4, 44.0, 40.8, 67.1],
}
ATK_VOL = {
    'labels': ['오프사이드', 'PA 외 슈팅', '드리블 성공', '경기당 득점'],
    'v25':    [1.13, 6.58, 1.82, 1.24],
    'v26':    [0.43, 3.14, 3.14, 2.14],
}
PASS_RATE = {
    'labels': ['롱패스', '전방패스', '공격 3분지1', '전체 패스', '숏패스'],
    'v25':    [56.2, 71.4, 84.2, 85.5, 88.4],
    'v26':    [52.3, 70.6, 84.9, 84.9, 88.4],
}
DEF_EFF = {
    'labels': ['태클 성공률', '공중 경합 승률', '지상 경합 승률'],
    'v25':    [29.0, 48.5, 52.0],
    'v26':    [20.7, 42.8, 56.8],
}
DEF_VOL = {
    'labels': ['파울', '인터셉트', '태클 시도', '블록', '클리어링'],
    'v25':    [10.1, 8.95, 23.6, 17.6, 23.4],
    'v26':    [13.7, 7.86, 26.9, 19.9, 27.4],
}

KPI = [
    ('슈팅 전환율',   '8.4%',  '19.7%', '+135%', RED,  True),
    ('경기당 득점',   '1.24',  '2.14',  '+72.6%',RED,  True),
    ('드리블 성공률', '35.6%', '44.0%', '+23.6%',GREEN,True),
    ('공중 경합 승률','48.5%', '42.8%', '−11.8%',GOLD, False),
]

SUMMARY = [
    ('공격 결정력',  '슈팅 전환율',   '8.4%',     '19.7%',   '[UP] 대폭 향상', GREEN),
    ('슈팅 위치',   'PA 내 슈팅 비율', '55.3%',   '67.1%',   '[UP] 향상',     GREEN),
    ('개인 돌파',   '드리블 성공률',  '35.6%',    '44.0%',   '[UP] 향상',     GREEN),
    ('전술 규율',   '오프사이드/경기', '1.13회',  '0.43회',  '[UP] 향상',     GREEN),
    ('패스 빌드업', '전체 패스 성공률','85.5%',   '84.9%',   '[-] 유지',      LIGHT),
    ('지상 경합',   '경합 승률',      '52.0%',    '56.8%',   '[UP] 향상',     GREEN),
    ('공중 경합',   '경합 승률',      '48.5%',    '42.8%',   '[!] 주의',      GOLD),
    ('태클 효율',   '태클 성공률',    '29.0%',    '20.7%',   '[!] 확인 필요', GOLD),
    ('경기 강도',   '파울/피파울',    '10.1/10.3','13.7/13.7','[UP] 증가',    MID),
]

# ── 출력 폴더 ────────────────────────────────────────────────────────
os.makedirs('reports/cards', exist_ok=True)

# ════════════════════════════════════════════════════════════════════
# 헬퍼 함수
# ════════════════════════════════════════════════════════════════════

def set_bg(fig, color=BG):
    fig.patch.set_facecolor(color)

def styled_bar(ax, labels, v25, v26, unit='', x_min=None, x_max=None):
    """가로 그룹 바 차트"""
    y = np.arange(len(labels))
    h = 0.35
    bars25 = ax.barh(y + h/2, v25, h, color=PURPLE, alpha=0.85, label='2025시즌', zorder=3)
    bars26 = ax.barh(y - h/2, v26, h, color=RED,    alpha=0.85, label='2026시즌', zorder=3)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=10.5, color=MID)
    ax.xaxis.set_tick_params(labelsize=9, labelcolor=LIGHT)
    ax.set_facecolor(CARD)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['bottom'].set_color('#E8E8E4')
    ax.grid(axis='x', color='#F0F0EE', linewidth=0.8, zorder=0)
    ax.tick_params(left=False)
    if x_min is not None: ax.set_xlim(x_min, x_max)
    # 값 레이블
    for bar in bars25:
        w = bar.get_width()
        ax.text(w + (x_max - x_min)*0.01 if x_min else w*0.02,
                bar.get_y() + bar.get_height()/2,
                f'{w}{unit}', va='center', fontsize=8.5, color=PURPLE, fontweight='bold')
    for bar in bars26:
        w = bar.get_width()
        ax.text(w + (x_max - x_min)*0.01 if x_min else w*0.02,
                bar.get_y() + bar.get_height()/2,
                f'{w}{unit}', va='center', fontsize=8.5, color=RED, fontweight='bold')
    return ax

def dumbbell(ax, labels, v25, v26, x_min=0, x_max=80, unit='%'):
    """덤벨 차트"""
    ax.set_facecolor(CARD)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['bottom'].set_color('#E8E8E4')
    ax.grid(axis='x', color='#F0F0EE', linewidth=0.8, zorder=0)
    ax.tick_params(left=False, bottom=True)
    ax.set_xlim(x_min, x_max)
    ax.xaxis.set_tick_params(labelsize=9, labelcolor=LIGHT)
    ax.set_xticks(np.arange(x_min, x_max + 1, 20 if x_max > 50 else 10))
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f'{int(v)}%'))

    y_pos = np.arange(len(labels))
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=10.5, color=MID)

    for i, (l, a, b) in enumerate(zip(labels, v25, v26)):
        is_up = b >= a
        lc = '#E8002D33' if is_up else '#5B4FCF33'
        ax.plot([a, b], [i, i], color=lc, linewidth=3, zorder=2)

        # Arrow tip
        dx = 0.5 if is_up else -0.5
        ax.annotate('', xy=(b, i), xytext=(b - dx*2, i),
                    arrowprops=dict(arrowstyle='->', color=RED if is_up else PURPLE,
                                    lw=2), zorder=3)

        # Dots
        ax.scatter(a, i, s=200, color=PURPLE, zorder=4, linewidths=0)
        ax.scatter(b, i, s=200, color=RED,    zorder=4, linewidths=0)

        # Value labels
        ax.text(a, i + 0.28, f'{a}{unit}', ha='center', fontsize=8.5,
                color=PURPLE, fontweight='bold')
        ax.text(b, i + 0.28, f'{b}{unit}', ha='center', fontsize=8.5,
                color=RED, fontweight='bold')

        # Delta badge
        delta = (b - a) / a * 100 if a != 0 else 0
        sign = '+' if delta >= 0 else ''
        bc = GREEN if delta >= 0 else '#E8002D'
        ax.text(x_max, i, f'{sign}{delta:.1f}%',
                ha='right', va='center', fontsize=8.5,
                color=bc, fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.25', facecolor='#FAFAFA',
                          edgecolor=bc, linewidth=0.8))

def legend_handles():
    return [
        mpatches.Patch(color=PURPLE, label='2025시즌'),
        mpatches.Patch(color=RED,    label='2026시즌'),
    ]

def section_title(ax_or_fig, text, subtitle='', y=0.97, x=0.0, fontsize=15, fig=None):
    """섹션 제목 텍스트"""
    target = ax_or_fig if fig is None else fig
    target.text(x, y, text, transform=target.transFigure,
                fontsize=fontsize, fontweight='black', color=DARK, va='top')
    if subtitle:
        target.text(x, y - 0.042, subtitle, transform=target.transFigure,
                    fontsize=8.5, color=LIGHT, va='top')

def page_footer(fig, note=''):
    fig.text(0.5, 0.012,
             f'K리그 데이터랩 | FC 서울 team_id=29998 | '
             f'데이터: player_match_stats · kleague.db | '
             f'2025시즌 38경기 / 2026시즌 1R~7R (7경기){" | " + note if note else ""}',
             ha='center', fontsize=7, color=LIGHT, style='italic')

# ════════════════════════════════════════════════════════════════════
# PDF 생성
# ════════════════════════════════════════════════════════════════════
pdf_path = 'reports/seoul_analysis_2026.pdf'

with PdfPages(pdf_path) as pdf:

    # ── PAGE 1: 커버 ──────────────────────────────────────────────
    fig = plt.figure(figsize=(8.27, 11.69))  # A4
    set_bg(fig, DARK)

    # 상단 빨간 띠
    fig.add_axes([0, 0.88, 1, 0.12]).set_axis_off()
    ax_band = fig.add_axes([0, 0.88, 1, 0.12])
    ax_band.set_facecolor(RED)
    ax_band.set_axis_off()

    # 제목
    fig.text(0.08, 0.84, 'K리그1 데이터 분석', fontsize=10,
             color=RED, fontweight='bold', va='top')
    fig.text(0.08, 0.80, '슈팅은 줄었는데\n골은 두 배가 됐다', fontsize=30,
             color='white', fontweight='black', va='top', linespacing=1.25)
    fig.text(0.08, 0.66,
             'FC 서울의 2025→2026 시즌 스탯 변화\n'
             '결정력, 드리블, 수비 강도까지 — 경기당 평균 40개 지표 심층 분석',
             fontsize=11, color='#AAAACC', va='top', linespacing=1.6)

    # 메타
    fig.text(0.08, 0.58, '[*] 2025시즌 38경기  [*] 2026시즌 1R~7R (7경기)',
             fontsize=9, color='#AAAACC', va='top')
    fig.text(0.08, 0.55, '분석 기준일: 2026-04-25  |  K리그1  |  K리그 데이터랩',
             fontsize=9, color='#666699', va='top')

    # 구분선
    fig.add_artist(plt.Line2D([0.08, 0.92], [0.52, 0.52],
                              transform=fig.transFigure,
                              color='#333366', linewidth=1))

    # KPI 카드 4개
    kpi_labels = ['슈팅 전환율', '경기당 득점', '드리블 성공률', '공중 경합 승률']
    kpi_from   = ['8.4%',  '1.24골', '35.6%', '48.5%']
    kpi_to     = ['19.7%', '2.14골', '44.0%', '42.8%']
    kpi_delta  = ['+135%', '+72.6%', '+23.6%', '−11.8%']
    kpi_colors = [RED, RED, GREEN, GOLD]
    kpi_up     = [True, True, True, False]

    for i, (lbl, frm, to, dlt, col, up) in enumerate(
            zip(kpi_labels, kpi_from, kpi_to, kpi_delta, kpi_colors, kpi_up)):
        x0 = 0.08 + i * 0.22
        ax = fig.add_axes([x0, 0.30, 0.20, 0.20])
        ax.set_facecolor('#1E1E3A')
        ax.set_axis_off()

        ax.text(0.5, 0.92, lbl, ha='center', va='top', fontsize=8.5,
                color='#8888AA', fontweight='bold',
                transform=ax.transAxes)
        ax.text(0.5, 0.68, frm, ha='center', va='top', fontsize=12,
                color=PURPLE, fontweight='bold',
                transform=ax.transAxes)
        ax.text(0.5, 0.48, '↓' if not up else '↑', ha='center', va='top',
                fontsize=14, color=col, transform=ax.transAxes)
        ax.text(0.5, 0.28, to, ha='center', va='top', fontsize=16,
                color=col, fontweight='black', transform=ax.transAxes)
        ax.text(0.5, 0.06, dlt, ha='center', va='bottom', fontsize=9,
                color='white', fontweight='bold', transform=ax.transAxes)

    # 하단 안내
    fig.text(0.08, 0.28,
             '[!]  2026시즌은 7경기 기준 수치입니다. 방향성 파악 용도로 참고하시고 시즌 중반 재검토를 권장합니다.',
             fontsize=8, color=GOLD, va='top')

    page_footer(fig)
    pdf.savefig(fig, bbox_inches='tight', facecolor=DARK)
    plt.close(fig)

    # ── PAGE 2: 공격 분석 ─────────────────────────────────────────
    fig = plt.figure(figsize=(8.27, 11.69))
    set_bg(fig, BG)

    # 섹션 헤더
    ax_hdr = fig.add_axes([0, 0.92, 1, 0.08])
    ax_hdr.set_facecolor(RED)
    ax_hdr.set_axis_off()
    ax_hdr.text(0.04, 0.55, '01  공격 분석 — 슈팅을 줄이고 골을 늘리다',
                fontsize=14, color='white', fontweight='black', va='center',
                transform=ax_hdr.transAxes)

    # 리드 텍스트
    fig.text(0.06, 0.90,
             '2026시즌 슈팅은 경기당 14.76 → 10.86개(-26%)로 감소했지만 득점은 1.24 → 2.14골(+73%)로 급증했다.',
             fontsize=9.5, color=MID, va='top', wrap=True)
    fig.text(0.06, 0.875,
             '슈팅 전환율 8.4% → 19.7%: 무의미한 장거리 슈팅을 제거하고 PA 내 고확률 위치에서만 슈팅하는 패턴으로 변화했다.',
             fontsize=9.5, color=MID, va='top')

    # 인사이트 박스
    ax_ins = fig.add_axes([0.06, 0.80, 0.88, 0.05])
    ax_ins.set_facecolor('#FDEAEE')
    ax_ins.set_axis_off()
    ax_ins.text(0.01, 0.7, '핵심:  PA 외 슈팅 -52% + PA 내 슈팅 비율 55.3%→67.1% → 슈팅 전환율 2.35배 향상',
                fontsize=9, color=RED, fontweight='bold', va='center',
                transform=ax_ins.transAxes)

    # Chart A: 공격 효율 덤벨
    ax1 = fig.add_axes([0.06, 0.52, 0.88, 0.27])
    ax1.set_facecolor(CARD)
    ax1.set_title('공격 효율 지표 비교 (%)', fontsize=10.5, color=DARK,
                  fontweight='bold', loc='left', pad=8)
    dumbbell(ax1, ATK_EFF['labels'], ATK_EFF['v25'], ATK_EFF['v26'],
             x_min=0, x_max=80, unit='%')
    ax1.legend(handles=legend_handles(), loc='lower right',
               fontsize=8.5, frameon=False)

    # Chart B: 공격 볼륨 바
    ax2 = fig.add_axes([0.06, 0.18, 0.88, 0.30])
    ax2.set_facecolor(CARD)
    ax2.set_title('경기당 평균 — 공격 볼륨 지표', fontsize=10.5, color=DARK,
                  fontweight='bold', loc='left', pad=8)
    styled_bar(ax2, ATK_VOL['labels'], ATK_VOL['v25'], ATK_VOL['v26'])
    ax2.legend(handles=legend_handles(), loc='lower right',
               fontsize=8.5, frameon=False)

    # 해설 요약
    fig.text(0.06, 0.155,
             '▶  드리블 시도 +40%, 성공률 35.6→44.0%: 개인 돌파가 유효한 공격 옵션으로 정착',
             fontsize=8.5, color=MID)
    fig.text(0.06, 0.135,
             '▶  오프사이드 1.13 → 0.43회(-62%): 침투 타이밍의 전술적 정교화',
             fontsize=8.5, color=MID)

    page_footer(fig)
    pdf.savefig(fig, bbox_inches='tight', facecolor=BG)
    plt.close(fig)

    # ── PAGE 3: 패스 분석 ─────────────────────────────────────────
    fig = plt.figure(figsize=(8.27, 11.69))
    set_bg(fig, BG)

    ax_hdr = fig.add_axes([0, 0.92, 1, 0.08])
    ax_hdr.set_facecolor(PURPLE)
    ax_hdr.set_axis_off()
    ax_hdr.text(0.04, 0.55, '02  패스 분석 — 스타일 유지, 미세 조정',
                fontsize=14, color='white', fontweight='black', va='center',
                transform=ax_hdr.transAxes)

    fig.text(0.06, 0.90,
             '전체 패스 성공률 85.5% → 84.9%, 숏패스 성공률 88.4% → 88.4%(불변). 빌드업 안정성은 시즌 간 일관되게 유지됐다.',
             fontsize=9.5, color=MID, va='top')
    fig.text(0.06, 0.875,
             '롱패스 시도가 경기당 49.5 → 53.3회(+7.7%)로 증가했으나 성공률은 56.2% → 52.3%로 소폭 하락해 주의가 필요하다.',
             fontsize=9.5, color=MID, va='top')

    ax_ins = fig.add_axes([0.06, 0.84, 0.88, 0.04])
    ax_ins.set_facecolor('#F0EEFF')
    ax_ins.set_axis_off()
    ax_ins.text(0.01, 0.5,
                '포인트:  공격 3분지 1 패스 성공률 84.2% → 84.9% 미세 상승 — 최종 3분지 1 패스 선택의 질이 향상됨',
                fontsize=9, color=PURPLE, fontweight='bold', va='center',
                transform=ax_ins.transAxes)

    ax3 = fig.add_axes([0.06, 0.46, 0.88, 0.36])
    ax3.set_facecolor(CARD)
    ax3.set_title('패스 성공률 비교 (%)', fontsize=10.5, color=DARK,
                  fontweight='bold', loc='left', pad=8)
    styled_bar(ax3, PASS_RATE['labels'], PASS_RATE['v25'], PASS_RATE['v26'],
               unit='%', x_min=40, x_max=100)
    ax3.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f'{int(v)}%'))
    ax3.legend(handles=legend_handles(), loc='lower right',
               fontsize=8.5, frameon=False)

    # 패스 수치 테이블
    ax_tbl = fig.add_axes([0.06, 0.20, 0.88, 0.24])
    ax_tbl.set_axis_off()
    ax_tbl.set_facecolor(CARD)

    tbl_data = [
        ['지표', '2025 (경기당)', '2026 (경기당)', '변화'],
        ['패스 시도', '479.8회', '448.1회', '−6.6%'],
        ['패스 성공', '410.3회', '380.4회', '−7.3%'],
        ['전방패스 시도', '194.1회', '188.7회', '−2.8%'],
        ['롱패스 시도', '49.5회', '53.3회', '+7.7%'],
        ['숏패스 성공률', '88.4%', '88.4%', '±0%'],
    ]
    col_w = [0.34, 0.22, 0.22, 0.18]
    colors_row0 = [DARK] * 4
    for ri, row in enumerate(tbl_data):
        for ci, cell in enumerate(row):
            x = sum(col_w[:ci]) + 0.01
            y = 1.0 - ri * 0.165
            fc = DARK if ri == 0 else (CARD if ri % 2 else '#FAFAFA')
            ax_tbl.add_patch(FancyBboxPatch((x, y-0.14), col_w[ci]-0.01, 0.14,
                                            boxstyle='square', fc=fc, ec='#E8E8E4', lw=0.5))
            tc = 'white' if ri == 0 else (
                GREEN if '+' in cell and ri > 0 else
                (RED if '−' in cell and ri > 0 else MID)
            )
            fw = 'bold' if ri == 0 or ci == 3 else 'normal'
            ax_tbl.text(x + col_w[ci]/2 - 0.005, y - 0.07, cell,
                        ha='center', va='center', fontsize=8.5, color=tc, fontweight=fw)
    ax_tbl.set_xlim(0, 1)
    ax_tbl.set_ylim(0, 1)

    page_footer(fig)
    pdf.savefig(fig, bbox_inches='tight', facecolor=BG)
    plt.close(fig)

    # ── PAGE 4: 수비 분석 ─────────────────────────────────────────
    fig = plt.figure(figsize=(8.27, 11.69))
    set_bg(fig, BG)

    ax_hdr = fig.add_axes([0, 0.92, 1, 0.08])
    ax_hdr.set_facecolor('#0D9B5A')
    ax_hdr.set_axis_off()
    ax_hdr.text(0.04, 0.55, '03  수비 분석 — 강도는 높아졌지만 균열도 보인다',
                fontsize=14, color='white', fontweight='black', va='center',
                transform=ax_hdr.transAxes)

    fig.text(0.06, 0.90,
             '클리어링(+17.1%)·블록(+13.0%)·지상경합 승률(+9.2%)은 향상됐으나 공중경합 승률(-11.8%)과 태클 성공률(-28.6%)은 하락했다.',
             fontsize=9.5, color=MID, va='top')

    ax_ins = fig.add_axes([0.06, 0.845, 0.88, 0.04])
    ax_ins.set_facecolor('#FFF8E6')
    ax_ins.set_axis_off()
    ax_ins.text(0.01, 0.5,
                '주의:  파울(+36%)·피파울(+33%)·경고(+55%) 동반 급증 — 경기 강도 전반 상승 또는 강한 상대 편성 영향',
                fontsize=9, color=GOLD, fontweight='bold', va='center',
                transform=ax_ins.transAxes)

    # 수비 경합 덤벨
    ax4 = fig.add_axes([0.06, 0.62, 0.88, 0.21])
    ax4.set_facecolor(CARD)
    ax4.set_title('수비 경합 승률 비교 (%)', fontsize=10.5, color=DARK,
                  fontweight='bold', loc='left', pad=8)
    dumbbell(ax4, DEF_EFF['labels'], DEF_EFF['v25'], DEF_EFF['v26'],
             x_min=0, x_max=65, unit='%')
    ax4.legend(handles=legend_handles(), loc='lower right',
               fontsize=8.5, frameon=False)

    # 수비 볼륨 바
    ax5 = fig.add_axes([0.06, 0.30, 0.88, 0.30])
    ax5.set_facecolor(CARD)
    ax5.set_title('수비 볼륨 지표 — 경기당 평균', fontsize=10.5, color=DARK,
                  fontweight='bold', loc='left', pad=8)
    styled_bar(ax5, DEF_VOL['labels'], DEF_VOL['v25'], DEF_VOL['v26'])
    ax5.legend(handles=legend_handles(), loc='lower right',
               fontsize=8.5, frameon=False)

    fig.text(0.06, 0.27,
             '▶  클리어링·블록 증가: 수비 저지선이 낮아졌거나 강도 높은 상대와의 경기 영향 가능성',
             fontsize=8.5, color=MID)
    fig.text(0.06, 0.25,
             '▶  태클 성공률 하락: 전방 압박 전술 채택으로 낮은 확률 태클 감수 또는 수비 효율 순수 하락 — 추세 확인 필요',
             fontsize=8.5, color=MID)
    fig.text(0.06, 0.23,
             '▶  볼 미스 5.16 → 4.43(-14%): 볼 점유·처리의 질은 오히려 향상',
             fontsize=8.5, color=MID)

    page_footer(fig)
    pdf.savefig(fig, bbox_inches='tight', facecolor=BG)
    plt.close(fig)

    # ── PAGE 5: 종합 평가 ─────────────────────────────────────────
    fig = plt.figure(figsize=(8.27, 11.69))
    set_bg(fig, BG)

    ax_hdr = fig.add_axes([0, 0.92, 1, 0.08])
    ax_hdr.set_facecolor(DARK)
    ax_hdr.set_axis_off()
    ax_hdr.text(0.04, 0.55, '04  종합 평가',
                fontsize=14, color='white', fontweight='black', va='center',
                transform=ax_hdr.transAxes)

    # 결론 텍스트
    fig.text(0.06, 0.905,
             '2026시즌 FC 서울의 강세는 "더 많이 쏴서"가 아니라 "더 좋은 위치에서 정확하게 쏴서" 만들어진 결과다.',
             fontsize=10, color=DARK, fontweight='bold', va='top')
    fig.text(0.06, 0.878,
             'PA 외 슈팅 제거 + 드리블 성공률 향상 + 오프사이드 급감이 맞물리며 공격 효율이 극대화됐다. 패스 빌드업 안정성은\n'
             '유지되고 있으며, 수비에서는 지상 경합 장악력 향상이 눈에 띄지만 공중 경합·태클 효율은 모니터링이 필요하다.',
             fontsize=9.5, color=MID, va='top', linespacing=1.7)

    # 종합 테이블
    ax_t = fig.add_axes([0.06, 0.35, 0.88, 0.50])
    ax_t.set_axis_off()
    ax_t.set_facecolor(CARD)

    headers = ['부문', '핵심 지표', '2025', '2026', '평가']
    cw = [0.18, 0.28, 0.15, 0.15, 0.20]
    row_h = 0.085

    # 헤더
    for ci, (h, w) in enumerate(zip(headers, cw)):
        x = sum(cw[:ci])
        ax_t.add_patch(FancyBboxPatch((x, 1.0 - row_h), w - 0.005, row_h,
                                      boxstyle='square', fc=DARK, ec='white', lw=1))
        ax_t.text(x + w/2 - 0.003, 1.0 - row_h/2, h,
                  ha='center', va='center', fontsize=8.5,
                  color='white', fontweight='bold')

    for ri, row in enumerate(SUMMARY):
        y_top = 1.0 - (ri + 1) * row_h
        fc = CARD if ri % 2 == 0 else '#FAFAFA'
        for ci, (cell, w) in enumerate(zip(row, cw)):
            x = sum(cw[:ci])
            ax_t.add_patch(FancyBboxPatch((x, y_top - row_h), w - 0.005, row_h,
                                          boxstyle='square', fc=fc, ec='#E8E8E4', lw=0.5))
            tc = row[5] if ci == 4 else MID
            fw = 'bold' if ci in (0, 4) else 'normal'
            if ci < 5:
                ax_t.text(x + w/2 - 0.003, y_top - row_h/2, cell,
                          ha='center', va='center', fontsize=8.5, color=tc, fontweight=fw)

    ax_t.set_xlim(0, 1)
    ax_t.set_ylim(0, 1)

    # 향후 분석 제안
    fig.text(0.06, 0.325, '향후 분석 제안', fontsize=10, color=DARK,
             fontweight='bold', va='top')
    suggestions = [
        '1.  선수별 기여도 분석 — 슈팅 전환율 향상을 주도한 특정 선수 특정',
        '2.  라운드별 추이 — 7라운드 중 주요 지표 트렌드 시각화',
        '3.  리그 평균 대비 비교 — 서울 지표의 K리그1 내 상대적 위치 파악',
    ]
    for i, s in enumerate(suggestions):
        fig.text(0.06, 0.298 - i * 0.022, s, fontsize=9, color=MID, va='top')

    page_footer(fig, note='[!] 2026시즌 7경기 샘플 -- 시즌 중반 재검토 권장')
    pdf.savefig(fig, bbox_inches='tight', facecolor=BG)
    plt.close(fig)

print('[OK] PDF saved: ' + pdf_path)

# ════════════════════════════════════════════════════════════════════
# 카드뉴스 PNG (커뮤니티용 1080×1350)
# ════════════════════════════════════════════════════════════════════
DPI = 150
W_PX, H_PX = 1080, 1350
W_IN, H_IN = W_PX / DPI, H_PX / DPI

def save_card(fig, name):
    path = f'reports/cards/{name}.png'
    fig.savefig(path, dpi=DPI, bbox_inches='tight', facecolor=fig.get_facecolor())
    plt.close(fig)
    print('[OK] Card saved: ' + path)

# ── CARD 1: KPI 커버 ─────────────────────────────────────────────
fig = plt.figure(figsize=(W_IN, H_IN))
set_bg(fig, DARK)

# 상단 빨간 바
ax_b = fig.add_axes([0, 0.88, 1, 0.12])
ax_b.set_facecolor(RED); ax_b.set_axis_off()
ax_b.text(0.5, 0.5, 'K리그 데이터랩', ha='center', va='center',
          fontsize=18, color='white', fontweight='black',
          transform=ax_b.transAxes)

fig.text(0.5, 0.84, '슈팅은 줄었는데 골은 두 배?',
         ha='center', fontsize=24, color='white', fontweight='black', va='top')
fig.text(0.5, 0.78, 'FC 서울 2025 vs 2026 시즌 비교',
         ha='center', fontsize=14, color='#AAAACC', va='top')
fig.text(0.5, 0.73, '7경기 기준 · 경기당 평균 비교',
         ha='center', fontsize=11, color='#666699', va='top')

# KPI 4개 카드
kpi_data = [
    ('슈팅 전환율', '8.4%', '19.7%', '+135%', RED),
    ('경기당 득점', '1.24', '2.14', '+72.6%', RED),
    ('드리블 성공률', '35.6%', '44.0%', '+23.6%', GREEN),
    ('공중 경합 승률', '48.5%', '42.8%', '−11.8%', GOLD),
]
for i, (lbl, frm, to, dlt, col) in enumerate(kpi_data):
    row, col_i = divmod(i, 2)
    x0 = 0.06 + col_i * 0.47
    y0 = 0.52 - row * 0.25
    ax = fig.add_axes([x0, y0, 0.43, 0.22])
    ax.set_facecolor('#1E1E3A'); ax.set_axis_off()
    ax.text(0.5, 0.88, lbl, ha='center', va='top', fontsize=11,
            color='#8888AA', fontweight='bold', transform=ax.transAxes)
    ax.text(0.5, 0.65, frm, ha='center', va='top', fontsize=15,
            color=PURPLE, fontweight='bold', transform=ax.transAxes)
    ax.text(0.5, 0.42, to, ha='center', va='top', fontsize=22,
            color=col, fontweight='black', transform=ax.transAxes)
    ax.text(0.5, 0.10, dlt, ha='center', va='bottom', fontsize=12,
            color='white', fontweight='bold', transform=ax.transAxes)

fig.text(0.5, 0.06,
         '[!] 2026시즌 7경기 샘플 | K리그 데이터랩 | kleague.db',
         ha='center', fontsize=8, color='#444466', va='bottom')

save_card(fig, 'card_01_cover')

# ── CARD 2: 공격 효율 덤벨 ───────────────────────────────────────
fig = plt.figure(figsize=(W_IN, H_IN))
set_bg(fig, BG)

ax_hdr = fig.add_axes([0, 0.88, 1, 0.12])
ax_hdr.set_facecolor(RED); ax_hdr.set_axis_off()
ax_hdr.text(0.5, 0.55, '01 공격 효율 지표 비교',
            ha='center', va='center', fontsize=18, color='white',
            fontweight='black', transform=ax_hdr.transAxes)

fig.text(0.5, 0.855, '슈팅 전환율 8.4% → 19.7% (+135%)',
         ha='center', fontsize=13, color=RED, fontweight='black', va='top')
fig.text(0.5, 0.828,
         'PA 내 슈팅에 집중하고 무의미한 장거리 슈팅을 줄인 결과\n결정력이 2.35배 향상됐다',
         ha='center', fontsize=10.5, color=MID, va='top', linespacing=1.6)

ax_c = fig.add_axes([0.06, 0.30, 0.88, 0.50])
ax_c.set_facecolor(CARD)
dumbbell(ax_c, ATK_EFF['labels'], ATK_EFF['v25'], ATK_EFF['v26'],
         x_min=0, x_max=80, unit='%')
ax_c.legend(handles=legend_handles(), loc='lower right', fontsize=10, frameon=False)
ax_c.set_title('경기당 평균 · 단위: %', fontsize=10, color=LIGHT, loc='right', pad=6)

fig.add_artist(plt.Line2D([0.06, 0.94], [0.28, 0.28],
                          transform=fig.transFigure, color='#E8E8E4'))

for i, (lbl, v25, v26) in enumerate(zip(ATK_EFF['labels'], ATK_EFF['v25'], ATK_EFF['v26'])):
    delta = (v26 - v25) / v25 * 100
    sign = '+' if delta >= 0 else ''
    col = GREEN if delta >= 0 else RED
    x_pos = 0.06 + i * 0.185
    fig.text(x_pos + 0.08, 0.22, lbl, ha='center', fontsize=7.5, color=MID)
    fig.text(x_pos + 0.08, 0.195, f'{sign}{delta:.1f}%', ha='center',
             fontsize=11, color=col, fontweight='black')

fig.text(0.5, 0.06, 'K리그 데이터랩 | FC 서울 · K리그1 2025→2026',
         ha='center', fontsize=8, color=LIGHT, va='bottom')

save_card(fig, 'card_02_attack_efficiency')

# ── CARD 3: 수비 지표 ────────────────────────────────────────────
fig = plt.figure(figsize=(W_IN, H_IN))
set_bg(fig, BG)

ax_hdr = fig.add_axes([0, 0.88, 1, 0.12])
ax_hdr.set_facecolor('#0D5A3A'); ax_hdr.set_axis_off()
ax_hdr.text(0.5, 0.55, '03 수비 지표 비교',
            ha='center', va='center', fontsize=18, color='white',
            fontweight='black', transform=ax_hdr.transAxes)

fig.text(0.5, 0.855, '지상 경합 ↑ · 공중 경합 ↓ · 태클 효율 ↓',
         ha='center', fontsize=13, color=DARK, fontweight='black', va='top')
fig.text(0.5, 0.826,
         '클리어링·블록 증가로 수비 강도는 높아졌지만\n공중 경합과 태클 성공률 하락은 모니터링이 필요하다',
         ha='center', fontsize=10.5, color=MID, va='top', linespacing=1.6)

ax_d = fig.add_axes([0.06, 0.52, 0.88, 0.30])
ax_d.set_facecolor(CARD)
ax_d.set_title('수비 경합 승률 비교', fontsize=11, color=DARK,
               fontweight='bold', loc='left', pad=8)
dumbbell(ax_d, DEF_EFF['labels'], DEF_EFF['v25'], DEF_EFF['v26'],
         x_min=0, x_max=65, unit='%')
ax_d.legend(handles=legend_handles(), loc='lower right', fontsize=10, frameon=False)

ax_dv = fig.add_axes([0.06, 0.17, 0.88, 0.32])
ax_dv.set_facecolor(CARD)
ax_dv.set_title('수비 볼륨 지표 (경기당 평균)', fontsize=11, color=DARK,
                fontweight='bold', loc='left', pad=8)
styled_bar(ax_dv, DEF_VOL['labels'], DEF_VOL['v25'], DEF_VOL['v26'])
ax_dv.legend(handles=legend_handles(), loc='lower right', fontsize=10, frameon=False)

fig.text(0.5, 0.06, 'K리그 데이터랩 | FC 서울 · K리그1 2025→2026',
         ha='center', fontsize=8, color=LIGHT, va='bottom')

save_card(fig, 'card_03_defense')

# ── CARD 4: 종합 요약 ────────────────────────────────────────────
fig = plt.figure(figsize=(W_IN, H_IN))
set_bg(fig, DARK)

ax_hdr = fig.add_axes([0, 0.88, 1, 0.12])
ax_hdr.set_facecolor('#222244'); ax_hdr.set_axis_off()
ax_hdr.text(0.5, 0.55, '04 종합 평가',
            ha='center', va='center', fontsize=18, color='white',
            fontweight='black', transform=ax_hdr.transAxes)

fig.text(0.5, 0.855,
         '"더 많이 쏴서"가 아니라\n"더 좋은 위치에서 정확하게 쏴서"',
         ha='center', fontsize=16, color='white', fontweight='black',
         va='top', linespacing=1.4)
fig.text(0.5, 0.795,
         'PA 외 슈팅 제거 + 드리블 성공률 향상 + 오프사이드 급감이\n맞물리며 FC 서울 공격 효율 극대화',
         ha='center', fontsize=10.5, color='#AAAACC', va='top', linespacing=1.6)

# 점수판 카드
items = [
    ('공격 결정력', '[UP] 대폭 향상', GREEN),
    ('슈팅 위치 선택', '[UP] 향상', GREEN),
    ('개인 돌파', '[UP] 향상', GREEN),
    ('전술 규율', '[UP] 향상', GREEN),
    ('패스 빌드업', '[-] 유지', LIGHT),
    ('지상 경합', '[UP] 향상', GREEN),
    ('공중 경합', '[!] 주의', GOLD),
    ('태클 효율', '[!] 확인 필요', GOLD),
]
for i, (lbl, verdict, col) in enumerate(items):
    row, ci = divmod(i, 2)
    x0 = 0.06 + ci * 0.47
    y0 = 0.73 - row * 0.11
    ax = fig.add_axes([x0, y0 - 0.095, 0.44, 0.09])
    ax.set_facecolor('#1E1E3A'); ax.set_axis_off()
    ax.text(0.05, 0.5, lbl, ha='left', va='center', fontsize=10.5,
            color='#CCCCEE', transform=ax.transAxes)
    ax.text(0.95, 0.5, verdict, ha='right', va='center', fontsize=11,
            color=col, fontweight='bold', transform=ax.transAxes)

fig.text(0.5, 0.27,
         '[!]  2026시즌 7경기 샘플 기준 -- 시즌 중반 재검토 권장',
         ha='center', fontsize=9, color=GOLD, va='top')
fig.text(0.5, 0.10, 'K리그 데이터랩', ha='center',
         fontsize=14, color='white', fontweight='black', va='top')
fig.text(0.5, 0.075, 'FC 서울 · K리그1 2025→2026 시즌 분석',
         ha='center', fontsize=9, color='#666699', va='top')

save_card(fig, 'card_04_summary')

print('\n[DONE]')
print('   PDF:  reports/seoul_analysis_2026.pdf')
print('   Cards: reports/cards/card_01~04.png')
