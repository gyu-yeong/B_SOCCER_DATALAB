# K리그 데이터랩 웹 디자인 가이드

> 이 문서는 세션 간 디자인 일관성 유지를 위한 **단일 진실 공급원(Single Source of Truth)**입니다.
> 새 웹 페이지/컴포넌트를 만들 때 반드시 이 가이드를 따르세요.

---

## 1. 디자인 철학

| 키워드 | 설명 |
|--------|------|
| **데이터 퍼스트** | 시각화가 주인공, UI는 배경 |
| **K리그 아이덴티티** | 라임 #e8ff3c 강조색, 다크 배경, 한글 폰트 |
| **정보 밀도** | 태블로 레퍼런스처럼 지표를 최대한 표시하되 가독성 유지 |
| **소셜 친화** | 인스타/X 공유용 카드는 1080×1080 or 1080×1350 기준 |

---

## 2. 컬러 시스템

```css
:root {
  /* 배경 */
  --bg:       #0a0d12;   /* 최하단 배경 */
  --surface:  #111519;   /* 카드/패널 */
  --surface2: #181d24;   /* 중첩 레이어 */
  --border:   #1e2530;   /* 구분선 */

  /* 텍스트 */
  --text:     #f0f4ff;   /* 본문 */
  --muted:    #5a6578;   /* 비활성/힌트 */
  --muted2:   #8a97a8;   /* 서브텍스트 */

  /* 강조 (K리그 라임) */
  --accent:   #e8ff3c;   /* PRIMARY — K리그 아이덴티티 */

  /* 선수 구분 색 (최대 3인 비교) */
  --p1: #e8ff3c;          /* 라임 — Player 1 */
  --p2: #3caaff;          /* 스카이블루 — Player 2 */
  --p3: #ff8f3c;          /* 오렌지 — Player 3 */
  --p4: #c03cff;          /* 퍼플 — Player 4 (확장 시) */
}
```

### 사용 규칙
- `--accent` 는 **최고값 강조, 로고, 탭 활성화, 주요 CTA**에만 사용
- 배경에 색을 넣을 때는 반드시 알파 13% 이하: `rgba(232,255,60,0.13)`
- 소셜카드용 라이트모드는 별도 섹션 참조

---

## 3. 타이포그래피

| 역할 | 폰트 | 용도 |
|------|------|------|
| **디스플레이** | `Bebas Neue` | 로고, 대제목, 숫자 강조 |
| **본문 (한글)** | `Noto Sans KR` | 선수명, 팀명, 설명 |
| **데이터/코드** | `JetBrains Mono` | 스탯 수치, 배지, 레이블 |

```html
<!-- Google Fonts 임포트 — 모든 페이지 공통 -->
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Noto+Sans+KR:wght@300;400;500;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
```

### 폰트 사이즈 스케일
```
로고/대제목:    Bebas Neue  3rem      letter-spacing: 3px
섹션 헤더:     Bebas Neue  1.8rem    letter-spacing: 2px
선수명:        Noto Sans   1rem      weight: 700
서브텍스트:    Noto Sans   0.72–0.82rem  weight: 300–400
레이블/배지:   JetBrains   0.62–0.72rem  letter-spacing: 1–2px
```

---

## 4. 컴포넌트 라이브러리

### 4-1. 헤더 (공통)
```html
<header> <!-- sticky, backdrop-blur: 14px, bg: rgba(10,13,18,0.94) -->
  <div class="logo">
    <span class="logo-k">K</span>          <!-- Bebas Neue, --accent -->
    <span class="logo-rest">LEAGUE STATS</span>  <!-- Bebas Neue, --text, opacity 0.82 -->
  </div>
  <span class="season-badge">2024 SEASON</span>  <!-- JetBrains Mono, border, --muted2 -->
</header>
```

### 4-2. 탭 버튼
- 기본: `background: none`, `color: --muted`, `border-bottom: 2px solid transparent`
- 활성: `color: --accent`, `border-bottom-color: --accent`
- 폰트: JetBrains Mono, 0.7rem, uppercase, letter-spacing: 1.5px

### 4-3. 선수 비교 바 (Squawka 스타일)
- 바 높이: `3px`, border-radius: `2px`
- 최고값: 숫자 크기 `1.35rem`, 해당 선수 색
- 나머지: `1.2rem`, `--text` 색
- 최고 배지: `★ 최고` 텍스트, JetBrains Mono 0.56rem

### 4-4. 레이더 차트 (Canvas 기반)
- 배경: `#0a0d12`, 그리드선: `#1a2030` / `#2a3340`
- 각 선수 fill: 해당 색 `alpha 0.13`
- 선 두께: 2px, 도트 반경: 4px, 외곽선: `#0a0d12` 1.5px
- 스포크 레이블: Noto Sans KR 11px, `--muted2`

### 4-5. 스탯 칩 필터
- 기본: `border: 1px solid --border`, `color: --muted2`, `border-radius: 2px`
- 선택: `border-color: --accent`, `color: --accent`, `bg: rgba(232,255,60,0.07)`
- 폰트: JetBrains Mono 0.67rem

### 4-6. 소셜 카드 (모바일 export용)
> 별도 섹션 참조

---

## 5. 구현 우선순위 & 페이지 목록

### Priority 1: 선수 비교 매트릭스 ✅ (v2 완성)
- 파일: `demo html/kleague-comparison-v2.html`
- 기능: 3인 비교, 표/레이더 전환, 카테고리 탭, 스탯 칩 필터
- **다음 단계**: DB 연동 (정적 DATA 객체 → API fetch)

**DB 연동 계획:**
```javascript
// 현재 (정적)
const DATA = { attack: { rows: [...] } }

// 목표 (API)
const res = await fetch('/api/compare?players=조규성,주민규,이승우&season=2024')
const DATA = await res.json()
```

### Priority 2: 소셜 미디어 카드 (미구현)
- 타깃: 인스타그램 1080×1080 / 1080×1350
- 스타일 레퍼런스: DATAMB.FOOTBALL (다크, 심플, 큰 숫자)
- 출력: PNG export (html2canvas 또는 Puppeteer)
- 컴포넌트 유형:
  - 선수 주간 성적 카드
  - 팀 경기 결과 요약 카드
  - 리그 순위표 카드

**소셜 카드 컬러 (라이트모드 옵션):**
```css
/* 소셜 카드 전용 — 인스타 가독성 */
--sc-bg:     #0a0d12;    /* 다크 유지 (DATAMB 스타일) */
--sc-accent: #e8ff3c;    /* K리그 라임 */
--sc-text:   #ffffff;
```

### Priority 3: 팀/선수 스타일 지표 차트 (미구현)
- 레퍼런스: PSG 스타일 슬라이더 차트 (Sofascore 스타일)
- 스펙: 수평 점/슬라이더 배치, 양 끝 레이블, 중앙값 기준
- K리그 적용: Patient↔Direct, Press Less↔Press More 등

---

## 6. 시각화 패턴 (레퍼런스 분석)

### 레퍼런스 1: PSG 스타일 슬라이더 (Sofascore)
```
특징:
- 화이트 배경
- 점(dot) + 라인 슬라이더
- 양 극단 텍스트 레이블
- 섹션 헤더: 검정 박스 안 흰 텍스트 (STYLE / PERFORMANCE)
- 10점 척도
K리그 적용시: 배경 --bg, 점 색상 --accent, 라인 --border
```

### 레퍼런스 2: 산점도 (DatoBHJ 스타일)
```
특징:
- 다크 네이비 배경
- 티얼(teal)/민트 색 도트
- 점선 중앙값 기준선
- 선수명 레이블 (각 도트 옆)
- X축: 패스/90, Y축: 키패스/90
K리그 적용시: 도트색 --p1 (라임), 중앙선 --border, 레이블 --muted2
```

### 레퍼런스 3: Squawka Comparison Matrix
```
특징:
- 퍼플 배경 + 선수 카드 헤더
- 가로 바 차트 (녹색=최고, 적색=아님)
- Total / Per90 토글
- 최대 4명 비교
- Add Player 빈 슬롯
K리그 적용: 퍼플 → --bg 다크, 녹색/적색 → --accent/--muted
```

---

## 7. 레이아웃 그리드

```
데스크탑: max-width 1320px, padding 36px 40px
모바일:   padding 16px 20px
그리드:   CSS Grid, 3열 (선수 비교 기본)
갭:      16–28px
```

---

## 8. 데이터 연결 전략

### 현재 상태
- 모든 페이지: 하드코딩된 정적 데이터

### 단계별 연동 계획
```
Phase 1 (현재): 정적 HTML + 더미 데이터
Phase 2:        FastAPI 백엔드 → /api/players, /api/compare 엔드포인트
Phase 3:        PostgreSQL 연동 (기존 DB 스키마 활용)
Phase 4:        자동 업데이트 (크롤링 → DB → API → 웹)
```

### API 엔드포인트 설계 (예정)
```
GET /api/players?season=2024&position=FW
GET /api/compare?ids=1,2,3&season=2024&mode=per90
GET /api/team/{team_id}/style?season=2024
GET /api/social-card/{player_id}?type=weekly
```

---

## 9. 배포 환경

- **목표**: 외부 공개 (Vercel 또는 GitHub Pages)
- **백엔드**: FastAPI → Vercel Serverless 또는 Railway
- **정적 에셋**: CDN 활용

---

## 10. 파일 구조 (예정)

```
app/
├── web/
│   ├── index.html              # 메인 랜딩
│   ├── comparison/
│   │   └── index.html          # 선수 비교 매트릭스
│   ├── social-card/
│   │   └── index.html          # 소셜 카드 생성기
│   ├── team-style/
│   │   └── index.html          # 팀 스타일 지표
│   └── assets/
│       ├── css/
│       │   └── design-system.css   # 공통 CSS 변수
│       └── js/
│           └── api.js              # fetch 헬퍼
demo html/
├── kleague-comparison-v2.html  # ✅ 완성 — 비교 매트릭스 프로토타입
└── (다음 파일들 추가 예정)
```

---

## 11. 서브에이전트 사용 지침

새 웹 페이지 구현 요청 시 Claude에게 전달할 컨텍스트:

```
1. 이 가이드(docs/WEBDESIGN_GUIDE.md)를 먼저 읽을 것
2. 기존 demo html/kleague-comparison-v2.html을 베이스 레퍼런스로 사용
3. 새 파일은 demo html/ 아래에 생성 (프로토타입 단계)
4. 외부 라이브러리 최소화 — 순수 HTML/CSS/JS 우선
5. 데이터는 정적 더미 데이터로 먼저 구현 후 API 연동
```

---

*최종 업데이트: 2026-03-15*
*담당: K리그 데이터랩 프로젝트*
