# K리그 데이터랩 웹 디자인 가이드

> 이 문서는 세션 간 디자인 일관성 유지를 위한 **단일 진실 공급원(Single Source of Truth)**입니다.
> 새 웹 페이지/컴포넌트를 만들 때 반드시 이 가이드를 따르세요.

---

## 1. 디자인 철학

| 키워드 | 설명 |
|--------|------|
| **데이터 퍼스트** | 시각화가 주인공, UI는 배경 |
| **모드 일관성** | 다크 / 라이트 두 모드만 허용, 혼용 금지 |
| **정보 밀도** | 지표를 최대한 표시하되 가독성 유지 |
| **소셜 친화** | 인스타/X 공유용 카드는 1080×1080 or 1080×1350 기준 |
| **K리그 아이덴티티** | 한글 폰트 필수. 색상·로고·브랜드 요소는 추후 확정 예정 → [섹션 9] |

---

## 2. 컬러 시스템 — 모드 제약

### 원칙
- **허용 모드: 다크(Dark) / 라이트(Light) 두 가지만**
- 모드 내 accent, card, border 등 구체적 hex 값은 **레퍼런스에 따라 자유**
- 다크와 라이트를 한 페이지에 혼용하는 것은 **금지**

### 다크 모드 제약
```
배경:   어두운 계열 필수 (명도 기준 HSL Lightness ≤ 15%)
본문:   밝은 텍스트 (HSL Lightness ≥ 85%)
카드:   배경보다 밝고, 배경보다 어두운 사이 (중간 레이어)
구분선: 반투명 흰색 계열 (rgba(255,255,255, 0.05~0.15))
```

### 라이트 모드 제약
```
배경:   밝은 계열 필수 (HSL Lightness ≥ 92%)
본문:   어두운 텍스트 (HSL Lightness ≤ 20%)
카드:   흰색 또는 배경보다 밝은 색
구분선: 연한 회색 계열 (rgba(0,0,0, 0.05~0.12))
```

### 공통 제약
- accent 색은 모드 내 배경과 **대비비 4.5:1 이상** 확보
- 선수 구분 색(P1/P2/P3)은 서로 충분히 구별 가능해야 함
- 색맹 접근성: 빨강-초록 단독 구분 지양 (아이콘/패턴 병행)

---

## 3. 타이포그래피

### 필수 (모든 페이지 공통)
| 역할 | 폰트 | 이유 |
|------|------|------|
| **한글 본문** | `Noto Sans KR` | 선수명·팀명·설명 — 한글 필수 |

### 권장 (레퍼런스에 따라 교체 가능)
| 역할 | 기본 권장 | 대안 예시 |
|------|-----------|-----------|
| **디스플레이/대제목** | `Bebas Neue` | `Anton`, `Black Han Sans`, `Oswald` |
| **데이터/수치** | `JetBrains Mono` | `Roboto Mono`, `IBM Plex Mono` |

```html
<!-- 최소 임포트 (한글 필수) -->
<link href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700&display=swap" rel="stylesheet">
```

### 폰트 사이즈 원칙
```
대제목:     2rem 이상
섹션 헤더:  1.4~1.8rem
선수명:     0.9~1.1rem, weight 700
서브텍스트: 0.7~0.85rem, weight 300~400
레이블:     0.6~0.75rem, letter-spacing 1px 이상
```

---

## 4. 레이아웃 그리드

```
데스크탑: max-width 1320px, padding 36px 40px
태블릿:   max-width 960px,  padding 24px 28px
모바일:   padding 16px 20px (모바일 퍼스트 — 전체 트래픽 78% 이상)
그리드:   CSS Grid 권장, 선수 비교 기본 3열
갭:       16~28px
```

---

## 5. 컴포넌트 공통 규칙

> 구체적 색상 값은 버전마다 달라질 수 있으므로 아래는 **구조·동작 규칙**만 정의한다.

### 5-1. 헤더
- sticky + backdrop-blur 14px
- 로고: 브랜드 문자 "K" 강조 + "LEAGUE STATS" 서브텍스트
- 우측: 시즌 배지 (JetBrains Mono 계열, letter-spacing)

### 5-2. 탭 버튼
- 기본: 색 없음, muted 텍스트
- 활성: accent 색 텍스트 + 하단 border 2~3px

### 5-3. 선수 비교 바
- 바 높이: 3~10px, border-radius 100px (pill)
- 최고값: 숫자 강조 + 배지 (★ 최고 또는 동등 표기)
- 바 색상: 선수별 구분색, 값 크기에 따른 그라디에이션 허용

### 5-4. 레이더 차트
- 폴리곤 fill: 선수 색 alpha 0.10~0.15
- 선 두께: 2px, 꼭짓점 도트 반경 4px
- 그리드선: 모드에 맞는 미묘한 대비

### 5-5. 필터 (칩 또는 드롭다운)
- 칩: 기본 border → 선택 시 accent 색 border + bg tint
- 드롭다운: 라운드 border, hover 시 accent 색 border

### 5-6. 소셜 카드 (모바일 export용)
- 사이즈: 1080×1080 (정방형) 또는 1080×1350 (세로형)
- PNG export: html2canvas 또는 Puppeteer
- 다크 모드 권장 (DATAMB 스타일 레퍼런스)

---

## 6. 산출물 규칙

### HTML 파일
- 저장 위치: `demo html/` 하위
- 컴포넌트 경계 주석 필수:
  ```html
  <!-- COMPONENT: PlayerCard -->
  ...
  <!-- /COMPONENT: PlayerCard -->
  ```
- 파일 상단 버전 헤더 필수:
  ```html
  <!-- v{X.Y} | {날짜} | 레퍼런스: {출처/키워드} | 변경: {요약} -->
  ```

### 단계별 전환 순서
```
1단계: HTML 정적 데모 (현재)   — demo html/ 저장
2단계: React 컴포넌트          — 컴포넌트 주석 경계 기준으로 분리
3단계: Streamlit 배포          — st.columns + plotly
```

---

## 7. 구현 우선순위 & 페이지 목록

### Priority 1: 선수 비교 매트릭스
- ✅ `demo html/kleague-comparison-v2.html` — 다크 테마
- ✅ `demo html/kleague-comparison-v3-datamb.html` — 라이트 테마
- **다음 단계**: DB 연동 (정적 DATA 객체 → API fetch)

### Priority 2: 선수 랭킹 차트
- ✅ `demo html/kleague-datamb-ranking-v1.html` — 다크 테마
- ✅ `demo html/kleague-datamb-ranking-v2.html` — 라이트 테마
- **다음 단계**: 지표 드롭다운 실데이터 연결

### Priority 3: 소셜 미디어 카드 (미구현)
- 타깃: 인스타그램 1080×1080 / 1080×1350
- 컴포넌트: 선수 주간 성적 카드, 팀 경기 결과, 리그 순위표

### Priority 4: 팀/선수 스타일 지표 차트 (미구현)
- 레퍼런스: PSG 스타일 슬라이더 차트 (Sofascore)

---

## 8. 데이터 연결 전략

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

## 9. K리그 아이덴티티 (미확정 — 추후 작성)

> POC 단계에서는 색상·로고·브랜드 요소를 고정하지 않는다.
> 디자인 방향 확정 후 아래 항목을 채운다.

```
[ ] 공식 대표 색상 (Primary / Secondary)
[ ] 공식 폰트 또는 K리그 느낌 폰트
[ ] 로고 사용 규칙
[ ] 팀 엠블럼 처리 방식
[ ] K리그 특화 UI 패턴
```

---

## 10. 배포 환경

- **GitHub Pages**: `demo html/` → gh-pages 브랜치 자동 배포
- **배포 URL**: `https://gyu-yeong.github.io/B_SOCCER_DATALAB/`
- **최종 목표**: Vercel 또는 Railway (FastAPI 백엔드 포함)

---

*최종 업데이트: 2026-03-21*
*담당: K리그 데이터랩 프로젝트*
