@echo off
chcp 65001 >nul
REM ============================================================
REM  K리그 데이터랩 - 비교 시안 로컬 데모 실행기
REM  더블클릭하면: 프로젝트 폴더 기준 정적 서버(8770) 실행 + 브라우저 오픈
REM  종료: 새로 뜨는 "KLeague Demo Server" 창을 닫으면 됨
REM ============================================================

cd /d "%~dp0"

set PORT=8770
set BASE=http://localhost:%PORT%/demo%%20html

echo.
echo  K-League DataLab demo
echo  ------------------------------------------------
echo  root : %cd%
echo  url  : %BASE%/kleague-comparison-v2-barchart.html
echo         %BASE%/kleague-comparison-v1-heatmap.html
echo  ------------------------------------------------
echo.

REM 1) 정적 서버를 별도 창에서 실행 (창 닫으면 종료)
start "KLeague Demo Server" cmd /k "cd /d "%~dp0" && python -m http.server %PORT%"

REM 2) 서버가 뜰 시간을 잠깐 기다린 뒤 브라우저 오픈
timeout /t 2 /nobreak >nul
start "" "%BASE%/kleague-comparison-v2-barchart.html"

exit /b 0
