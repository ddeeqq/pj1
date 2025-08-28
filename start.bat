@echo off
chcp 65001
echo.
echo =================================================
echo    🚗 중고차 vs 신차 가성비 분석 시스템
echo =================================================
echo.

:menu
echo 📋 실행 옵션을 선택하세요:
echo.
echo 1. 🌐 Streamlit 웹앱 실행
echo 2. 🔧 데이터베이스 초기화
echo 3. 🕷️ 데이터 크롤링 실행
echo 4. ⏰ 스케줄러 시작
echo 5. 🧪 시스템 테스트
echo 6. 📊 샘플 데이터 생성
echo 7. 📈 일일 리포트 생성
echo 8. 🧹 데이터 정리
echo 9. ❌ 종료
echo.

set /p choice="선택 (1-9): "

if "%choice%"=="1" goto run_app
if "%choice%"=="2" goto init_db
if "%choice%"=="3" goto crawl_data
if "%choice%"=="4" goto start_scheduler
if "%choice%"=="5" goto test_system
if "%choice%"=="6" goto init_sample_data
if "%choice%"=="7" goto daily_report
if "%choice%"=="8" goto cleanup_data
if "%choice%"=="9" goto exit
goto menu

:run_app
echo 🌐 Streamlit 웹앱을 시작합니다...
python run.py run
pause
goto menu

:init_db
echo 🔧 데이터베이스를 초기화합니다...
python run.py init
pause
goto menu

:crawl_data
echo 🕷️ 데이터 크롤링을 시작합니다...
python run.py crawl
pause
goto menu

:start_scheduler
echo ⏰ 스케줄러를 시작합니다...
echo 종료하려면 Ctrl+C를 누르세요.
python scheduler_enhanced.py --config config/scheduler_config.json
pause
goto menu

:test_system
echo 🧪 시스템 테스트를 실행합니다...
python run.py test
python scheduler_enhanced.py --test
pause
goto menu

:init_sample_data
echo 📊 샘플 데이터를 생성합니다...
python init_data.py
pause
goto menu

:daily_report
echo 📈 일일 리포트를 생성합니다...
python scheduler_enhanced.py --task report
pause
goto menu

:cleanup_data
echo 🧹 데이터 정리를 시작합니다...
python scheduler_enhanced.py --task cleanup
pause
goto menu

:exit
echo 👋 프로그램을 종료합니다.
exit /b 0
