@echo off
echo ==========================================
echo      Mikon AI Scout - Data Update Tool
echo ==========================================
echo.
echo [1/6] Scraping latest TSA passenger data...
python -m src.etl.build_tsa_db
if %errorlevel% neq 0 (
    echo Error: TSA scraping failed!
    pause
    exit /b %errorlevel%
)

echo.
echo [2/6] Syncing recent flight data (OpenSky)...
python -m src.etl.fetch_opensky --recent
if %errorlevel% neq 0 (
    echo Error: Flight data fetching failed!
    pause
    exit /b %errorlevel%
)

echo.
echo [3/6] Fetching historical weather data...
python -m src.etl.get_weather_features
if %errorlevel% neq 0 (
    echo Error: Weather fetching failed!
    pause
    exit /b %errorlevel%
)

echo.
echo [4/6] Merging datasets into traffic_full...
python -m src.etl.merge_db
if %errorlevel% neq 0 (
    echo Error: Database merge failed!
    pause
    exit /b %errorlevel%
)

echo.
echo [5/6] Training Prophet Model (Trend Analysis)...
python -m src.models.train_prophet

echo.
echo [6/6] Training XGBoost Model (Precision Forecast)...
python -m src.models.train_xgb

echo.
echo ==========================================
echo        All Data Updated Successfully!
echo ==========================================
echo.
pause
