@echo off
echo ==========================================
echo      Mikon AI Scout - Data Update Tool
echo ==========================================
echo.
echo [1/3] Scraping latest TSA passenger data...
python build_tsa_db.py
if %errorlevel% neq 0 (
    echo Error: TSA scraping failed!
    pause
    exit /b %errorlevel%
)

echo.
echo [2/3] Fetching historical weather data...
python get_weather_features.py
if %errorlevel% neq 0 (
    echo Error: Weather fetching failed!
    pause
    exit /b %errorlevel%
)

echo.
echo [3/3] Merging datasets into traffic_full...
python merge_db.py
if %errorlevel% neq 0 (
    echo Error: Database merge failed!
    pause
    exit /b %errorlevel%
)

echo.
echo [4/5] Training Prophet Model (Trend Analysis)...
python train_model.py

echo.
echo [5/5] Training XGBoost Model (Precision Forecast)...
python train_xgb.py

echo.
echo ==========================================
echo        All Data Updated Successfully!
echo ==========================================
echo.
pause
