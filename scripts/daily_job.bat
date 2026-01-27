@echo off
chcp 65001
cd /d %~dp0\..
echo =======================================================
echo          Mikon AI Scout - 每日自动更新脚本
echo =======================================================
echo.

set PYTHON_EXEC=python

echo [1/6] 下拉最新 TSA 旅客吞吐量数据...
%PYTHON_EXEC% src/etl/build_tsa_db.py --latest
if %errorlevel% neq 0 (
    echo [ERROR] TSA 数据抓取失败!
    goto :error
)
echo [OK] TSA 数据更新完成.
echo.

echo [2/6] (Skipped) OpenSky Flight Data...
echo.

echo [3/6] 更新天气预报骨架 (Weather Forecast)...
%PYTHON_EXEC% src/etl/get_weather_features.py
if %errorlevel% neq 0 (
    echo [ERROR] 天气数据抓取失败!
    goto :error
)
echo [OK] 天气骨架更新完成.
echo.

echo [4/6] 抓取 Polymarket 预测盘口 (Sentiment)...
%PYTHON_EXEC% src/etl/fetch_polymarket.py
if %errorlevel% neq 0 (
    echo [ERROR] Polymarket 数据抓取失败!
    goto :error
)
echo [OK] 市场数据更新完成.
echo.

echo [5/6] 执行数据库合并 (Robust Merge)...
%PYTHON_EXEC% src/etl/merge_db.py
if %errorlevel% neq 0 (
    echo [ERROR] 数据库合并失败!
    goto :error
)
echo [OK] 数据库合并完成.
echo.

echo [6/6] 重新训练预测模型 (XGBoost)...
%PYTHON_EXEC% src/models/train_xgb.py
if %errorlevel% neq 0 (
    echo [ERROR] 模型训练失败!
    goto :error
)
echo [OK] 模型更新完成.
echo.

echo =======================================================
echo      ✅  全流程执行成功! 数据已是最新状态.
echo =======================================================
timeout /t 10
exit /b 0

:error
echo.
echo =======================================================
echo      ❌  任务执行失败，请检查上方错误信息.
echo =======================================================
pause
exit /b 1
