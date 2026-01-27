@echo off
chcp 65001
echo ==================================================
echo       Mikon AI Scout - 启动脚本
echo ==================================================
echo.
echo [1/2] 正在后台启动服务器 (Flask App)...
cd /d %~dp0\..
start /min python app.py

echo.
echo [2/2] 等待服务就绪并打开浏览器...
timeout /t 5 >nul
start http://localhost:5001

echo.
echo ✅ 启动完成! 请在浏览器中查看.
echo (如果是第一次运行, 可能需要稍微多等几秒刷新页面)
timeout /t 3 >nul
exit
