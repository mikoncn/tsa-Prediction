console.log("--- 指挥中心 V2.0 脚本已装载 ---");
let chart;
let allData = [];
const availableYears = new Set();
let currentRawOffset = 0;
let isFirstLoadRaw = true;

// 初始化 Chart.js 图表
function initChart() {
    const ctx = document.getElementById('trafficChart').getContext('2d');
    chart = new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [{
                label: '历史旅客吞吐量',
                data: [],
                borderColor: '#007bff',
                backgroundColor: 'rgba(0, 123, 255, 0.1)',
                borderWidth: 2,
                fill: true,
                tension: 0.3,
                // 根据气象指数动态改变数据点颜色
                pointBackgroundColor: function(context) {
                    const idx = context.dataIndex;
                    const item = context.dataset.data[idx];
                    if (!item || item.y === null) return 'transparent';
                    const w = item.weather_index;
                    if (w >= 30) return '#dc3545'; // 极端天气(红)
                    if (w >= 15) return '#fd7e14'; // 恶劣天气(橙)
                    return '#007bff';
                },
                // [FIX] User requested strict hover (radius 1-3)
                pointRadius: 3, 
                pointHoverRadius: 5,
            }, {
                label: 'AI 预测',
                data: [],
                borderColor: '#fd7e14',
                backgroundColor: 'rgba(253, 126, 20, 0.1)',
                borderWidth: 2,
                borderDash: [5, 5],
                pointRadius: 3, 
                pointHoverRadius: 5,
                pointBackgroundColor: '#fd7e14',
                fill: false,
                tension: 0.3
            }, {
                // [NEW] Challenger Line
                label: 'FLAML 挑战者 (Challenger)',
                data: [],
                borderColor: '#6f42c1', // Purple
                backgroundColor: 'rgba(111, 66, 193, 0.1)',
                borderWidth: 2,
                pointRadius: 3,
                borderDash: [2, 2],
                tension: 0.4,
                hidden: true // Default hidden
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'nearest',       // [FIX] Switch back to nearest point only
                intersect: true,       // [FIX] Require exact intersection (hovering over the point)
                axis: 'xy'             // [FIX] Consider both axes for distance
            },
            onClick: (e) => {
                const points = chart.getElementsAtEventForMode(e, 'nearest', { intersect: true }, true);
                if (points.length) {
                    const firstPoint = points[0];
                    const dataset = chart.data.datasets[firstPoint.datasetIndex];
                    const item = dataset.data[firstPoint.index];
                    if(item && item.y !== null) {
                        alert(`日期: ${item.x}\n客流: ${item.y}\n气象指数: ${item.weather_index || 0}\n节日: ${item.holiday_name || '无'}`);
                    }
                }
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        displayFormats: {
                            day: 'MM-dd',
                            week: 'MM-dd',
                            month: 'yyyy-MM'
                        },
                        tooltipFormat: 'yyyy-MM-dd'
                    },
                    ticks: {
                        maxRotation: 45,
                        minRotation: 0,
                        autoSkip: true,
                        autoSkipPadding: 15,
                        font: { size: 10 }
                    },
                    title: { display: true, text: '日期' }
                },
                y: {
                    title: { display: true, text: '人次' },
                    ticks: {
                        callback: function(value) {
                            return (value / 1000000).toFixed(1) + 'M';
                        }
                    }
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        title: function(context) {
                            const date = new Date(context[0].parsed.x);
                            return new Intl.DateTimeFormat('zh-CN', { 
                                year: 'numeric', month: '2-digit', day: '2-digit', weekday: 'short' 
                            }).format(date);
                        },
                        label: function(context) {
                            const item = context.raw;
                            if (item.y === null) return ' 预测中...';
                            return ' ' + context.dataset.label + ': ' + new Intl.NumberFormat().format(item.y);
                        },
                        afterLabel: function(context) {
                            const item = context.raw;
                            let lines = [];
                            
                            // 1. Holiday Factor
                            if (item.holiday_name) {
                                lines.push(' 🎉 节日: ' + item.holiday_name);
                            } else if (item.is_holiday === 1) {
                                lines.push(' 🎉 节日因子: 命中');
                            }
                            
                            // 2. Weather Factor
                            if (item.weather_index > 0) {
                                let weatherInfo = ` ⛈️ 气象指数: ${item.weather_index}`;
                                if (item.weather_index >= 30) weatherInfo += ' (⚠️ 系统熔断)';
                                else if (item.weather_index >= 15) weatherInfo += ' (⚠️ 恶劣天气)';
                                lines.push(weatherInfo);
                            }
                            
                            // 3. Flight Volume Factor (New)
                            if (item.flight_volume > 0) {
                                lines.push(` ✈️ 航班因子: ${item.flight_volume.toLocaleString()} 架次`);
                            }
                            
                            return lines;
                        }
                    }
                },
                annotation: {
                    annotations: {}
                },
                zoom: {
                    pan: { enabled: true, mode: 'x' },
                    zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'x' }
                }
            }
        }
    });
}

// 加载数据
// 从后端 API 加载历史数据
async function loadData() {
    try {
        const response = await fetch('/api/data');
        const data = await response.json();
        
        // [NEW] Populate Holiday Index for Client-side Calculation
        // Filter for actual holidays. 
        // Note: Sometimes a holiday might be merged with "Travel Window" string (e.g. "Travel Window, New Year's Day").
        // We should KEEP it if it contains a major holiday name, even if it has "Travel Window".
        window.availableHolidayDates = data
            .filter(d => d.is_holiday === 1 && d.holiday_name)
            .filter(d => {
                // If it's PURELY a window/week tag, exclude it.
                // But if it contains "Day", "Eve", or "Thanksgiving" (Major Holiday), keep it.
                if (d.holiday_name.includes('Travel Window') || d.holiday_name.includes('出行周')) {
                    return d.holiday_name.includes('Day') || d.holiday_name.includes('Eve') || d.holiday_name.includes('Thanksgiving') || d.holiday_name.includes('New Year');
                }
                return true;
            })
            .map(d => ({ date: d.date, name: d.holiday_name }));
        
        // 格式化数据供 Chart.js 使用
        allData = data.map(item => ({
            x: item.date,
            y: item.throughput,
            weather_index: item.weather_index || 0,
            is_holiday: item.is_holiday || 0,
            holiday_name: item.holiday_name || ''
        }));

        // 更新年份筛选器
        availableYears.clear();
        allData.forEach(item => {
            const year = item.x.split('-')[0];
            availableYears.add(year);
        });
        populateYearSelect();

        updateZoomLimits();    // 更新缩放限制
        setQuickRange(30);     // 默认显示最近30天
        await fetchPredictions(); // [FIX] Await to ensure future holidays are loaded before Table Render
        
        // Trigger Raw Data Load (Default Page 1) if not already triggered by setQuickRange?
        // Actually setQuickRange just sets Chart zoom. 
        // We explicitly call loadRawData(true) to ensure it resets and uses the latest merged data.
        loadRawData(true);
        
    } catch (error) {
        console.error('Error loading data:', error);
        alert('无法连接到后端服务，请确保 app.py 已启动');
    }
}

let forecastDataMap = {};

async function fetchPredictions() {
    try {
        const response = await fetch('/api/predictions');
        const data = await response.json();
        
        // [FIX] Merge History and Forecast for Chart
        let combinedPredictions = [];
        
        // 1. Add History (Past Predictions)
        if (data.history && data.history.length > 0) {
            data.history.forEach(item => {
                combinedPredictions.push({
                    x: item.date,
                    y: item.predicted,
                    weather_index: item.weather_index,
                    is_holiday: item.is_holiday,
                    flight_volume: item.flight_volume
                });
            });
        }
        
        // 2. Add Forecast (Future Predictions)
        // 2. Add Forecast (Future Predictions)
        if (data.forecast && data.forecast.length > 0) {
            // [NEW] Add Future Holidays to Global Index
            // Applying same robust filter as loadData
            const futureHolidays = data.forecast
                .filter(d => d.is_holiday === 1 && d.holiday_name)
                .filter(d => {
                    if (d.holiday_name.includes('Travel Window') || d.holiday_name.includes('出行周')) {
                        return d.holiday_name.includes('Day') || d.holiday_name.includes('Eve') || d.holiday_name.includes('Thanksgiving') || d.holiday_name.includes('New Year');
                    }
                    return true;
                })
                .map(d => ({ date: d.ds, name: d.holiday_name })); // Note: forecast uses 'ds', not 'date'
            
            console.log("Future Holidays Found in Forecast:", futureHolidays);

            // Merge unique
            futureHolidays.forEach(fh => {
               // Ensure window.availableHolidayDates exists
               if (!window.availableHolidayDates) window.availableHolidayDates = [];
               
               if (!window.availableHolidayDates.some(existing => existing.date === fh.date)) {
                   console.log("Merging Future Holiday:", fh);
                   window.availableHolidayDates.push(fh);
               }
            });
            console.log("Final Holiday Index:", window.availableHolidayDates);

            data.forecast.forEach(item => {
                // Avoid duplicates if forecast overlaps with history (though logic should prevent it)
                if (!combinedPredictions.some(p => p.x === item.ds)) {
                    combinedPredictions.push({
                        x: item.ds,
                        y: item.predicted_throughput,
                        weather_index: item.weather_index,
                        is_holiday: item.is_holiday,
                        flight_volume: item.flight_volume
                    });
                }
            });
            
            // Update dropdown (only for Future Forecast)
            const select = document.getElementById('predDateSelect');
            select.innerHTML = '';
            forecastDataMap = {};
            
            data.forecast.forEach((item) => {
                const opt = document.createElement('option');
                opt.value = item.ds;
                const d = new Date(item.ds);
                const dayName = d.toLocaleDateString('en-US', { weekday: 'short' });
                opt.text = `${item.ds.slice(5)} (${dayName})`;
                select.add(opt);
                forecastDataMap[item.ds] = item.predicted_throughput;
            });
            
            select.selectedIndex = 0;
            updatePredictionDisplay(select.value);
            select.onchange = function() { updatePredictionDisplay(this.value); };
        } else {
             document.getElementById('predPassengers').innerText = '-';
        }

        // 3. Sort by date and update Chart
        combinedPredictions.sort((a, b) => new Date(a.x) - new Date(b.x));
        chart.data.datasets[1].data = combinedPredictions;
        chart.update();


        if (data.validation && data.validation.length > 0) {
            const tableBody = document.querySelector('#accuracyTable tbody');
            tableBody.innerHTML = '';
            
            const recentValidation = data.validation.reverse();
            
            recentValidation.forEach(row => {
                const tr = document.createElement('tr');
                tr.style.borderBottom = '1px solid #eee';
                
                const errorRate = parseFloat(row.error_rate);
                let badgeText = '✅ 优秀';
                
                if (errorRate > 8.0) {
                    badgeText = '🔴 偏差大';
                    tr.style.backgroundColor = '#fff5f5';
                } else if (errorRate > 5.0) {
                    badgeText = '⚠️ 一般';
                    tr.style.backgroundColor = '#fffdf5';
                }

                tr.innerHTML = `
                    <td style="padding: 10px;">${row.date}</td>
                    <td style="padding: 10px; text-align: right;">${parseInt(row.actual).toLocaleString()}</td>
                    <td style="padding: 10px; text-align: right; font-weight: bold; color: #007bff;">${parseInt(row.predicted).toLocaleString()}</td>
                    <td style="padding: 10px; text-align: right;">${parseInt(row.difference).toLocaleString()}</td>
                    <td style="padding: 10px; text-align: center;">${errorRate.toFixed(2)}% <span style="font-size: 0.8em; margin-left: 5px;">${badgeText}</span></td>
                `;
                tableBody.appendChild(tr);
            });
        }
    } catch (error) {
        console.error('Error fetching predictions:', error);
    }
}

async function runPrediction() {
    const btn = document.getElementById('btnRunPred');
    const originalText = btn.innerText;
    
    try {
        btn.innerText = '⏳ 计算中...';
        btn.disabled = true;
        
        const response = await fetch('/api/run_prediction', { method: 'POST' });
        const result = await response.json();
        
        if (result.status === 'success') {
            alert('✅ 预测完成！数据已更新。');
            fetchPredictions();
        } else {
            alert('❌ 失败: ' + result.message);
        }
    } catch (e) {
        alert('❌ 请求错误: ' + e);
    } finally {
        btn.innerText = originalText;
        btn.disabled = false;
    }
}

function updatePredictionDisplay(date) {
    if (forecastDataMap[date]) {
        const val = forecastDataMap[date];
        document.getElementById('predPassengers').innerText = (val / 1000000).toFixed(2) + 'M';
    } else {
        document.getElementById('predPassengers').innerText = '-';
    }
}

function populateYearSelect() {
    const yearSelect = document.getElementById('yearSelect');
    yearSelect.innerHTML = '<option value="all">全部年份</option>';
    const sortedYears = Array.from(availableYears).sort().reverse();
    sortedYears.forEach(year => {
        const option = document.createElement('option');
        option.value = year;
        option.text = year + '年';
        yearSelect.add(option);
    });
}

function applyFilters() {
    const selectedYear = document.getElementById('yearSelect').value;
    let filteredData = allData;
    
    if (selectedYear !== 'all') {
        filteredData = allData.filter(item => item.x.startsWith(selectedYear));
    }
    
    updateChart(filteredData);
    generateHolidayAnnotations(filteredData);
    updateStats(filteredData);
}

function setQuickRange(days) {
    if (!allData || allData.length === 0) return;
    
    updateChart(allData);
    
    const validData = allData.filter(d => d.y !== null);
    const lastDateObj = new Date(validData[validData.length - 1].x);
    
    const startDateObj = new Date(lastDateObj);
    startDateObj.setDate(lastDateObj.getDate() - days);
    
    const minTime = startDateObj.getTime();
    const maxTime = lastDateObj.getTime();
    
    chart.options.scales.x.min = minTime;
    chart.options.scales.x.max = maxTime;
    
    document.getElementById('yearSelect').value = 'all';
    chart.update();
    
    generateHolidayAnnotations(allData);
    updateStats(allData);
}

function updateChart(data) {
    chart.data.datasets[0].data = data;
    chart.update();
}

function updateZoomLimits() {
    if (!allData || allData.length === 0) return;
    
    const dates = allData.map(d => new Date(d.x).getTime());
    const minDate = Math.min(...dates);
    const maxDate = Math.max(...dates);
    const buffer = 7 * 24 * 60 * 60 * 1000;
    
    chart.options.plugins.zoom.limits = {
        x: { min: minDate, max: maxDate + buffer }
    };
    chart.update();
}

function generateHolidayAnnotations(data) {
    const annotations = {};
    let inHoliday = false;
    let startDate = null;
    let currentName = '';

    const sortedData = [...data].sort((a, b) => new Date(a.x) - new Date(b.x));

    sortedData.forEach((item, index) => {
        if (item.is_holiday === 1 && !inHoliday) {
            inHoliday = true;
            startDate = item.x;
            currentName = item.holiday_name;
        } else if ((item.is_holiday === 0 || item.holiday_name !== currentName) && inHoliday) {
            const endDate = sortedData[index - 1].x;
            const key = 'holiday_' + index;
            annotations[key] = {
                type: 'box',
                xMin: startDate,
                xMax: endDate,
                backgroundColor: 'rgba(153, 102, 255, 0.15)',
                borderWidth: 0,
                drawTime: 'beforeDatasetsDraw'
            };
            
            inHoliday = false;
            if (item.is_holiday === 1) {
                inHoliday = true;
                startDate = item.x;
                currentName = item.holiday_name;
            }
        }
    });

    if (inHoliday) {
        const endDate = sortedData[sortedData.length - 1].x;
        const key = 'holiday_last';
        annotations[key] = {
            type: 'box',
            xMin: startDate,
            xMax: endDate,
            backgroundColor: 'rgba(153, 102, 255, 0.15)',
            borderWidth: 0,
            drawTime: 'beforeDatasetsDraw'
        };
    }
    
    chart.options.plugins.annotation.annotations = annotations;
    chart.update();
}

function updateStats(data) {
    const validData = data.filter(item => item.y !== null);
    
    if (validData.length === 0) {
        document.getElementById('latestPassengers').innerText = '-';
        document.getElementById('prevPassengers').innerText = '-';
        return;
    }
    
    const latest = validData[validData.length - 1];
    document.getElementById('latestPassengers').innerText = (latest.y / 1000000).toFixed(2) + 'M';
    document.getElementById('latestDate').innerText = latest.x;

    if (validData.length >= 2) {
        const prev = validData[validData.length - 2];
        document.getElementById('prevPassengers').innerText = (prev.y / 1000000).toFixed(2) + 'M';
        document.getElementById('prevDate').innerText = prev.x;
    } else {
        document.getElementById('prevPassengers').innerText = '-';
        document.getElementById('prevDate').innerText = '';
    }
}

document.getElementById('yearSelect').addEventListener('change', applyFilters);

document.addEventListener('DOMContentLoaded', () => {
    initChart();
    loadData();
});

// 更新数据功能: 调用后端 API 抓取最新 TSA 数据并同步天气
async function updateData() {
    const btn = document.getElementById('btnUpdateData');
    const originalText = btn.innerText;
    
    try {
        // 更新按钮状态
        btn.disabled = true;
        btn.innerText = '⏳ 更新中...';
        btn.style.backgroundColor = '#6c757d';
        
        console.log('开始数据更新...');
        const response = await fetch('/api/update_data', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'}
        });
        
        const data = await response.json();
        
        if (response.ok && data.status === 'success') {
            alert('✅ ' + data.message);
            console.log('更新成功:', data.results);
            // 重新加载数据并刷新图表
            await loadData();
        } else {
            const errorMsg = data.message || '更新失败';
            alert('❌ ' + errorMsg);
            console.error('更新失败:', data);
        }
    } catch (error) {
        console.error('更新错误:', error);
        alert('❌ 网络错误，请检查后端服务');
    } finally {
        // 恢复按钮状态
        btn.disabled = false;
        btn.innerText = originalText;
        btn.style.backgroundColor = '#17a2b8';
    }
}

// 狙击模型调用
async function runSniperModel() {
    const btn = document.getElementById('btnRunSniper');
    const originalText = btn.innerText;
    
    try {
        btn.disabled = true;
        btn.innerText = '🎯 锁定中...';
        btn.style.backgroundColor = '#a71d2a';
        
        const response = await fetch('/api/predict_sniper', { method: 'POST' });
        const result = await response.json();
        
        if (result.status === 'success') {
            const data = result.data;
            const modal = document.getElementById('sniperModal');
            
            // Populate Modal
            document.getElementById('sniperDate').innerText = data.date;
            document.getElementById('sniperValue').innerText = (data.predicted_throughput / 1000000).toFixed(2) + 'M';
            document.getElementById('sniperFlights').innerText = data.flight_volume.toLocaleString();
            
            const badge = document.getElementById('sniperBadge');
            if (data.is_fallback) {
                badge.style.backgroundColor = '#ffc107';
                badge.style.color = '#000';
                badge.innerText = '⚠️ 降级模式 (Fallback)';
            } else {
                badge.style.backgroundColor = '#28a745';
                badge.style.color = '#fff';
                badge.innerText = '✅ 实时同步 (High Precision)';
            }
            
            // Show
            modal.style.display = 'flex';
        } else {
            alert('❌ 狙击失败: ' + result.message);
        }
    } catch (e) {
        alert('❌ 网络错误: ' + e);
    } finally {
        btn.disabled = false;
        btn.innerText = originalText;
        btn.style.backgroundColor = '#dc3545';
    }
}

// [NEW] Run FLAML Challenger
window.runChallenger = async function() {
    const btn = document.getElementById('btnChallenger');
    const originalText = btn.innerText;
    
    // UI Loading State
    btn.disabled = true;
    btn.innerText = '⏳ 深度训练中 (约3分钟)...';
    btn.style.backgroundColor = '#5a32a3';
    
    try {
        const response = await fetch('/api/run_challenger', { method: 'POST' });
        const result = await response.json();
        
        if (result.status === 'success') {
            const forecast = result.data.forecast;
            const mape = (result.data.mape * 100).toFixed(2);
            const modelName = result.data.model.split('(')[0]; // Simplify name
            
            alert(`✅ 挑战成功！\n\n🏆 最佳模型: ${modelName}\n📉 验证误差: ${mape}%\n\n紫色曲线已绘制到图表中。`);
            
            // 绘制到图表 (Dataset Index 2)
            // 映射字段：如果是 forecast 模式，字段是 'forecast'；如果是 backtest，可能是 'predicted'
            // 后端统一为 'forecast'
            const challengerData = forecast.map(item => ({
                x: item.date,
                y: item.forecast || item.predicted // Fallback
            }));
            
            if (chart.data.datasets.length > 2) {
                const meta = chart.data.datasets[2];
                meta.data = challengerData;
                meta.hidden = false;
                
                // [FIX INTERACTION] Ensure it's interactive
                // Force dataset specific interactions if needed, but 'index' mode should work.
                // Reset to default style if previously hidden
                
                // [CRITICAL FIX] Extend X-axis to show future predictions
                const lastHistDate = new Date(chart.options.scales.x.max || Date.now());
                const lastForecastDate = new Date(challengerData[challengerData.length-1].x);
                
                if (lastForecastDate > lastHistDate) {
                     // Extend view to fit forecast + 1 day padding
                     const newMax = lastForecastDate.getTime() + (24 * 60 * 60 * 1000);
                     chart.options.scales.x.max = newMax;
                     
                     // Also update zoom limit
                     chart.options.plugins.zoom.limits.x.max = newMax + (7 * 24 * 60 * 60 * 1000);
                }

                chart.update();
            } else {
                 console.error("Chart dataset index 2 not found");
            }
            
        } else {
            alert('❌ 挑战者失败: ' + result.message);
        }
    } catch (error) {
        console.error('Challenger Error:', error);
        alert('系统错误: ' + error.message);
    } finally {
        btn.disabled = false;
        btn.innerText = originalText;
        btn.style.backgroundColor = '#6f42c1';
    }
};

// [ULTIMATE FIX] 一键导出预测数据 (Direct Link Strategy)
window.exportPredictions = function(event) {
    if (event) event.preventDefault();
    console.log("正在唤起指挥官确认弹窗...");
    const modal = document.getElementById('exportModal');
    if (modal) {
        modal.style.display = 'flex';
    } else {
        // 万一 Modal 丢失，直接触发下载
        const confirmBtn = document.getElementById('confirmExportBtn');
        if (confirmBtn && confirmBtn.onclick) {
            confirmBtn.onclick();
        } else {
            window.location.href = '/api/v2/secure_export?t=' + Date.now();
        }
    }
};

// 核心下载执行函数
// 直接绑定确认逻辑
(function bindExportConfirm() {
    const confirmBtn = document.getElementById('confirmExportBtn');
    if (confirmBtn) {
        confirmBtn.onclick = function() {
            console.log("执行物理层直连下载策略...");
            const originalText = confirmBtn.innerHTML;
            confirmBtn.innerHTML = '🚀 正在启动...';
            confirmBtn.disabled = true;

            try {
                // 采用最原始的 window.location 指向，给浏览器最清晰的头信息解析空间
                const downloadUrl = '/api/v2/secure_export?t=' + Date.now();
                window.location.href = downloadUrl;
                console.log("下载指令已直接赋予 window.location。");
            } catch (e) {
                console.error("下载启动失败:", e);
            }

            // 500ms 后恢复状态并关闭弹窗
            setTimeout(() => {
                const modal = document.getElementById('exportModal');
                if (modal) modal.style.display = 'none';
                confirmBtn.innerHTML = originalText;
                confirmBtn.disabled = false;
            }, 500); 
        };
    }
})();

// ==========================================
// [NEW] Raw Data Panel Logic (生数据 - Clean Light Mode)
// ==========================================

// ==========================================
// [NEW] Raw Data Panel Logic (生数据 - Clean Light Mode)
// ==========================================

// [NEW] Global Holiday Index for Client-side Window Calculation
window.availableHolidayDates = [];

// [NEW] Helper to calculate distance to nearest holiday
function getClientSideHolidayDistance(targetDateStr) {
    if (!window.availableHolidayDates || window.availableHolidayDates.length === 0) return null; // Use window var

    // [FIX] Use safe parsing to avoid Timezone offsets moving dates by 1 day
    function parseYMD(str) {
        const [y, m, d] = str.split('-').map(Number);
        return new Date(y, m - 1, d); // Local time 00:00:00
    }

    const target = parseYMD(targetDateStr);
    let minDiff = Infinity;
    let closestHoliday = null;

    availableHolidayDates.forEach(h => {
        const hDate = parseYMD(h.date);
        // Time difference in milliseconds
        const diffTime = target - hDate;
        // Round to nearest integer day
        const diffDays = Math.round(diffTime / (1000 * 60 * 60 * 24)); 
        
        if (Math.abs(diffDays) < Math.abs(minDiff)) {
            minDiff = diffDays;
            closestHoliday = h.name;
        }
    });

    // Window: +/- 4 days (User asked for T-3/T+3 but wider search safe)
    if (Math.abs(minDiff) <= 3 && minDiff !== 0) {
        return { dist: minDiff, name: closestHoliday };
    }
    return null;
}


let isFirstLoadPolymarket = true;

// Custom Tab Switcher (Original Style)
window.switchTab = function(tabName) {
    const tabVal = document.getElementById('tab-validation');
    const tabRaw = document.getElementById('tab-rawdata');
    const tabPoly = document.getElementById('tab-polymarket');
    
    const btnVal = document.getElementById('tab-btn-validation');
    const btnRaw = document.getElementById('tab-btn-rawdata');
    const btnPoly = document.getElementById('tab-btn-polymarket');
    const btnSync = document.getElementById('btnSyncSentiment');

    // Hide all
    tabVal.style.display = 'none';
    tabRaw.style.display = 'none';
    tabPoly.style.display = 'none';
    if(btnSync) btnSync.style.display = 'none';
    
    // Reset buttons (Opacity 0.5, Grey border)
    [btnVal, btnRaw, btnPoly].forEach(btn => {
        if(btn) {
            btn.style.opacity = '0.5';
            btn.style.borderColor = '#ccc';
        }
    });

    // Show target
    if (tabName === 'validation') {
        tabVal.style.display = 'block';
        btnVal.style.opacity = '1';
        btnVal.style.borderColor = '#28a745';
        
    } else if (tabName === 'rawdata') {
        tabRaw.style.display = 'block';
        btnRaw.style.opacity = '1';
        btnRaw.style.borderColor = '#17a2b8';
        if (isFirstLoadRaw) {
             window.loadRawData(true);
             isFirstLoadRaw = false;
        }
        
    } else if (tabName === 'polymarket') {
        tabPoly.style.display = 'block';
        btnPoly.style.opacity = '1';
        btnPoly.style.borderColor = '#6f42c1'; // Purple for Polymarket
        if(btnSync) btnSync.style.display = 'block';
        if (isFirstLoadPolymarket) {
            renderMarketSentiment();
            isFirstLoadPolymarket = false;
        }
    }
};

// Remove Bootstrap listener as we reverted to custom tabs
// document.addEventListener("DOMContentLoaded", ... ); 

// --- Polymarket Rendering Logic ---

async function syncSentiment() {
    const btn = document.getElementById('btnSyncSentiment');
    const originalText = btn.innerText;
    
    try {
        btn.innerText = '⏳';
        btn.disabled = true;
        
        // 1. 触发后台同步
        const res = await fetch('/api/sync_market_sentiment', { method: 'POST' });
        const result = await res.json();
        
        if (result.status === 'success') {
            // 2. 同步成功后局部刷新渲染
            await renderMarketSentiment();
            console.log("Sentiment Synced Successfully");
        } else {
            alert("同步失败: " + result.message);
        }
    } catch (e) {
        console.error(e);
        alert("同步请求出错");
    } finally {
        btn.innerText = originalText;
        btn.disabled = false;
    }
}

async function renderMarketSentiment() {
    const container = document.getElementById('polymarket-grid');
    if(!container) return;
    
    // 如果是第一次加载，显示 Loading
    if (container.innerHTML === '') {
        container.innerHTML = '<div style="text-align:center; padding:20px;">Fetching Market Data...</div>';
    }
    
    try {
        const res = await fetch('/api/market_sentiment');
        const data = await res.json();
        
        container.innerHTML = '';
        const dates = Object.keys(data).sort(); // Sort ASC to show main battleground (nearest dates) first
        
        if (dates.length === 0) {
            container.innerHTML = '<div style="text-align:center; padding:20px;">暂无活跃预测市场 (TSA 官网已悉数结算)</div>';
            return;
        }
        
        dates.forEach(date => {
            const items = data[date];
            const card = renderPolymarketCard(date, items);
            container.appendChild(card);
        });
        
    } catch(e) {
        console.error(e);
        container.innerHTML = `<div style="text-align:center; color:red;">Load Error: ${e.message}</div>`;
    }
}

function renderPolymarketCard(date, items) {
    const card = document.createElement('div');
    card.className = 'market-card';
    
    // 1. Header
    const dObj = new Date(date);
    const dateStr = `${dObj.getMonth()+1}月${dObj.getDate()}日`;
    const weekdays = ['周日','周一','周二','周三','周四','周五','周六'];
    const wk = weekdays[dObj.getDay()];
    
    const slug = items.length > 0 ? items[0].market_slug : '';
    const marketUrl = `https://polymarket.com/event/${slug}`;
    
    card.innerHTML = `
        <div class="market-header">
            <span class="market-date">${dateStr} <span style="font-size:0.8em; font-weight:normal; color:#6c757d;">${wk}</span></span>
            <a href="${marketUrl}" target="_blank" class="view-details-btn">↗ 更多详情</a>
        </div>
    `;
    
    // 2. Chips Container (Small Boxes)
    const list = document.createElement('div');
    list.className = 'bucket-grid'; // Use a grid/flex layout
    
    // Determine Winner (>50%)
    let maxPrice = -1;
    items.forEach(i => { if(i.price > maxPrice) maxPrice = i.price; });
    
    // Sort items by numeric value logic? Or keep backend order?
    // Let's rely on backend clean order.
    
    items.forEach(item => {
        const chip = document.createElement('div');
        chip.className = 'bucket-chip';
        
        // Formatting: Replace "-" with "~"
        let label = item.outcome.replace(/\s-\s/g, ' ~ ');
        const prob = Math.round(item.price * 100);
        
        // Highlight logic
        if (item.price === maxPrice && item.price > 0.50) {
            chip.classList.add('winner');
        }
        
        // 6H Change Arrow
        let changeHtml = '';
        if (item.change_6h) {
            const chg = Math.round(item.change_6h * 100);
            if (chg !== 0) {
                 const color = chg > 0 ? '#28a745' : '#dc3545';
                 const sign = chg > 0 ? '▲' : '▼';
                 changeHtml = `<span style="color:${color}; font-size:0.8em; margin-left:3px;">${sign}${Math.abs(chg)}</span>`;
            }
        }
        
        chip.innerHTML = `
            <div class="chip-prob">${prob}%${changeHtml}</div>
            <div class="chip-label">${label}</div>
        `;
        list.appendChild(chip);
    });
    
    card.appendChild(list);
    return card;
}

window.loadRawData = async function(isReset = false) {
    if (isReset) {
        currentRawOffset = 0;
        document.getElementById('rawTableBody').innerHTML = '';
    }
    
    const limit = currentRawOffset === 0 ? 15 : 50;
    
    try {
        const response = await fetch(`/api/raw_data?limit=${limit}&offset=${currentRawOffset}`);
        const result = await response.json();
        
        if (result.status === 'success') {
            renderRawTable(result.data);
            currentRawOffset += limit;
        } else {
            console.error("Failed to load raw data:", result);
        }
    } catch (e) {
        console.error("Raw Data API Error:", e);
    }
};

const holidayTranslations = {
    "New Year's Day": "元旦",
    "Martin Luther King Jr. Day": "马丁路德金日",
    "Presidents Day": "总统日",
    "Good Friday": "受难日",
    "Memorial Day": "阵亡将士纪念日",
    "Independence Day": "独立日",
    "Labor Day": "劳动节",
    "Columbus Day": "哥伦布日",
    "Veterans Day": "退伍军人节",
    "Thanksgiving Day": "感恩节",
    "Christmas Day": "圣诞节",
    "Christmas Eve": "平安夜",
    "New Year's Eve": "除夕",
    // Add more mappings as found in your DB
};

function translateHoliday(name) {
    if (!name) return "";
    for (const [eng, cn] of Object.entries(holidayTranslations)) {
        if (name.includes(eng)) return cn;
    }
    return name; // Fallback to English if not found
}

function renderRawTable(data) {
    const tbody = document.getElementById('rawTableBody');
    
    data.forEach((row, index) => {
        const tr = document.createElement('tr');
        
        // 1. Date (Chinese + Date)
        const d = new Date(row.date);
        const weekday = d.toLocaleDateString('zh-CN', {weekday:'short'});
        const dateHtml = `<span class="col-date">${row.date}</span><span class="col-weekday">${weekday}</span>`;

        // 2. Throughput (Raw Number) - [FIX] User requested full raw numbers
        let tp = '-';
        if (row.throughput) {
           tp = row.throughput.toLocaleString();
        }

        // 3. Holiday (Badges - Localized)
        let holidayHtml = '<span style="color:#eee;">-</span>';
        
        // [MODIFIED] Use Client-Side Calculation for robust T-x / T+x
        if (row.is_holiday === 1) {
            let engName = row.holiday_name || 'Holiday';
            let cnName = translateHoliday(engName);
            holidayHtml = `<span class="badge-holiday exact" title="${engName}">${cnName}</span>`;
            
        } else {
            // Check dynamic distance first
            const win = getClientSideHolidayDistance(row.date);
            
            if (win) {
                 const dist = win.dist;
                 // [MODIFIED] User requested unified label "假期出行窗口" instead of "T-x"
                 const label = "假期出行窗口";
                 const t_tag = dist < 0 ? `T${dist}` : `T+${dist}`;
                 
                 // Translate anchor holiday name too?
                 let anchorCn = translateHoliday(win.name);
                 holidayHtml = `<span class="badge-holiday window" title="距离 ${anchorCn} ${Math.abs(dist)} 天 (${t_tag})">${label}</span>`;
            } else if (row.is_holiday_travel_window === 1) {
                 // Fallback if DB marked it but our calculator didn't (rare)
                 holidayHtml = `<span class="badge-holiday window" title="假日出行窗口">假期出行窗口</span>`;
            }
        }

        // 4. Weather (Aligned Dots)
        let wIndex = row.weather_index || 0;
        let wColor = '#28a745'; 
        if (wIndex >= 30) wColor = '#fa5252'; 
        else if (wIndex >= 15) wColor = '#fd7e14';
        else if (wIndex >= 5) wColor = '#fab005';
        
        let weatherHtml = `
            <div class="weather-indicator">
                <div class="weather-dot" style="background:${wColor};"></div>
                <span style="font-weight:500; font-size:0.9em;">${wIndex}</span>
            </div>
        `;

        // 5. Flight Volume (Raw Number) - [FIX] Daily Change (T vs T-1) instead of vs MA7
        let flightHtml = '-';
        if (row.flight_volume > 0) {
            let diffHtml = '';
            // 获取数组中的下一项（即前一天的数据，因为数据是按日期倒序排列的）
            const prevRow = data[index + 1];
            if (prevRow && prevRow.flight_volume > 0) {
                 const diff = row.flight_volume - prevRow.flight_volume;
                 const sign = diff > 0 ? '+' : '';
                 const color = diff > 0 ? '#28a745' : '#dc3545';
                 // 增加 Tooltip 提示用户这是日环比
                 diffHtml = `<div class="flight-diff" style="color:${color}" title="较前日变动">${sign}${parseInt(diff)}</div>`;
            }
            
            flightHtml = `
                <div class="flight-cell">
                    <div style="font-weight:500;">${row.flight_volume.toLocaleString()}</div>
                    ${diffHtml}
                </div>
            `;
        }

        // 6. Lags (Raw Number for consistency)
        let lagsHtml = '';
        if (row.throughput_lag_7) lagsHtml += `<div class="lag-cell">L7: ${row.throughput_lag_7.toLocaleString()}</div>`;

        tr.innerHTML = `
            <td>${dateHtml}</td>
            <td class="col-number">${tp}</td>
            <td>${holidayHtml}</td>
            <td>${weatherHtml}</td>
            <td>${flightHtml}</td>
            <td>${lagsHtml}</td>
        `;

        tbody.appendChild(tr);
    });
}


