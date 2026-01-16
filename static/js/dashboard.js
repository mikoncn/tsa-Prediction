let chart;
let allData = [];
const availableYears = new Set();

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
                pointRadius: function(context) {
                    const idx = context.dataIndex;
                    const item = context.dataset.data[idx];
                    if (!item || item.y === null) return 0;
                    const w = item.weather_index;
                    if (w >= 30) return 6;
                    if (w >= 15) return 4;
                    return 2;
                },
                pointHoverRadius: 8
            }, {
                label: 'AI 预测',
                data: [],
                borderColor: '#fd7e14',
                backgroundColor: 'rgba(253, 126, 20, 0.1)',
                borderWidth: 2,
                borderDash: [5, 5],
                pointRadius: 4,
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
                intersect: false,
                mode: 'nearest', // 关键修改：从 index 改为 nearest，这样鼠标离得近就能触发，不用完全对齐
                axis: 'x' 
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
                            if (item.holiday_name) {
                                lines.push(' 🎉 节日: ' + item.holiday_name);
                            }
                            if (item.weather_index > 0) {
                                let weatherInfo = ` ⛈️ 气象指数: ${item.weather_index}`;
                                if (item.weather_index >= 30) weatherInfo += ' (⚠️ 系统熔断)';
                                else if (item.weather_index >= 15) weatherInfo += ' (⚠️ 恶劣天气)';
                                lines.push(weatherInfo);
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
        fetchPredictions();    // 加载预测数据
        
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
                    y: item.predicted
                });
            });
        }
        
        // 2. Add Forecast (Future Predictions)
        if (data.forecast && data.forecast.length > 0) {
            data.forecast.forEach(item => {
                // Avoid duplicates if forecast overlaps with history (though logic should prevent it)
                if (!combinedPredictions.some(p => p.x === item.ds)) {
                    combinedPredictions.push({
                        x: item.ds,
                        y: item.predicted_throughput
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
