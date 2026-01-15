let chart;
let allData = [];
const availableYears = new Set();

// 初始化图表
function initChart() {
    const ctx = document.getElementById('trafficChart').getContext('2d');
    chart = new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [{
                label: '旅客吞吐量',
                data: [],
                borderColor: '#007bff',
                backgroundColor: 'rgba(0, 123, 255, 0.1)',
                borderWidth: 2,
                fill: true,
                tension: 0.3,
                
                // 动态点样式 (Weather)
                pointBackgroundColor: function(context) {
                    const idx = context.dataIndex;
                    const item = context.dataset.data[idx];
                    if (!item || item.y === null) return 'transparent'; // Future/Null points invisible
                    
                    const w = item.weather_index;
                    if (w >= 30) return '#dc3545'; // Red (Meltdown)
                    if (w >= 15) return '#fd7e14'; // Orange (Severe)
                    return '#007bff'; // Blue (Normal)
                },
                pointRadius: function(context) {
                    const idx = context.dataIndex;
                    const item = context.dataset.data[idx];
                    if (!item || item.y === null) return 0;
                    
                    const w = item.weather_index;
                    if (w >= 30) return 6; // Big dot for meltdown
                    if (w >= 15) return 4;
                    return 2;
                },
                pointHoverRadius: 8
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                intersect: false,
                mode: 'index',
            },
            onClick: (e) => {
                const points = chart.getElementsAtEventForMode(e, 'nearest', { intersect: true }, true);
                if (points.length) {
                    const firstPoint = points[0];
                    const item = chart.data.datasets[firstPoint.datasetIndex].data[firstPoint.index];
                    if(item.y !== null) {
                        alert(`日期: ${item.x}\n客流: ${item.y}\n气象指数: ${item.weather_index}\n节日: ${item.holiday_name || '无'}`);
                    }
                }
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'month',
                        displayFormats: {
                            month: 'yyyy年MM月',
                            day: 'MM-dd (EEE)'
                        },
                        tooltipFormat: 'yyyy-MM-dd'
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
                            let label = ' 旅客: ' + new Intl.NumberFormat().format(item.y);
                            return label;
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
                    annotations: {} // 动态填充
                },
                zoom: {
                    pan: { enabled: true, mode: 'x' },
                    zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'x' },
                    limits: { x: {min: 'original', max: 'original'} }
                }
            }
        }
    });
}

// 加载数据
async function loadData() {
    try {
        const response = await fetch('/api/data');
        const data = await response.json();
        
        // 转换数据格式 (保留特征字段)
        allData = data.map(item => ({
            x: item.date,
            y: item.throughput, // Note: this can be null for future
            weather_index: item.weather_index || 0,
            is_holiday: item.is_holiday || 0,
            holiday_name: item.holiday_name || ''
        }));

        // 提取年份
        availableYears.clear();
        allData.forEach(item => {
            const year = item.x.split('-')[0];
            availableYears.add(year);
        });
        populateYearSelect();

        // 默认显示全部
        applyFilters(); 
        
        // [NEW] 加载预测数据
        fetchPredictions();
        
    } catch (error) {
        console.error('Error loading data:', error);
        alert('加载数据失败，请检查后端服务是否启动。');
    }
}

// [NEW] 获取并显示预测数据
let forecastDataMap = {}; // Cache forecast data

async function fetchPredictions() {
    try {
        const response = await fetch('/api/predictions');
        const data = await response.json();
        
        // 1. 显示未来预测 (Populate Dropdown)
        const select = document.getElementById('predDateSelect');
        select.innerHTML = ''; // Clear
        
        if (data.forecast && data.forecast.length > 0) {
            forecastDataMap = {}; // Reset cache
            
            data.forecast.forEach((item, index) => {
                const opt = document.createElement('option');
                opt.value = item.ds;
                // Display format: "01-14 (Wed)"
                const d = new Date(item.ds);
                const dayName = d.toLocaleDateString('en-US', { weekday: 'short' });
                opt.text = `${item.ds.slice(5)} (${dayName})`;
                select.add(opt);
                
                forecastDataMap[item.ds] = item.predicted_throughput;
            });
            
            // Trigger first update
            select.selectedIndex = 0;
            updatePredictionDisplay(select.value);
            
            // Add listener
            select.onchange = function() {
                updatePredictionDisplay(this.value);
            };

        } else {
            const opt = document.createElement('option');
            opt.text = "暂无数据";
            select.add(opt);
            document.getElementById('predPassengers').innerText = '-';
        }

        // 2. 填充回测准确率表格
        if (data.validation && data.validation.length > 0) {
            const tableBody = document.querySelector('#accuracyTable tbody');
            tableBody.innerHTML = ''; // Clear existing
            
            // Limit to last 10 records for cleaner view
            const recentValidation = data.validation.slice(-15).reverse(); // Reverse to show newest first
            
            recentValidation.forEach(row => {
                const tr = document.createElement('tr');
                tr.style.borderBottom = '1px solid #eee';
                
                const errorRate = parseFloat(row.error_rate);
                let badgeClass = '';
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

function updatePredictionDisplay(date) {
    if (forecastDataMap[date]) {
        const val = forecastDataMap[date];
        document.getElementById('predPassengers').innerText = (val / 1000000).toFixed(2) + 'M';
    } else {
        document.getElementById('predPassengers').innerText = '-';
    }
}

// ... rest of code (year select, etc)
// 填充年份选择框
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

// 应用筛选
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

// 快捷范围筛选
function setQuickRange(days) {
    if (!allData || allData.length === 0) return;
    
    // 找最后一个有数据的日期
    const validData = allData.filter(d => d.y !== null);
    const lastDate = new Date(validData[validData.length - 1].x);
    
    // 计算起始日期
    const startDate = new Date(lastDate);
    startDate.setDate(lastDate.getDate() - days);
    
    const startStr = startDate.toISOString().split('T')[0];
    
    const filteredData = allData.filter(d => d.x >= startStr);
    
    // Reset select to 'all' visually to avoid confusion, or handle nicely
    document.getElementById('yearSelect').value = 'all';
    
    updateChart(filteredData);
    generateHolidayAnnotations(filteredData);
    updateStats(filteredData);
}

// 更新图表数据
function updateChart(data) {
    chart.data.datasets[0].data = data;
    chart.update();
}

// 生成节假日 Annotations
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

// 更新统计信息
function updateStats(data) {
    // 1. 找到最后一个"实际"有数据的点 (y !== null)
    const validData = data.filter(item => item.y !== null);

    if (validData.length === 0) {
        document.getElementById('latestPassengers').innerText = '-';
        document.getElementById('prevPassengers').innerText = '-';
        document.getElementById('predPassengers').innerText = '数据不足';
        return;
    }

    // 2. 获取最新一天 (Latest)
    const latest = validData[validData.length - 1];
    document.getElementById('latestPassengers').innerText = (latest.y / 1000000).toFixed(2) + 'M';
    document.getElementById('latestDate').innerText = latest.x;

    // 3. 获取前一天 (Previous)
    if (validData.length >= 2) {
        const prev = validData[validData.length - 2];
        document.getElementById('prevPassengers').innerText = (prev.y / 1000000).toFixed(2) + 'M';
        document.getElementById('prevDate').innerText = prev.x;
    } else {
        document.getElementById('prevPassengers').innerText = '-';
        document.getElementById('prevDate').innerText = '';
    }

    // 4. 预测客流 (Predicted) - 占位符
    // 未来如果模型接入，这里可以读取 validData 之后的第一个点(如果后端给了预测值)
    // 目前保持"占位"状态
    document.getElementById('predPassengers').innerText = 'Waiting for Model...';
}

document.getElementById('yearSelect').addEventListener('change', applyFilters);

document.addEventListener('DOMContentLoaded', () => {
    initChart();
    loadData();
});
