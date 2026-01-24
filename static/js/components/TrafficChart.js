export default {
    name: 'TrafficChart',
    props: ['chartData', 'predictions', 'challengerData', 'annotations'],
    template: `
    <div class="chart-container">
        <canvas id="trafficChart"></canvas>
    </div>
    `,
    mounted() {
        this.initChart();
    },
    watch: {
        chartData: {
            handler(newVal) {
                if (this.chart) {
                    this.chart.data.datasets[0].data = newVal;
                    this.chart.update();
                }
            },
            deep: true
        },
        predictions: {
            handler(newVal) {
                if (this.chart) {
                    this.chart.data.datasets[1].data = newVal;
                    this.chart.update();
                }
            },
            deep: true
        },
        challengerData: {
            handler(newVal) {
                if (this.chart && newVal.length > 0) {
                    this.chart.data.datasets[2].data = newVal;
                    this.chart.data.datasets[2].hidden = false;
                    this.chart.update();
                }
            },
            deep: true
        },
        annotations: {
            handler(newVal) {
                if (this.chart) {
                    this.chart.options.plugins.annotation.annotations = newVal;
                    this.chart.update();
                }
            },
            deep: true
        }
    },
    methods: {
        initChart() {
            const ctx = document.getElementById('trafficChart').getContext('2d');
            this.chart = new Chart(ctx, {
                type: 'line',
                data: {
                    datasets: [{
                        label: '历史旅客吞吐量',
                        data: this.chartData || [],
                        borderColor: '#007bff',
                        backgroundColor: 'rgba(0, 123, 255, 0.1)',
                        borderWidth: 2,
                        fill: true,
                        tension: 0.3,
                        pointBackgroundColor: function(context) {
                            const idx = context.dataIndex;
                            const item = context.dataset.data[idx];
                            if (!item || item.y === null) return 'transparent';
                            const w = item.weather_index;
                            if (w >= 30) return '#dc3545'; // 极端天气
                            if (w >= 15) return '#fd7e14'; // 恶劣天气
                            return '#007bff';
                        },
                        pointRadius: 3, 
                        pointHoverRadius: 5,
                    }, {
                        label: 'AI 预测',
                        data: this.predictions || [],
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
                        label: 'FLAML 挑战者 (Challenger)',
                        data: this.challengerData || [],
                        borderColor: '#6f42c1',
                        backgroundColor: 'rgba(111, 66, 193, 0.1)',
                        borderWidth: 2,
                        pointRadius: 3,
                        borderDash: [2, 2],
                        tension: 0.4,
                        hidden: true
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: {
                        mode: 'nearest',
                        intersect: true,
                        axis: 'xy'
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
                                autoSkip: true,
                                autoSkipPadding: 15
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
                                afterLabel: function(context) {
                                    const item = context.raw;
                                    let lines = [];
                                    if (item.holiday_name) lines.push(' 🎉 节日: ' + item.holiday_name);
                                    else if (item.is_holiday === 1) lines.push(' 🎉 节日因子: 命中');
                                    
                                    if (item.weather_index > 0) {
                                        let wInfo = ` ⛈️ 气象指数: ${item.weather_index}`;
                                        if (item.weather_index >= 30) wInfo += ' (⚠️ 系统熔断)';
                                        else if (item.weather_index >= 15) wInfo += ' (⚠️ 恶劣天气)';
                                        lines.push(wInfo);
                                    }
                                    return lines;
                                }
                            }
                        },
                        zoom: {
                            pan: { enabled: true, mode: 'x' },
                            zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'x' }
                        },
                        annotation: {
                            annotations: this.annotations || {}
                        }
                    }
                }
            });
        },
        resetZoom() {
            if(this.chart) this.chart.resetZoom();
        },
        setRange(minTime, maxTime) {
            if(this.chart) {
                this.chart.options.scales.x.min = minTime;
                this.chart.options.scales.x.max = maxTime;
                this.chart.update();
            }
        }
    }
};
