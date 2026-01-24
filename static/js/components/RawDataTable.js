export default {
    name: 'RawDataTable',
    props: ['data'],
    emits: ['load-more'],
    template: `
    <div style="background: rgba(255, 255, 255, 0.7); backdrop-filter: blur(12px); border-radius: 16px; border: 1px solid rgba(255, 255, 255, 0.6); overflow: hidden;">
        <div class="glass-scroll" style="overflow-x: auto; max-height: 600px; overflow-y: auto;">
            <table id="rawTable">
                <thead>
                    <tr>
                        <th style="padding: 10px; background: rgba(255,255,255,0.9);">日期 (Date)</th>
                        <th style="padding: 10px; background: rgba(255,255,255,0.9); text-align: center;">客流量 (Throughput)</th>
                        <th style="padding: 10px; background: rgba(255,255,255,0.9); text-align: center;">节假日 (Holiday)</th>
                        <th style="padding: 10px; background: rgba(255,255,255,0.9); text-align: center;">气象因子 (Weather)</th>
                        <th style="padding: 10px; background: rgba(255,255,255,0.9); text-align: center;">航班因子 (Flight)</th>
                        <th style="padding: 10px; background: rgba(255,255,255,0.9); text-align: center;">时序锚点 (Lags)</th>
                    </tr>
                </thead>
                <tbody>
                    <tr v-for="row in data" :key="row.date" style="border-bottom: 1px solid #eee;">
                        <!-- Date -->
                        <td style="padding: 12px 8px; text-align: center;">
                            <span class="col-date">{{ row.date }}</span>
                            <span class="col-weekday">{{ getWeekday(row.date) }}</span>
                        </td>
                        
                        <!-- Throughput -->
                        <td style="padding: 12px 8px; text-align: center;">
                            <span v-if="row.throughput" class="col-number" style="font-weight: 600; font-size: 1.1em;">
                                {{ (row.throughput / 1000000).toFixed(2) }}M
                            </span>
                            <span v-else style="color: #adb5bd;">-</span>
                        </td>
                        
                        <!-- Holiday -->
                        <td style="padding: 12px 8px; text-align: center;">
                            <span v-if="row.is_holiday || row.is_holiday_travel_window" class="badge-holiday" 
                                  :class="getHolidayClass(row)">
                                {{ row.holiday_name || 'Holiday' }}
                            </span>
                            <span v-else style="color: #dee2e6;">-</span>
                        </td>
                        
                        <!-- Weather -->
                        <td style="padding: 12px 8px; text-align: center;">
                            <div class="weather-indicator" v-if="row.weather_index > 0">
                                <span class="weather-dot" :style="{ background: getWeatherColor(row.weather_index) }"></span>
                                <span style="font-weight: 500; font-size: 0.9em; margin-left: 6px;">{{ row.weather_index }}</span>
                            </div>
                            <!-- Show Green Dot for 0 if it's not null (Nice Weather) -->
                            <div class="weather-indicator" v-else-if="row.weather_index === 0">
                                <span class="weather-dot" style="background: #28a745;"></span>
                                <span style="font-weight: 500; font-size: 0.9em; margin-left: 6px;">0</span>
                            </div>
                            <span v-else style="color: #dee2e6;">-</span>
                        </td>
                        
                        <!-- Flight -->
                        <td style="padding: 12px 8px; text-align: center;">
                            <div class="flight-cell" v-if="row.flight_volume">
                                <span style="font-weight: 600; font-size: 0.9em;">{{ parseInt(row.flight_volume).toLocaleString() }}</span>
                                <span class="flight-diff" 
                                      v-if="row.flight_lag_1"
                                      :style="{ color: (row.flight_volume - row.flight_lag_1) >= 0 ? '#28a745' : '#dc3545' }">
                                    {{ (row.flight_volume - row.flight_lag_1) > 0 ? '+' : '' }}{{ (row.flight_volume - row.flight_lag_1).toLocaleString() }}
                                </span>
                            </div>
                            <span v-else style="color: #dee2e6;">-</span>
                        </td>
                        
                        <!-- Lags -->
                        <td style="padding: 12px 8px; text-align: center;">
                            <span class="lag-cell" style="font-family: 'Consolas', monospace; color: #6c757d; background: #f8f9fa; padding: 4px 8px; border-radius: 4px; font-size: 0.8em; letter-spacing: -0.5px;">
                                L7: {{ getLagDiff(row) }}
                            </span>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
        <div style="text-align: center; padding: 15px;">
            <button @click="$emit('load-more')" style="padding: 10px 30px; background: #17a2b8; color: white; border: none; border-radius: 4px; cursor: pointer;">⬇️ 加载更多 (Load More 50)</button>
        </div>
    </div>
    `,
    methods: {
        getWeekday(dateStr) {
            if(!dateStr) return '';
            const d = new Date(dateStr);
            // UTC fix for simple date string
            const utcDay = new Date(d.getTime() + d.getTimezoneOffset() * 60000).getDay();
            const days = ['周日', '周一', '周二', '周三', '周四', '周五', '周六'];
            return days[utcDay] || ''; 
        },
        getHolidayClass(row) {
            // Check if exact day or window
            if (row.is_holiday_travel_window) return 'window';
            
            if (row.is_holiday_exact_day || (row.holiday_name && !row.holiday_name.includes('Window') && !row.holiday_name.includes('窗口'))) {
                return 'exact';
            }
            return 'window';
        },
        getWeatherColor(idx) {
            if (idx >= 30) return '#dc3545'; // Red
            if (idx >= 15) return '#fd7e14'; // Orange
            if (idx >= 5) return '#ffc107'; // Yellow
            return '#28a745'; // Green
        },
        getLagDiff(row) {
            // Logic to calculate Lag 7 vs Actual
            if (row.throughput && row.throughput_lag_7) {
                // If actual exists, show current
                return (row.throughput_lag_7 / 1000000).toFixed(2) + 'M';
            } else if (row.throughput_lag_7) {
                 return (row.throughput_lag_7 / 1000000).toFixed(2) + 'M';
            }
            return '-';
        }
    }
};
