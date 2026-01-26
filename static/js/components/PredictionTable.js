export default {
    name: 'PredictionTable',
    props: ['validationData'],
    template: `
    <div class="prediction-table-container">
        <!-- Summary Stats Board -->
        <div style="display: flex; gap: 20px; margin-bottom: 25px;">
            <div style="flex: 1; background: #fff5f5; border: 1px solid #feb2b2; padding: 15px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.02); text-align: center;">
                <div style="font-size: 0.85em; color: #c53030; margin-bottom: 5px; font-weight: bold;">🔥 最大误差度 (Worst Error)</div>
                <div style="font-size: 1.6em; font-weight: bold; color: #c53030;">{{ maxError.toFixed(2) }}%</div>
                <div style="font-size: 0.7em; color: #e53e3e; margin-top: 4px;">由历史实测点中的最大偏差计算</div>
            </div>
            <div style="flex: 1; background: #f0fff4; border: 1px solid #9ae6b4; padding: 15px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.02); text-align: center;">
                <div style="font-size: 0.85em; color: #276749; margin-bottom: 5px; font-weight: bold;">🎯 平均准确度 (Avg Accuracy)</div>
                <div style="font-size: 1.6em; font-weight: bold; color: #2f855a;">{{ avgAccuracy.toFixed(2) }}%</div>
                <div style="font-size: 0.7em; color: #38a169; margin-top: 4px;">100% - 平均绝对百分比误差 (MAPE)</div>
            </div>
        </div>

        <div style="overflow-x: auto; max-height: 500px; overflow-y: auto; border: 1px solid #dee2e6; border-radius: 10px; box-shadow: 0 4px 12px rgba(0,0,0,0.03);">
            <table style="width: 100%; border-collapse: collapse; margin-top: 0;">
                <thead>
                    <tr style="background: #f8f9fa; border-bottom: 2px solid #dee2e6; position: sticky; top: 0; z-index: 1;">
                        <th style="padding: 12px; text-align: left;">日期</th>
                        <th style="padding: 12px; text-align: right;">真实客流</th>
                        <th style="padding: 12px; text-align: right;">AI 预测</th>
                        <th style="padding: 12px; text-align: right;">误差 (人数)</th>
                        <th style="padding: 12px; text-align: center;">准确度</th>
                    </tr>
                </thead>
                <tbody>
                    <tr v-for="row in validationData" :key="row.date" style="border-bottom: 1px solid #eee;" 
                        :style="{ backgroundColor: getBgColor(row.error_rate) }">
                        <td style="padding: 10px;">{{ row.date }}</td>
                        <td style="padding: 10px; text-align: right;">{{ parseInt(row.actual).toLocaleString() }}</td>
                        <td style="padding: 10px; text-align: right; font-weight: bold; color: #007bff;">{{ parseInt(row.predicted).toLocaleString() }}</td>
                        <td style="padding: 10px; text-align: right;">{{ parseInt(row.difference).toLocaleString() }}</td>
                        <td style="padding: 10px; text-align: center;">
                            {{ parseFloat(row.error_rate).toFixed(2) }}% 
                            <span style="font-size: 0.8em; margin-left: 5px;">{{ getBadgeText(row.error_rate) }}</span>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>
    `,
    computed: {
        maxError() {
            if (!this.validationData || this.validationData.length === 0) return 0;
            return Math.max(...this.validationData.map(d => parseFloat(d.error_rate) || 0));
        },
        avgAccuracy() {
            if (!this.validationData || this.validationData.length === 0) return 0;
            const sumError = this.validationData.reduce((acc, curr) => acc + (parseFloat(curr.error_rate) || 0), 0);
            const avgError = sumError / this.validationData.length;
            return 100 - avgError;
        }
    },
    methods: {
        getBgColor(rate) {
            if (rate > 8.0) return '#fff5f5';
            if (rate > 5.0) return '#fffdf5';
            return 'transparent';
        },
        getBadgeText(rate) {
            if (rate > 8.0) return '🔴 偏差大';
            if (rate > 5.0) return '⚠️ 一般';
            return '✅ 优秀';
        }
    }
};
