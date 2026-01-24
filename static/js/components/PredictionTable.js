export default {
    name: 'PredictionTable',
    props: ['validationData'],
    template: `
    <div style="overflow-x: auto; max-height: 500px; overflow-y: auto; border: 1px solid #dee2e6;">
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
    `,
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
