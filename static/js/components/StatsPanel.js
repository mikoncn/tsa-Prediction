export default {
    name: 'StatsPanel',
    props: ['stats', 'prediction'],
    template: `
    <div class="stats-panel" style="display: flex; justify-content: space-around; margin-top: 20px; text-align: center;">
        <div class="stat-card">
            <h3>最新客流 (<span>{{ stats.latestDate || '-' }}</span>)</h3>
            <div class="value" style="font-size: 1.5em; font-weight: bold; color: #007bff;">
                {{ stats.latestValue ? (stats.latestValue / 1000000).toFixed(2) + 'M' : '-' }}
            </div>
        </div>
        <div class="stat-card">
            <h3>前日客流 (<span>{{ stats.prevDate || '-' }}</span>)</h3>
            <div class="value" style="font-size: 1.5em; font-weight: bold; color: #6c757d;">
                {{ stats.prevValue ? (stats.prevValue / 1000000).toFixed(2) + 'M' : '-' }}
            </div>
        </div>
        <div class="stat-card" style="border: 2px solid #28a745; background: #f0fff4; min-width: 250px;">
            <h3>
                预测客流 
                <select :value="prediction.selectedDate" @change="$emit('update:selectedDate', $event.target.value)" style="font-size: 0.8em; padding: 2px; margin-left: 5px; border-radius: 4px; border: 1px solid #ccc;">
                    <option v-if="!prediction.options.length" value="">Loading...</option>
                    <option v-for="opt in prediction.options" :key="opt.date" :value="opt.date">
                        {{ opt.label }}
                    </option>
                </select>
            </h3>
            <div class="value" style="font-size: 1.5em; font-weight: bold; color: #28a745;">
                {{ prediction.value ? (prediction.value / 1000000).toFixed(2) + 'M' : '-' }}
            </div>
        </div>
    </div>
    `
};
