export default {
    name: 'PolymarketGrid',
    props: ['marketData', 'isSyncing'],
    emits: ['sync-market'],
    template: `
    <div class="polymarket-section">
        <div class="section-header" style="display: flex; justify-content: space-between; align-items: center; padding: 0 10px 15px 10px;">
            <h3 style="margin: 0; color: #6f42c1;">🔮 当前活跃预测市场 (Polymarket)</h3>
            <button 
                @click="$emit('sync-market')" 
                :disabled="isSyncing"
                class="sync-btn"
                :style="{
                    background: isSyncing ? '#aaa' : '#6f42c1',
                    color: 'white',
                    border: 'none',
                    padding: '6px 15px',
                    borderRadius: '6px',
                    cursor: isSyncing ? 'not-allowed' : 'pointer',
                    fontSize: '0.9em',
                    fontWeight: 'bold',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '5px'
                }"
            >
                <span v-if="isSyncing">⏳ 同步中...</span>
                <span v-else>⚡ 实时同步最高赔率</span>
            </button>
        </div>

        <!-- Section 1: Daily Markets -->
        <div id="polymarket-grid" style="display: grid; grid-template-columns: repeat(auto-fill, minmax(350px, 1fr)); gap: 20px; padding: 10px;">
            <template v-for="(markets, date) in marketData" :key="date">
                <!-- Daily Criteria: Short date strings AND not containing full month names like 'January' -->
                <div v-if="date.length <= 15 && !date.includes('January')" class="market-card">
                    <div class="market-header">
                        <span class="market-date">{{ date }}</span>
                        <a 
                            v-if="markets && markets.length > 0"
                            :href="'https://polymarket.com/event/' + markets[0].market_slug" 
                            target="_blank" 
                            class="view-details-btn"
                        >
                            更多详情 ↗
                        </a>
                    </div>
                    
                    <div class="bucket-grid">
                        <div v-for="item in markets" :key="item.outcome" 
                             class="bucket-chip"
                             :class="{ winner: isWinner(item.price) }">
                            <span class="chip-prob">{{ (item.price * 100).toFixed(0) }}%</span>
                            <span class="chip-label">{{ item.outcome }}</span>
                        </div>
                    </div>
                </div>
            </template>
        </div>

        <!-- Divider (Always show if we have data, or rely on visual separation) -->
        <div style="border-top: 2px dashed #ccc; margin: 40px 10px 20px 10px; position: relative;">
            <span style="position: absolute; top: -12px; left: 50%; transform: translateX(-50%); background: #fff; padding: 0 15px; color: #888; font-size: 0.9em;">
                ▼ 周度总人数盘 (Weekly) ▼
            </span>
        </div>

        <!-- Section 2: Weekly Markets (Same Style) -->
        <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(350px, 1fr)); gap: 20px; padding: 10px;">
            <template v-for="(markets, date) in marketData" :key="date">
                <!-- Weekly Criteria: Long date strings OR containing month names -->
                <div v-if="date.length > 15 || date.includes('January') || date.includes('February')" class="market-card">
                    <div class="market-header">
                        <span class="market-date">{{ date }}</span>
                        <a 
                            v-if="markets && markets.length > 0"
                            :href="'https://polymarket.com/event/' + markets[0].market_slug" 
                            target="_blank" 
                            class="view-details-btn"
                        >
                            更多详情 ↗
                        </a>
                    </div>
                    
                    <div class="bucket-grid">
                        <div v-for="item in markets" :key="item.outcome" 
                             class="bucket-chip"
                             :class="{ winner: isWinner(item.price) }">
                            <span class="chip-prob">{{ (item.price * 100).toFixed(0) }}%</span>
                            <span class="chip-label">{{ item.outcome }}</span>
                        </div>
                    </div>
                </div>
            </template>
        </div>
    </div>
    `,
    methods: {
        isWinner(price) {
            return price > 0.5;
        }
    }
};
