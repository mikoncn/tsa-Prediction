export default {
    name: 'PolymarketGrid',
    props: ['marketData'],
    template: `
    <div id="polymarket-grid" style="display: grid; grid-template-columns: repeat(auto-fill, minmax(350px, 1fr)); gap: 20px; padding: 10px;">
        <div v-for="(markets, date) in marketData" :key="date" class="market-card">
            <div class="market-header">
                <span class="market-date">{{ date }}</span>
                <a 
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
    </div>
    `,
    methods: {
        isWinner(price) {
            // Simple heuristic to highlight the most probable outcome
            // In a real app we might want to compare against siblings
            return price > 0.5;
        }
    }
};
