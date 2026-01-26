// api.js - Centralized API Service

const API = {
    async getHistory() {
        const res = await fetch('/api/data');
        if (!res.ok) throw new Error('Failed to fetch history');
        return await res.json();
    },

    async getPredictions() {
        const res = await fetch('/api/predictions');
        if (!res.ok) throw new Error('Failed to fetch predictions');
        return await res.json();
    },

    async getRawData(limit = 50, offset = 0) {
        const res = await fetch(`/api/raw_data?limit=${limit}&offset=${offset}`);
        if (!res.ok) throw new Error('Failed to fetch raw data');
        return await res.json();
    },

    async getMarketSentiment() {
        const res = await fetch('/api/market_sentiment');
        if (!res.ok) throw new Error('Failed to fetch market sentiment');
        return await res.json();
    },

    async updateData() {
        const res = await fetch('/api/update_data', { method: 'POST' });
        if (!res.ok) throw new Error('Update failed');
        return await res.json();
    },

    async runPrediction() {
        const res = await fetch('/api/run_prediction', { method: 'POST' });
        if (!res.ok) throw new Error('Prediction run failed');
        return await res.json();
    },

    async runSniper() {
        const res = await fetch('/api/predict_sniper', { method: 'POST' });
        if (!res.ok) throw new Error('Sniper run failed');
        return await res.json();
    },

    async runChallenger() {
        const res = await fetch('/api/run_challenger', { method: 'POST' });
        if (!res.ok) throw new Error('Challenger run failed');
        return await res.json();
    },

    async syncMarketSentiment() {
        const res = await fetch('/api/sync_market_sentiment', { method: 'POST' });
        if (!res.ok) throw new Error('Market sync failed');
        return await res.json();
    }
};

export default API;
