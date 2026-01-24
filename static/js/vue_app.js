import { createApp, ref, onMounted, computed, reactive } from 'https://unpkg.com/vue@3/dist/vue.esm-browser.js';
import API from './api.js';
import ControlPanel from './components/ControlPanel.js';
import StatsPanel from './components/StatsPanel.js';
import TrafficChart from './components/TrafficChart.js';
import PredictionTable from './components/PredictionTable.js';
import RawDataTable from './components/RawDataTable.js';
import PolymarketGrid from './components/PolymarketGrid.js';

const App = {
    components: {
        ControlPanel,
        StatsPanel,
        TrafficChart,
        PredictionTable,
        RawDataTable,
        PolymarketGrid
    },
    setup() {
        // State
        const allData = ref([]);
        const predictions = ref([]);
        const challengerData = ref([]);
        const rawData = ref([]);
        const marketData = ref({});
        const validationHistory = ref([]);
        
        const years = ref([]);
        const activeTab = ref('validation');
        const showModal = ref(false); // Export Modal
        const showSniperModal = ref(false);
        
        const stats = reactive({
            latestDate: '-', latestValue: null,
            prevDate: '-', prevValue: null
        });
        
        const predictionState = reactive({
            options: [],
            selectedDate: '',
            value: null
        });
        
        // Sniper Result
        const sniperResult = reactive({
            date: '-', value: null, flights: 0, is_fallback: false
        });

        // Chart Props
        const currentChartData = ref([]);
        const currentAnnotations = ref({});
        
        // Refs
        const chartRef = ref(null);

        // Methods
        const loadHistory = async () => {
            try {
                const data = await API.getHistory();
                allData.value = data.map(item => ({
                    x: item.date,
                    y: item.throughput,
                    weather_index: item.weather_index || 0,
                    is_holiday: item.is_holiday || 0,
                    holiday_name: item.holiday_name || ''
                }));
                
                // Extract Years
                const ySet = new Set(allData.value.map(d => d.x.split('-')[0]));
                years.value = Array.from(ySet).sort().reverse();
                
                updateStats(allData.value);
                currentChartData.value = allData.value; // Initial full view
                generateAnnotations(allData.value);
                
                // Initial Zoom (Recent 30 days)
                setQuickRange(30);
            } catch (e) {
                console.error(e);
            }
        };

        const loadPredictions = async () => {
            try {
                const res = await API.getPredictions();
                
                // 1. Chart Predictions (History + Forecast)
                let combined = [];
                if (res.history) {
                    res.history.forEach(p => combined.push({
                        x: p.date, y: p.predicted,
                        weather_index: p.weather_index,
                        is_holiday: p.is_holiday,
                        holiday_name: 'Prediction'
                    }));
                }
                if (res.forecast) {
                    predictionState.options = [];
                    res.forecast.forEach(p => {
                        // Avoid dupes
                        if (!combined.some(c => c.x === p.ds)) {
                            combined.push({
                                x: p.ds, y: p.predicted_throughput,
                                weather_index: p.weather_index,
                                is_holiday: p.is_holiday,
                                holiday_name: p.holiday_name
                            });
                        }
                        // Dropdown options
                        predictionState.options.push({
                            date: p.ds,
                            label: `${p.ds.slice(5)} (${new Date(p.ds).toLocaleDateString('en-US',{weekday:'short'})})`,
                            value: p.predicted_throughput // Store directly
                        });
                    });
                    
                    // Select first forecast
                    if (predictionState.options.length > 0) {
                        predictionState.selectedDate = predictionState.options[0].date;
                        // Directly set value
                        predictionState.value = predictionState.options[0].value;
                    }
                }
                
                combined.sort((a,b) => new Date(a.x) - new Date(b.x));
                predictions.value = combined;
                
                // 2. Validation Table
                if (res.validation) {
                    validationHistory.value = res.validation.reverse();
                }
                
            } catch (e) { console.error(e); }
        };

        const updatePredictionDisplay = (date) => {
            if (!date) return;
            // Find in options first (Forecast)
            const opt = predictionState.options.find(o => o.date === date);
            if (opt) {
                predictionState.value = opt.value;
                return;
            }
            
            // Fallback to Chart Data (History)
            let hit = predictions.value.find(p => p.x === date);
            if (hit) {
                predictionState.value = hit.y;
            } else {
                predictionState.value = null;
            }
        };

        // Watch dropdown change
        const onPredictionDateChange = (val) => {
            predictionState.selectedDate = val;
            updatePredictionDisplay(val);
        };

        const loadRaw = async () => {
            try {
                const res = await API.getRawData(50, rawData.value.length);
                if (res.status === 'success') {
                    // Append unique
                    // rawData.value.push(...res.data); // Vue 3 push ok
                    // Check dupes just in case
                    const newItems = res.data.filter(n => !rawData.value.some(e => e.date === n.date));
                    rawData.value.push(...newItems);
                }
            } catch (e) { alert(e); }
        };

        const loadMarket = async () => {
            try {
                marketData.value = await API.getMarketSentiment();
            } catch (e) { console.error(e); }
        };

        const updateData = async () => {
            if(!confirm('确定要更新数据吗？可能需要几十秒。')) return;
            try {
                const res = await API.updateData();
                if(res.status === 'success') {
                    alert('✅ 更新成功');
                    loadHistory();
                    loadPredictions();
                } else {
                    alert('❌ ' + res.message);
                }
            } catch (e) { alert(e); }
        };

        const runPrediction = async () => {
            try {
                const res = await API.runPrediction();
                if (res.status === 'success') {
                    alert('✅ 预测完成');
                    loadPredictions();
                }
            } catch(e) { alert(e); }
        };

        const runSniper = async () => {
            try {
                const res = await API.runSniper();
                if (res.status === 'success') {
                    const d = res.data;
                    sniperResult.date = d.date;
                    sniperResult.value = d.predicted_throughput;
                    sniperResult.flights = d.flight_volume;
                    sniperResult.is_fallback = d.is_fallback;
                    showSniperModal.value = true;
                }
            } catch(e) { alert(e); }
        };

        const runChallenger = async () => {
            if(!confirm('启动深度对决训练需耗时约3分钟，确定继续？')) return;
            try {
                const res = await API.runChallenger();
                if (res.status === 'success') {
                    alert(`✅ 挑战成功!\n模型: ${res.data.model}\nMAPE: ${(res.data.mape*100).toFixed(2)}%`);
                    // Update chart
                    if (res.data.forecast) {
                        challengerData.value = res.data.forecast.map(i => ({ x: i.date, y: i.predicted }));
                    }
                }
            } catch(e) { alert(e); }
        };

        // UI Helpers
        const setQuickRange = (days) => {
            if (!allData.value.length) return;
            const valid = allData.value.filter(d => d.y !== null);
            const last = new Date(valid[valid.length-1].x);
            const start = new Date(last);
            start.setDate(last.getDate() - days);
            
            if (chartRef.value) {
                chartRef.value.setRange(start.getTime(), last.getTime());
            }
        };

        const filterYear = (year) => {
            if (year === 'all') {
                currentChartData.value = allData.value;
            } else {
                currentChartData.value = allData.value.filter(d => d.x.startsWith(year));
            }
            generateAnnotations(currentChartData.value);
        };

        const generateAnnotations = (data) => {
            const anns = {};
            let inHoliday = false;
            let start = null;
            let name = '';
            
            const sorted = [...data].sort((a,b) => new Date(a.x) - new Date(b.x));
            
            sorted.forEach((item, idx) => {
                const isHol = item.is_holiday === 1;
                if (isHol && !inHoliday) {
                    inHoliday = true; start = item.x; name = item.holiday_name;
                } else if ((!isHol || item.holiday_name !== name) && inHoliday) {
                    const end = sorted[idx-1].x;
                    anns[`holiday_${idx}`] = {
                        type: 'box', xMin: start, xMax: end,
                        backgroundColor: 'rgba(153, 102, 255, 0.15)', borderWidth: 0, drawTime: 'beforeDatasetsDraw'
                    };
                    inHoliday = false;
                    if (isHol) { // New holiday starts immediately
                        inHoliday = true; start = item.x; name = item.holiday_name;
                    }
                }
            });
            // Close last
            if (inHoliday) {
                const end = sorted[sorted.length-1].x;
                anns['holiday_last'] = {
                    type: 'box', xMin: start, xMax: end,
                    backgroundColor: 'rgba(153, 102, 255, 0.15)', borderWidth: 0, drawTime: 'beforeDatasetsDraw'
                };
            }
            currentAnnotations.value = anns;
        };

        const updateStats = (data) => {
            const valid = data.filter(d => d.y !== null);
            if (!valid.length) return;
            const last = valid[valid.length-1];
            const prev = valid.length >= 2 ? valid[valid.length-2] : null;
            
            stats.latestDate = last.x;
            stats.latestValue = last.y;
            stats.prevDate = prev ? prev.x : '-';
            stats.prevValue = prev ? prev.y : null;
        };

        onMounted(() => {
            loadHistory();
            loadPredictions();
            loadRaw(); // Preload Raw
            loadMarket(); // Preload Market
        });

        return {
            years, filterYear, setQuickRange,
            updateData, runPrediction, runSniper, runChallenger,
            allData, predictions, challengerData, rawData, marketData, validationHistory,
            stats, predictionState, updatePredictionDisplay,
            activeTab, showModal, showSniperModal, sniperResult,
            currentChartData, currentAnnotations, chartRef,
            onPredictionDateChange, loadRaw
        };
    }
};

const app = createApp(App);
app.mount('#app');
