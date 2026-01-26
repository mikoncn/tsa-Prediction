import { createApp, ref, onMounted, computed, reactive, watch } from 'https://unpkg.com/vue@3/dist/vue.esm-browser.js';
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
        
        // [NEW] Global Metrics for Probability Ranges
        const globalMetrics = reactive({
            maxError: 0,
            avgError: 0
        });
        
        const years = ref([]);
        const activeTab = ref('validation');
        const showModal = ref(false); // Export Modal
        const showSniperModal = ref(false);
        const isUpdating = ref(false); // [NEW] Loading State
        
        const stats = reactive({
            latestDate: '-', latestValue: null,
            prevDate: '-', prevValue: null
        });
        
        const predictionState = reactive({
            options: [],
            selectedDate: '',
            value: null
        });
        
        // [NEW] Weekly Prediction State
        const weeklyState = reactive({
            selected: '',
            options: [],
            sum: null,
            breakdown: [], 
            showChallenger: false, // [NEW] Toggle state
            challengerSum: null,   // [NEW] Comparison sum
            challengerBreakdown: [], // [NEW] Comparison list
            ranges: { standard: null, challenger: null } // [NEW] Probability ranges
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
                
                // [NEW] Generate Weekly Options and Sum
                console.log("Calling generateWeeklyOptions with", combined.length, "items");
                generateWeeklyOptions();
                
                // 2. Validation Table
                if (res.validation) {
                    validationHistory.value = res.validation.reverse();
                    
                    // Update global metrics
                    if (validationHistory.value.length > 0) {
                        const rates = validationHistory.value.map(d => parseFloat(d.error_rate) || 0);
                        globalMetrics.maxError = Math.max(...rates);
                        globalMetrics.avgError = rates.reduce((a, b) => a + b, 0) / rates.length;
                    }
                }
                
                // [NEW] Load Persisted Sniper Prediction
                if (res.sniper_latest) {
                    console.log("Loaded Persisted Sniper Data:", res.sniper_latest);
                    sniperResult.date = res.sniper_latest.date;
                    sniperResult.value = res.sniper_latest.predicted_throughput;
                    sniperResult.flights = res.sniper_latest.flight_volume;
                    sniperResult.is_fallback = res.sniper_latest.is_fallback;
                    // Note: We don't auto-show the modal, but the weekly calc will pick it up via watcher.
                }
                
            } catch (e) { console.error(e); }
        };

        // [NEW] Generate Weekly Options (Mon-Sun) based on ALL predictions
        const generateWeeklyOptions = () => {
            if (!predictions.value.length) {
                console.warn("No predictions to generate options");
                return;
            }
            
            // Get active market keys for filtering
            // keys example: "january-19-january-25"
            // We want to match against these to show only relevant weeks
            const activeKeys = Object.keys(marketData.value || {});
            console.log("Active Market Keys:", activeKeys);
            
            // Find all Mondays in the dataset
            const mondayMap = new Set();
            predictions.value.forEach(p => {
                const parts = p.x.split('-');
                const d = new Date(parts[0], parts[1]-1, parts[2]);
                const day = d.getDay();
                const diff = day === 0 ? 6 : day - 1; 
                const monday = new Date(d);
                monday.setDate(d.getDate() - diff);
                mondayMap.add(monday.getFullYear() + '-' + String(monday.getMonth()+1).padStart(2, '0') + '-' + String(monday.getDate()).padStart(2, '0'));
            });
            
            const sortedMondays = Array.from(mondayMap).sort((a,b) => new Date(b) - new Date(a));
            
            weeklyState.options = [];
            
            sortedMondays.forEach(mStr => {
                const parts = mStr.split('-');
                const m = new Date(parts[0], parts[1]-1, parts[2]);
                const s = new Date(m);
                s.setDate(m.getDate() + 6); // Sunday
                
                // 1. Display Label: "1月19日 - 1月25日" (Chinese)
                const labelStart = m.toLocaleDateString('zh-CN', {month:'numeric', day:'numeric'});
                const labelEnd = s.toLocaleDateString('zh-CN', {month:'numeric', day:'numeric'});
                const displayLabel = `${labelStart} - ${labelEnd}`;
                
                // 2. Match Key Construction (to match "january-19-january-25")
                // Needs Full English Month Name Lowercase
                const monName = m.toLocaleDateString('en-US', {month:'long'}).toLowerCase();
                const sunName = s.toLocaleDateString('en-US', {month:'long'}).toLowerCase();
                const matchKey = `${monName}-${m.getDate()}-${sunName}-${s.getDate()}`;
                
                // Filter: Only add if matchKey exists in marketData
                // OR if it's the current week (optional fallback)
                // User said: "只有那个盘里能显示的选项，那个盘里有这个选项的时候，我们才显示"
                // So strict matching is safer.
                
                // Debug log
                // console.log(`Checking ${matchKey} against active keys`);
                
                if (activeKeys.includes(matchKey)) {
                    weeklyState.options.push({
                        label: displayLabel,
                        value: mStr // The Monday date string for calculation
                    });
                }
            });
            
            if (weeklyState.options.length > 0) {
                 if (predictionState.selectedDate) {
                     // Try to sync with selected daily date
                     const target = new Date(predictionState.selectedDate);
                     const day = target.getDay();
                     const diff = day === 0 ? 6 : day - 1;
                     const targetMon = new Date(target);
                     targetMon.setDate(target.getDate() - diff);
                     const tStr = targetMon.getFullYear() + '-' + String(targetMon.getMonth()+1).padStart(2,'0') + '-' + String(targetMon.getDate()).padStart(2,'0');
                     
                     if (weeklyState.options.some(o => o.value === tStr)) {
                         weeklyState.selected = tStr;
                     } else {
                         weeklyState.selected = weeklyState.options[0].value;
                     }
                } else {
                    weeklyState.selected = weeklyState.options[0].value;
                }
                calculateWeeklySum(weeklyState.selected);
            } else {
                // If no match found (e.g. data not loaded yet), add a fallback or leave empty?
                // Maybe the keys in marketData are slightly different?
                // Let's add a debug fallback if empty so user sees SOMETHING in dev
                console.warn("No matching weekly options found. Keys available:", activeKeys);
                
                // Double check if keys might be swapped or format diff "january-26-february-1"
                // logic covers cross-month since we use sunName.
            }
        };

        // [NEW] Helper to sum Mon-Sun traffic for a specific Monday
        // Hybrid Mode: Uses Actual History if available, otherwise Forecast
        const calculateWeeklySum = (mondayStr) => {
            if (!mondayStr) return;
            // Robust parsing
            const parts = mondayStr.split('-');
            const monday = new Date(parts[0], parts[1]-1, parts[2]);
            
            let sum = 0;
            let count = 0;
            
            let breakdownList = [];
            let challengerList = []; // [NEW] Challenger comparison list
            let challengerSum = 0;
            let challengerCount = 0;
            
            // Debug Log
            console.log("Calculating Weekly. Sniper State:", sniperResult.date, sniperResult.value);

            // Sum next 7 days starting from Monday
            for (let i = 0; i < 7; i++) {
                const d = new Date(monday);
                d.setDate(monday.getDate() + i);
                const dateStr = d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0');
                const label = d.toLocaleDateString('zh-CN', {month:'numeric', day:'numeric'});
                
                // --- Logic A: Standard Breakdown ---
                let valA = 0;
                let typeA = 'prediction';
                let labelA = label;

                // 1. Try Actual Data First
                const actual = allData.value.find(x => x.x === dateStr);
                
                if (actual && actual.y !== null) {
                    valA = actual.y;
                    typeA = 'history';
                } 
                // 2. Check Sniper Result (High Priority Forecast)
                else if (sniperResult.date === dateStr && sniperResult.value) {
                    valA = sniperResult.value;
                    typeA = 'sniper';
                    labelA += ' (Sniper)';
                }
                // 3. Fallback to General Prediction
                else {
                    const p = predictions.value.find(x => x.x === dateStr);
                    if (p && p.y) valA = p.y;
                }
                
                if (valA > 0) {
                    sum += valA;
                    count++;
                    breakdownList.push({ date: dateStr, label: labelA, value: valA, type: typeA });
                }

                // --- Logic B: Challenger Breakdown ---
                // Base logic is SAME as A (History & Sniper are absolute truth/nowcast)
                // Difference is only in Step 3 (Forecast Source)
                let valB = valA; 
                let typeB = typeA; 
                let labelB = labelA;

                if (typeA === 'prediction') { // Only diverge if we are using the generic forecast
                     // Try find Challenger Data
                     const ch = challengerData.value.find(x => x.x === dateStr);
                     if (ch && ch.y) {
                         valB = Math.round(ch.y); // [FIX] Round to int
                         typeB = 'challenger';
                         labelB = label + ' (Challenger)';
                     }
                }
                
                if (valB > 0) {
                    challengerSum += valB;
                    challengerCount++;
                    challengerList.push({ date: dateStr, label: labelB, value: valB, type: typeB });
                }
            }
            
            // [NEW] Calculate Probability Ranges
            const calcRanges = (total, list, errorRate) => {
                if (!total || errorRate === 0) return null;
                // Calculate delta based ONLY on forecast components (sniper, prediction, challenger)
                let forecastDelta = 0;
                list.forEach(item => {
                    if (item.type !== 'history') {
                        forecastDelta += item.value * (errorRate / 100);
                    }
                });
                return {
                    min: Math.round(total - forecastDelta),
                    max: Math.round(total + forecastDelta)
                };
            };

            // Update stats
            weeklyState.sum = count > 0 ? sum : null;
            weeklyState.breakdown = breakdownList;
            
            // [NEW] Update Challenger Stats
            weeklyState.challengerSum = challengerCount > 0 ? challengerSum : null;
            weeklyState.challengerBreakdown = challengerList;

            // [NEW] Populate Ranges
            weeklyState.ranges.standard = {
                maxError: calcRanges(sum, breakdownList, globalMetrics.maxError),
                avgError: calcRanges(sum, breakdownList, globalMetrics.avgError)
            };
            weeklyState.ranges.challenger = {
                maxError: calcRanges(challengerSum, challengerList, globalMetrics.maxError),
                avgError: calcRanges(challengerSum, challengerList, globalMetrics.avgError)
            };
        };
        
        const updateWeeklyState = (val) => {
            weeklyState.selected = val;
            calculateWeeklySum(val);
        };

        const updatePredictionDisplay = (date) => {
            if (!date) return;
            // Find in options first (Forecast)
            const opt = predictionState.options.find(o => o.date === date);
            if (opt) {
                predictionState.value = opt.value;
            } else {
                // Try finding in history/combined
                const p = predictions.value.find(x => x.x === date);
                predictionState.value = p ? p.y : null;
            }
        };

        // Watch dropdown change
        const onPredictionDateChange = (val) => {
            predictionState.selectedDate = val;
            updatePredictionDisplay(val);
            calculateWeeklySum(val); // [NEW] Sync weekly stat
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

        const isSyncingMarket = ref(false);
        const syncMarket = async () => {
            isSyncingMarket.value = true;
            try {
                const res = await API.syncMarketSentiment();
                if (res.status === 'success') {
                    await loadMarket();
                    alert('⚡ 市场赔率已实时同步！');
                } else {
                    alert('❌ 同步失败: ' + res.message);
                }
            } catch (e) {
                alert('❌ 同步出错: ' + e);
            } finally {
                isSyncingMarket.value = false;
            }
        };

        const updateData = async () => {
            if(!confirm('确定要更新数据吗？全流程约需 30-60 秒。\n请耐心等待按钮状态变更。')) return;
            
            isUpdating.value = true;
            try {
                const res = await API.updateData();
                if(res.status === 'success') {
                    alert('✅ 更新成功! \n' + (res.message || '数据已刷新'));
                    loadHistory();
                    loadPredictions();
                } else {
                    alert('❌ 更新失败: ' + res.message);
                }
            } catch (e) { 
                alert('❌ 网络超时或错误: ' + e); 
            } finally {
                isUpdating.value = false;
            }
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
                    // [FIX] Force update weekly calculation immediately
                    generateWeeklyOptions();
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
                        challengerData.value = res.data.forecast.map(i => ({ x: i.date, y: i.forecast }));
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
        
        // [NEW] Reactive Watcher to handle async data loading
        // Ensures options are generated only when ALL data is ready
        watch([predictions, marketData, allData, sniperResult, challengerData], () => {
             console.log("Data updated (Predictions, Market, History, Sniper, or Challenger), regenerating weekly options...");
             generateWeeklyOptions();
        });

        return {
            years, filterYear, setQuickRange,
            updateData, runPrediction, runSniper, runChallenger,
            allData, predictions, challengerData, rawData, marketData, validationHistory,
            stats, predictionState, updatePredictionDisplay,
            activeTab, showModal, showSniperModal, sniperResult,
            currentChartData, currentAnnotations, chartRef,
            onPredictionDateChange, loadRaw, isUpdating,
            isSyncingMarket, syncMarket,
            weeklyState, updateWeeklyState
        };
    }
};

const app = createApp(App);
app.mount('#app');
