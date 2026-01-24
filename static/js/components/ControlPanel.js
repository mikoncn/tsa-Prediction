export default {
    name: 'ControlPanel',
    props: ['years'],
    emits: ['update-range', 'run-prediction', 'update-data', 'run-sniper', 'run-challenger', 'filter-year'],
    setup(props, { emit }) {
        return { emit };
    },
    template: `
    <div class="controls">
        <button @click="emit('update-range', 365)" class="active">最近1年</button>
        <button @click="emit('update-range', 180)">最近半年</button>
        <button @click="emit('update-range', 30)">最近30天</button>
        
        <div class="divider"></div>
        
        <button @click="emit('run-prediction')" style="background-color: #28a745; color: white;">🚀 立即预测</button>
        <button @click="emit('update-data')" style="background-color: #17a2b8; color: white; margin-left: 10px;">🔄 更新数据</button>
        <button @click="emit('run-sniper')" style="background-color: #dc3545; color: white; margin-left: 10px; font-weight: bold;">🎯 智能狙击 (Smart Sniper)</button>
        <button @click="emit('run-challenger')" style="background-color: #6f42c1; color: white; margin-left: 10px; font-weight: bold;">🟣 深度对决 (Deep Comparison)</button>

        <select @change="emit('filter-year', $event.target.value)">
            <option value="all">全部年份</option>
            <option v-for="year in years" :key="year" :value="year">{{ year }}年</option>
        </select>

        <button @click="emit('reset-zoom')" style="margin-left: 20px;">重置缩放</button>
    </div>
    `
};
