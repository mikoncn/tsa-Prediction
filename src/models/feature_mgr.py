
# feature_mgr.py - 特征与业务逻辑统一管理器
# 这是全项目模型特征的“唯一事实来源”(Source of Truth)

# 1. 影子模型特征 (Shadow Model Features)
SHADOW_FEATURES = [
    'max_snow', 'mean_snow', 
    'max_snow_sq', 'mean_snow_sq',
    'max_wind', 'mean_wind', 
    'max_precip', 'mean_precip', 
    'min_temp', 'mean_temp', 
    'national_severity', 'month', 'day_of_year'
]

# 2. 老模型核心特征集 (Classic v2 Features)
FEAT_CLASSIC_V2 = [
    'day_of_week', 'month', 'year', 'day_of_year', 'week_of_year', 'is_weekend',
    'is_holiday', 'is_holiday_exact_day', 'is_holiday_travel_window', 
    'lag_7_clean', 'lag_holiday_yoy', 'weather_index',
    'is_off_peak_workday', 'days_to_nearest_holiday', 'is_long_weekend',
    'holiday_intensity'
]

# 3. 新模型核心特征集 (Hybrid Features)
# 注意：Hybrid 模型是 Classic v2 的超集，外加感应特征
FEAT_HYBRID = FEAT_CLASSIC_V2 + [
    'predicted_cancel_rate',     # 影子模型输出
    'revenge_index',             # 报复性反弹指数
    'lag_7_adjusted',            # 取消率修正后的滞后 7
    'lag_364_adjusted',          # 取消率修正后的滞后 364
    'lead_1_shadow_cancel_rate'  # 恐惧特征 (预判明天)
]

# 4. 业务逻辑与熔断阈值 (Circuit Breakers - Scheme B)
def apply_blind_protocol(base_pred, row, baseline_pred=None):
    """
    方案 B：动态补位逻辑
    最终预测 = min(模型原始预测, 正常基准 * (1 - 熔断比例))
    
    Args:
        base_pred: 模型原始预测 (XGBoost 输出)
        row: 当前行的特征字典
        baseline_pred: 正常水平基准 (如 lag_7)。若为 None 则退化为原有的独立乘法。
    """
    w_idx = row.get('weather_index', 0)
    w_lag_1 = row.get('w_lag_1', 0) 
    lead_1 = row.get('lead_1_shadow_cancel_rate', 0)
    
    # 基础熔断系数 (Rule Penalty)
    rule_multiplier = 1.0
    
    # 1. Blind Protocol (Today) - 线性插值逻辑 (Scheme B + Smooth Curve)
    # 从 Index 10 开始计算惩罚，每点 2%，在 Index 20 处封顶达到 -20% (0.80)
    if w_idx >= 10:
        interpolation_multiplier = 1.0 - (w_idx - 10) * 0.02
        rule_multiplier = max(0.80, min(1.0, interpolation_multiplier))
    else:
        rule_multiplier = 1.0
        
    # 2. 额外叠加因子 (这些通常是模型难以预见的突发效应)
    # 宿醉效应 (-10%)
    if w_lag_1 >= 30: rule_multiplier *= 0.90
    # 恐惧效应 (-10%)
    if lead_1 > 0.20: rule_multiplier *= 0.90
    
    # --- 核心逻辑切换 (Scheme B: Refined) ---
    if rule_multiplier < 1.0 and baseline_pred is not None and baseline_pred > 0:
        # 仅在有灾难规则触发时，才启用补位逻辑
        # 补位逻辑：至少要降到 baseline * rule_multiplier 这个水位
        # 但如果模型自己已经降得更多了，就听模型的
        floor_value = int(baseline_pred * rule_multiplier)
        final_pred = min(int(base_pred), floor_value)
    else:
        # 正常日子，或者没有基准值，走老的逻辑 (如果是正常日，rule_multiplier=1 则等于原值)
        final_pred = int(base_pred * rule_multiplier)
        
    return final_pred

# 5. 辅助函数：特征对齐检查
def validate_features(df, model_type='HYBRID'):
    """确保 DataFrame 包含了模型所需的所有特征"""
    target_list = FEAT_HYBRID if model_type == 'HYBRID' else FEAT_CLASSIC_V2
    missing = [f for f in target_list if f not in df.columns]
    if missing:
        raise ValueError(f"特征丢失! 模型 {model_type} 缺少以下特征: {missing}")
    return True
