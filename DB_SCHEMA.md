# 数据库结构与迁移规范 (Database Schema & Migration Protocol)

为了实现数据流的“全数据库化”，我们将统一使用 SQLite (`data/tsa_data.db`) 作为唯一数据存储，移除所有中间 CSV 文件。

本文档定义了当前的数据库结构以及为了支持“Weather-in-DB”而设计的新表结构。

## 1. 核心数据表 (Core Tables)

### `traffic` (原始客流表)

存储从 TSA 官网抓取的原始数据。

- **用途**: 数据源头 (Source of Truth)。
- **更新频率**: 每日增量更新 (`build_tsa_db.py`)。

| Column Name    | Type        | Description       |
| :------------- | :---------- | :---------------- |
| **date**       | `TEXT` (PK) | 日期 (YYYY-MM-DD) |
| **throughput** | `INTEGER`   | TSA 每日安检人数  |

---

### `bts_traffic` (BTS 历史统计表)

**[NEW]** 存储 2019年至今的 12 大枢纽每日航班聚合数据。

- **用途**: 模型训练的高质量特征源 (Feature Source)。
- **更新频率**: 可通过 `scripts/import_bts_to_db.py` 导入。

| Column Name         | Type        | Description              |
| :------------------ | :---------- | :----------------------- |
| **date**            | `TEXT` (PK) | 日期 (YYYY-MM-DD)        |
| **total_flights**   | `INTEGER`   | 12 枢纽总计划航班量      |
| **total_cancelled** | `INTEGER`   | 总取消架次               |
| **avg_delay**       | `REAL`      | 平均起飞延误 (分钟)      |
| **cancel_rate**     | `REAL`      | 取消率 (Cancelled/Total) |
| **updated_at**      | `TIMESTAMP` | 入库时间                 |

---

### `flight_stats` (航班数据表)

存储从 OpenSky Network 抓取的每日航班起降数据。

- **用途**: 模型输入特征 (Sniper Model)。
- **更新频率**: 每日增量更新 (`fetch_opensky.py`)。

| Column Name       | Type        | Description              |
| :---------------- | :---------- | :----------------------- |
| **date**          | `TEXT` (PK) | 日期 (YYYY-MM-DD)        |
| **airport**       | `TEXT` (PK) | 机场 ICAO 代码 (如 KATL) |
| **arrival_count** | `INTEGER`   | 当日抵达航班数           |
| **updated_at**    | `TIMESTAMP` | 记录更新时间             |

---

### `weather` (☁️ 新增表 - 替代 weather_features.csv)

**[NEW]** 存储 OpenMeteo 历史及预测天气数据。

- **用途**: 替代 CSV，作为天气特征的唯一存储。
- **更新频率**: 每次运行 `get_weather_features.py` 时全量/增量更新。

| Column Name           | Type        | Description          |
| :-------------------- | :---------- | :------------------- |
| **date**              | `TEXT` (PK) | 日期 (YYYY-MM-DD)    |
| **airport**           | `TEXT` (PK) | 机场代码 (如 ORD)    |
| **snowfall_cm**       | `REAL`      | 降雪量 (cm)          |
| **windspeed_kmh**     | `REAL`      | 最大风速 (km/h)      |
| **precipitation_mm**  | `REAL`      | 降雨量 (mm)          |
| **temperature_min_c** | `REAL`      | 最低气温 (°C) [NEW]  |
| **severity_score**    | `INTEGER`   | 机场单点恶劣天气评分 |
| **updated_at**        | `TIMESTAMP` | 数据抓取时间         |

---

### `daily_weather_index` (全美天气指数表)

**[NEW]** 存储每日全美航空加权天气指数。

- **用途**: 缓存计算后的天气指数，用于快速查询和模型输入。
- **更新频率**: 每日更新 (`get_weather_features.py`).

| Column Name       | Type        | Description       |
| :---------------- | :---------- | :---------------- |
| **date**          | `TEXT` (PK) | 日期 (YYYY-MM-DD) |
| **weather_index** | `INTEGER`   | 全美天气熔断指数  |
| **updated_at**    | `TIMESTAMP` | 更新时间          |

---

## 2. 分析与模型表 (Analytics & Model Tables)

### `traffic_full` (全量宽表 - 替代 TSA_Final_Analysis.csv)

这是模型训练直接使用的主表，由 `merge_db.py` 生成。

- **用途**: XGBoost / Prophet / Flaml 的训练集。
- **更新频率**: 数据更新流程的最后一步 (`merge_db.py`)。

| Column Name                  | Type        | Description                          |
| :--------------------------- | :---------- | :----------------------------------- |
| **date**                     | `TEXT` (PK) | 日期                                 |
| **throughput**               | `REAL`      | 真实客流 (未来日期为 NULL)           |
| **weather_index**            | `INTEGER`   | 全美天气熔断指数 (聚合自 weather 表) |
| **is_holiday**               | `INTEGER`   | 是否节日 (0/1)                       |
| **holiday_name**             | `TEXT`      | 节日名称                             |
| **is_holiday_exact_day**     | `INTEGER`   | 正日标识                             |
| **is_holiday_travel_window** | `INTEGER`   | 窗口期标识                           |
| **is_spring_break**          | `INTEGER`   | 春假标识                             |
| **throughput_lag_7**         | `REAL`      | 7 天滞后特征                         |
| **flight_volume**            | `INTEGER`   | 航班量 (OpenSky)                     |
| **flight_ma_7**              | `INTEGER`   | 7日航班均值                          |
| **flight_lag_1**             | `INTEGER`   | 昨日航班量                           |

---

### `prediction_history` (预测记录表)

存储模型每日运行的预测结果，用于回测分析。

| Column Name              | Type           | Description                            |
| :----------------------- | :------------- | :------------------------------------- |
| **id**                   | `INTEGER` (PK) | 自增 ID                                |
| **target_date**          | `TEXT`         | 预测的目标日期                         |
| **predicted_throughput** | `INTEGER`      | 预测值                                 |
| **model_run_date**       | `TEXT`         | 模型运行日期                           |
| **weather_index**        | `INTEGER`      | [NEW] 预测日天气熔断指数               |
| **is_holiday**           | `INTEGER`      | [NEW] 是否节日 (0/1)                   |
| **holiday_name**         | `TEXT`         | [NEW] 节日名称 (用于前端 T-x 标签计算) |
| **flight_volume**        | `INTEGER`      | [NEW] 预测日航班量 (Lag-1)             |
| **is_weekend**           | `INTEGER`      | [NEW] 是否周末                         |
| **created_at**           | `TIMESTAMP`    | 记录创建时间                           |

---

### `sniper_predictions` (狙击模型结果缓存)

**[NEW]** 存储狙击模型的高频预测结果，用于前端持久化展示。

- **用途**: 保存 Sniper 预测值，防止刷新丢失；当官方数据通过 `traffic` 进来后，此表数据仅作为历史参考。
- **更新频率**: 每次点击“智能狙击”成功时插入 (`predict_sniper`)。

| Column Name         | Type           | Description              |
| :------------------ | :------------- | :----------------------- |
| **id**              | `INTEGER` (PK) | 自增 ID                  |
| **target_date**     | `TEXT`         | 目标日期 (YYYY-MM-DD)    |
| **predicted_value** | `INTEGER`      | 预测客流 (e.g. 2345678)  |
| **flights_volume**  | `INTEGER`      | 当日实时航班量 (架次)    |
| **model_version**   | `TEXT`         | 模型版本 (e.g. SniperV1) |
| **is_fallback**     | `INTEGER`      | 是否降级模式 (0/1)       |
| **created_at**      | `TIMESTAMP`    | 创建时间                 |

---

## 3. 迁移数据流变化 (Data Flow Change)

### Old Flow (Current)

1.  TSA -> `traffic` (DB)
2.  Weather API -> `weather_features.csv` ❌
3.  Merge Script -> Reads CSV & DB -> `TSA_Final_Analysis.csv` ❌
4.  Train Script -> Reads `TSA_Final_Analysis.csv` ❌

### New Flow (Target)

1.  TSA -> `traffic` (DB)
2.  Weather API -> `weather` (DB) ✅
3.  Merge Script -> Reads `traffic` & `weather` (DB) -> Write `traffic_full` (DB) ✅
4.  Train Script -> Reads `traffic_full` (DB) ✅

此架构确保了所有中间数据都在 SQLite 中流转，具备更好的 ACID 特性和查询能力。
