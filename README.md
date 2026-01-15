# TSA 全美航空客流高精度分析系统 (Mikon AI Scout Edition)

> **Governance by Data, For the Prediction.**

本项目是一套集**自动化数据采集**、**交互式可视化**与**高维度特征工程**于一体的航空客流分析系统。它专为捕捉“黑天鹅”事件（如极端天气、突发疫情）及复杂节假日效应而设计，为 Prophet/XGBoost 等预测模型提供高质量的“燃料”。

---

## 🏗️ 架构全景 (v2.0 Logic)

系统采用 **ETL + 混合模型 (Hybrid Model)** 架构：

```mermaid
graph LR
    A[TSA 官网] -->|Scraper| B(SQLite 主数据库)
    C[Open-Meteo 气象局] -->|API| D(气象特征表)
    E[Holidays 库] -->|Advanced Logic| F(节日特征: 正日/窗口)
    B --> G{Merge Engine}
    D --> G
    F --> G
    G -->|Generate| H[TSA_Final_Analysis.csv]

    H --> I[🔵 Prophet (趋势模型)]
    H --> J[🔴 XGBoost (高精模型)]

    J -->|Forecast| K[未来 7 日预测]
    J -->|Validation| L[历史回测报告]

    I & J --> M[Dashboard (Web)]
```

## 🧩 核心算法详解 (The Secret Sauce)

### 1. 节日双重逻辑 (Dual Holiday Feature)

为了解决模型在节日当天的预测偏差，我们引入了高级特征：

- **Is_Holiday_Exact_Day (正日)**: 标记感恩节、圣诞节当天。模型学会了**"正日不出门"**的抑制逻辑（系数为负）。
- **Is_Holiday_Travel_Window (窗口期)**: 标记节日前后 7 天。模型学会了**"节前大迁徙"**的激增逻辑（系数为正）。

### 2. 多枢纽熔断气象模型 (Hub Meltdown Model)

监测 5 大枢纽（ATL, ORD, DFW, DEN, JFK）的暴雪、强风、暴雨。若 3 个以上枢纽同时恶劣天气，触发**"系统熔断"**信号。

---

## 🛠️ 项目结构

- `update_data.bat`: **一键司令部**. 串联爬虫、天气、融合、训练全流程。
- `merge_db.py`: **核心融合器**. 生成 Feature A/B，处理 Holiday/Weather/Lags。
- `train_xgb.py`: **XGBoost 引擎**. 负责高精度预测 + 2025 全年回测。
- `train_model.py`: **Prophet 引擎**. 负责长期趋势分析。
- `app.py`: **Web 后端**.
- `backtest_2025_full.py`: **压力测试**. 盲测 2025 全年数据。

**已移除**: `add_features.py`, `export_table.py` (功能已集成至 Merge DB)。

## 🚀 快速开始

**日常更新 (Daily Routine)**:

只需双击运行：

```bash
./update_data.bat
```

它会自动：

1. 抓取最新 TSA 数据
2. 更新天气
3. 重训模型
4. 生成未来 7 天预测 (`xgb_forecast.csv`)

**启动看板**:

```bash
python app.py
# 访问 http://127.0.0.1:5000
```

## 🔮 路线图 (Roadmap)

- [x] **超级碗 (Super Bowl)**: 算法已实装。
- [x] **模型接入**: Prophet + XGBoost 双核驱动。
- [x] **节日逻辑升级**: 正日 vs 窗口期分离。
- [x] **前端升级**: 支持自由日期选择 & 准确率回测面板。

---

_Mikon AI Army Engineer Division_
