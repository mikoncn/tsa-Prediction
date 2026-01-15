let chart;
let allData = [];
let availableYears = new Set();

// 初始化图表
function initChart() {
  const ctx = document.getElementById("trafficChart").getContext("2d");
  chart = new Chart(ctx, {
    type: "line",
    data: {
      datasets: [
        {
          label: "旅客吞吐量",
          data: [],
          borderColor: "#007bff",
          backgroundColor: "rgba(0, 123, 255, 0.1)",
          borderWidth: 2,
          fill: true,
          tension: 0.3,
          pointRadius: 2,
          pointHoverRadius: 6,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        intersect: false,
        mode: "index",
      },
      scales: {
        x: {
          type: "time",
          time: {
            unit: "month",
            displayFormats: {
              month: "yyyy年MM月",
              day: "MM-dd (EEE)",
            },
            tooltipFormat: "yyyy-MM-dd",
          },
          title: {
            display: true,
            text: "日期",
          },
        },
        y: {
          title: {
            display: true,
            text: "人次",
          },
          ticks: {
            callback: function (value) {
              return (value / 1000000).toFixed(1) + "M";
            },
          },
        },
      },
      plugins: {
        tooltip: {
          callbacks: {
            title: function (context) {
              const date = new Date(context[0].parsed.x);
              return new Intl.DateTimeFormat("zh-CN", {
                year: "numeric",
                month: "2-digit",
                day: "2-digit",
                weekday: "short",
              }).format(date);
            },
            label: function (context) {
              return (
                " 旅客: " + new Intl.NumberFormat().format(context.parsed.y)
              );
            },
          },
        },
        zoom: {
          pan: {
            enabled: true,
            mode: "x",
            modifierKey: null,
          },
          zoom: {
            wheel: {
              enabled: true,
            },
            pinch: {
              enabled: true,
            },
            mode: "x",
          },
          limits: {
            x: { min: "original", max: "original" },
          },
        },
      },
    },
  });
}

// 加载数据
async function loadData() {
  try {
    const response = await fetch("/api/data");
    const data = await response.json();

    // 转换数据格式
    allData = data.map((item) => ({
      x: item.date,
      y: item.throughput,
    }));

    // 提取年份用于下拉框
    allData.forEach((item) => {
      const year = item.x.split("-")[0];
      availableYears.add(year);
    });
    populateYearSelect();

    // 默认显示全部
    updateChart(allData);
    updateStats(allData);
  } catch (error) {
    console.error("Error loading data:", error);
    alert("加载数据失败，请检查后端服务是否启动。");
  }
}

// 填充年份下拉框
function populateYearSelect() {
  const select = document.getElementById("yearSelect");
  // 排序年份 (降序)
  const sortedYears = Array.from(availableYears).sort((a, b) => b - a);

  sortedYears.forEach((year) => {
    const option = document.createElement("option");
    option.value = year;
    option.textContent = year + "年";
    select.appendChild(option);
  });
}

// 更新图表数据
function updateChart(data) {
  chart.data.datasets[0].data = data;

  // 自动调整 x 轴单位
  if (data.length <= 60) {
    chart.options.scales.x.time.unit = "day";
  } else {
    chart.options.scales.x.time.unit = "month";
  }

  chart.update();
  chart.resetZoom();
}

function resetZoom() {
  chart.resetZoom();
}

// 快捷范围选择
function setQuickRange(range) {
  // UI 状态
  document
    .querySelectorAll("button")
    .forEach((btn) => btn.classList.remove("active"));
  const btn = document.getElementById(`btn-${range}`);
  if (btn) btn.classList.add("active");

  // 重置下拉框
  document.getElementById("yearSelect").value = "all";
  document.getElementById("monthSelect").value = "all";

  if (range === "all") {
    updateChart(allData);
    updateStats(allData);
  } else {
    const now = new Date();
    let cutoffDate = new Date();

    if (range === "14d") {
      cutoffDate.setDate(now.getDate() - 14);
    } else if (range === "7d") {
      cutoffDate.setDate(now.getDate() - 7);
    }

    const filteredData = allData.filter(
      (item) => new Date(item.x) >= cutoffDate
    );
    updateChart(filteredData);
    updateStats(filteredData);
  }
}

// 级联筛选逻辑
function applyFilters() {
  // 清除快捷按钮状态
  document
    .querySelectorAll("button")
    .forEach((btn) => btn.classList.remove("active"));

  const selectedYear = document.getElementById("yearSelect").value;
  const selectedMonth = document.getElementById("monthSelect").value;

  let filteredData = allData;

  // 1. 年份筛选
  if (selectedYear !== "all") {
    filteredData = filteredData.filter((item) =>
      item.x.startsWith(selectedYear)
    );
  }

  // 2. 月份筛选
  if (selectedMonth !== "all") {
    if (selectedYear !== "all") {
      const prefix = `${selectedYear}-${selectedMonth}`;
      filteredData = filteredData.filter((item) => item.x.startsWith(prefix));
    } else {
      const monthStr = `-${selectedMonth}-`;
      filteredData = filteredData.filter((item) => item.x.includes(monthStr));
    }
  }

  updateChart(filteredData);
  updateStats(filteredData);
}

// 更新统计信息
function updateStats(data) {
  if (data.length === 0) {
    document.getElementById("latest-val").textContent = "-";
    document.getElementById("max-val").textContent = "-";
    document.getElementById("avg-val").textContent = "-";
    return;
  }

  // 最新值
  const latest = data[data.length - 1].y;
  document.getElementById("latest-val").textContent =
    new Intl.NumberFormat().format(latest);

  // 峰值
  const max = Math.max(...data.map((d) => d.y));
  document.getElementById("max-val").textContent =
    new Intl.NumberFormat().format(max);

  // 平均值
  const avg = data.reduce((a, b) => a + b.y, 0) / data.length;
  document.getElementById("avg-val").textContent =
    new Intl.NumberFormat().format(Math.round(avg));
}

// 启动
document.addEventListener("DOMContentLoaded", () => {
  initChart();
  loadData();
});
