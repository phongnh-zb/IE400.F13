// webapp/static/js/index.js

// Khởi tạo biến Chart toàn cục để quản lý instance
let pieChart = null;
let scatterChart = null;

async function fetchData() {
  try {
    const response = await fetch("/api/realtime-data");
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    const result = await response.json();
    updateDashboard(result);
  } catch (error) {
    console.error("Lỗi lấy dữ liệu từ Server:", error);
  }
}

function updateDashboard(data) {
  // 1. Cập nhật số liệu thống kê (Text)
  // Kiểm tra phần tử tồn tại trước khi gán để tránh lỗi null
  const elTotal = document.getElementById("total-count");
  const elRisk = document.getElementById("risk-count");
  const elSafe = document.getElementById("safe-count");

  if (elTotal) elTotal.innerText = data.summary.total;
  if (elRisk) elRisk.innerText = data.summary.risk;
  if (elSafe) elSafe.innerText = data.summary.safe;

  // 2. Cập nhật Pie Chart (Biểu đồ tròn)
  const ctxPie = document.getElementById("riskPieChart").getContext("2d");

  // Nếu biểu đồ đã tồn tại thì hủy đi vẽ lại (tránh bị đè hình)
  if (pieChart) {
    pieChart.destroy();
  }

  pieChart = new Chart(ctxPie, {
    type: "doughnut",
    data: {
      labels: ["Nguy cơ", "An toàn"],
      datasets: [
        {
          data: [data.summary.risk, data.summary.safe],
          backgroundColor: ["#dc3545", "#28a745"],
          borderWidth: 1,
        },
      ],
    },
    options: {
      responsive: true,
      plugins: {
        legend: { position: "bottom" },
      },
    },
  });

  // 3. Cập nhật Scatter Chart (Biểu đồ phân tán)
  // Chuẩn bị dữ liệu: x=Score, y=Clicks
  const scatterDataRisk = data.raw_data
    .filter((d) => d.risk === 1)
    .map((d) => ({ x: d.score, y: d.clicks }));

  const scatterDataSafe = data.raw_data
    .filter((d) => d.risk === 0)
    .map((d) => ({ x: d.score, y: d.clicks }));

  const ctxScatter = document.getElementById("scatterChart").getContext("2d");

  if (scatterChart) {
    scatterChart.destroy();
  }

  scatterChart = new Chart(ctxScatter, {
    type: "scatter",
    data: {
      datasets: [
        {
          label: "Nguy cơ bỏ học (Risk)",
          data: scatterDataRisk,
          backgroundColor: "#dc3545", // Đỏ
          pointRadius: 4,
        },
        {
          label: "Học tốt (Safe)",
          data: scatterDataSafe,
          backgroundColor: "#28a745", // Xanh
          pointRadius: 4,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: "bottom" },
        tooltip: {
          callbacks: {
            label: function (context) {
              return `ID: ${context.raw.id || "N/A"} (Score: ${
                context.raw.x
              }, Clicks: ${context.raw.y})`;
            },
          },
        },
      },
      scales: {
        x: {
          title: { display: true, text: "Điểm trung bình (Score)" },
          min: 0,
          max: 100,
        },
        y: {
          title: { display: true, text: "Tổng số Click" },
          beginAtZero: true,
        },
      },
    },
  });
}

// --- MAIN EXECUTION ---
document.addEventListener("DOMContentLoaded", () => {
  console.log("Dashboard initialized. Waiting for data...");

  // Gọi lần đầu ngay khi trang tải xong
  fetchData();

  // Tự động refresh mỗi 15 giây (Real-time Simulation)
  setInterval(fetchData, 15000);
});
