/**
 * students.js
 * Xử lý logic cho trang Danh sách sinh viên:
 * - Client-side Pagination (Phân trang)
 * - Filtering (Lọc theo Status & Search ID)
 * - URL Synchronization (Đồng bộ trạng thái lên URL)
 */

document.addEventListener("DOMContentLoaded", () => {
  console.log("Students Manager Loaded successfully.");

  // --- 1. DOM ELEMENTS ---
  const searchInput = document.getElementById("studentSearch");
  const statusFilter = document.getElementById("statusFilter");
  const tableBody = document.getElementById("studentTableBody");
  const noResultsDiv = document.getElementById("noResults");
  const pageSizeSelect = document.getElementById("pageSizeSelect");

  // Pagination Elements
  const startRowEl = document.getElementById("startRow");
  const endRowEl = document.getElementById("endRow");
  const totalRowsEl = document.getElementById("totalRows");
  const prevBtn = document.getElementById("prevBtn");
  const nextBtn = document.getElementById("nextBtn");

  // Lấy toàn bộ dòng dữ liệu gốc từ HTML ban đầu
  const allRows = Array.from(tableBody.getElementsByTagName("tr"));

  // --- 2. INIT STATE FROM URL ---
  // Đọc tham số từ URL để giữ trạng thái khi reload hoặc back
  const urlParams = new URLSearchParams(window.location.search);

  let currentPage = parseInt(urlParams.get("page")) || 1;
  let rowsPerPage = parseInt(urlParams.get("page_size")) || 10;

  // Set giá trị mặc định cho dropdown đúng với URL
  if (pageSizeSelect) pageSizeSelect.value = rowsPerPage;

  // Danh sách sau khi lọc (Ban đầu bằng danh sách gốc)
  let filteredRows = [...allRows];

  // --- 3. CORE FUNCTIONS ---

  /**
   * Cập nhật URL trình duyệt mà không reload trang
   */
  function updateURL() {
    const newUrl = new URL(window.location);
    newUrl.searchParams.set("page", currentPage);
    newUrl.searchParams.set("page_size", rowsPerPage);

    // Dùng pushState để lưu history
    window.history.pushState({ path: newUrl.href }, "", newUrl.href);
  }

  /**
   * Render bảng dựa trên currentPage và filteredRows
   */
  function renderTable() {
    // 1. Ẩn tất cả dòng trước
    allRows.forEach((row) => (row.style.display = "none"));

    const totalItems = filteredRows.length;
    const totalPages = Math.ceil(totalItems / rowsPerPage);

    // Validate trang hiện tại (tránh trường hợp đang ở trang 5 lọc còn 1 trang)
    if (currentPage < 1) currentPage = 1;
    if (currentPage > totalPages && totalPages > 0) currentPage = totalPages;
    if (totalItems === 0) currentPage = 1;

    // 2. Tính toán các dòng cần hiện (Slice)
    const start = (currentPage - 1) * rowsPerPage;
    const end = start + rowsPerPage;
    const visibleRows = filteredRows.slice(start, end);

    // 3. Hiển thị các dòng trong range
    visibleRows.forEach((row) => (row.style.display = ""));

    // 4. Xử lý Empty State (Không có kết quả)
    if (totalItems === 0) {
      noResultsDiv.style.display = "flex"; // Hiện thông báo
      tableBody.parentElement.style.minHeight = "auto";
    } else {
      noResultsDiv.style.display = "none";
    }

    // 5. Cập nhật Text thống kê
    startRowEl.innerText = totalItems === 0 ? 0 : start + 1;
    endRowEl.innerText = end > totalItems ? totalItems : end;
    totalRowsEl.innerText = totalItems;

    // 6. Cập nhật trạng thái nút bấm
    prevBtn.disabled = currentPage === 1;
    nextBtn.disabled = currentPage >= totalPages || totalPages === 0;
  }

  /**
   * Hàm lọc dữ liệu (Gọi khi Search hoặc chọn Dropdown Status)
   */
  function filterData() {
    const searchText = searchInput
      ? searchInput.value.toLowerCase().trim()
      : "";
    const statusValue = statusFilter ? statusFilter.value : "all";

    filteredRows = allRows.filter((row) => {
      // Cột 1: ID, Cột 4: Status (index tính từ 0)
      const cells = row.getElementsByTagName("td");
      if (!cells || cells.length < 5) return false;

      const idText = cells[1].textContent || cells[1].innerText;
      const statusText = cells[4].textContent || cells[4].innerText;

      // 1. Check Search Text (ID)
      const matchesSearch = idText.toLowerCase().includes(searchText);

      // 2. Check Status
      let matchesStatus = true;
      if (statusValue === "risk") {
        // Kiểm tra xem text có chứa từ khóa của Risk không
        matchesStatus = statusText.includes("Nguy cơ");
      } else if (statusValue === "safe") {
        matchesStatus = statusText.includes("An toàn");
      }

      return matchesSearch && matchesStatus;
    });

    // Reset về trang 1 mỗi khi lọc dữ liệu
    currentPage = 1;
    updateURL();
    renderTable();
  }

  // --- 4. EVENT LISTENERS ---

  // Nút Previous
  prevBtn.addEventListener("click", () => {
    if (currentPage > 1) {
      currentPage--;
      updateURL();
      renderTable();
      // Scroll nhẹ lên đầu bảng cho user dễ nhìn
      document.querySelector(".overflow-x-auto").scrollTop = 0;
    }
  });

  // Nút Next
  nextBtn.addEventListener("click", () => {
    const totalPages = Math.ceil(filteredRows.length / rowsPerPage);
    if (currentPage < totalPages) {
      currentPage++;
      updateURL();
      renderTable();
      document.querySelector(".overflow-x-auto").scrollTop = 0;
    }
  });

  // Thay đổi số dòng hiển thị (Page Size)
  if (pageSizeSelect) {
    pageSizeSelect.addEventListener("change", (e) => {
      rowsPerPage = parseInt(e.target.value);
      currentPage = 1; // Reset về trang 1
      updateURL();
      renderTable();
    });
  }

  // Sự kiện Search & Filter
  if (searchInput) {
    searchInput.addEventListener("keyup", filterData);
    // Xử lý nút 'x' clear trong ô input search (nếu trình duyệt hỗ trợ)
    searchInput.addEventListener("search", filterData);
  }

  if (statusFilter) {
    statusFilter.addEventListener("change", filterData);
  }

  // Sự kiện nút Back/Forward của trình duyệt (Popstate)
  window.addEventListener("popstate", () => {
    const params = new URLSearchParams(window.location.search);
    currentPage = parseInt(params.get("page")) || 1;
    rowsPerPage = parseInt(params.get("page_size")) || 10;

    if (pageSizeSelect) pageSizeSelect.value = rowsPerPage;
    renderTable();
  });

  // --- 5. INITIAL RENDER ---
  // Chạy bộ lọc lần đầu (để đề phòng trường hợp ô input đang có sẵn text do browser cache)
  filterData();
});
