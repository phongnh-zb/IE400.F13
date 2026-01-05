// webapp/static/js/students.js

document.addEventListener("DOMContentLoaded", () => {
  console.log("Students page script loaded.");

  // Lấy tham chiếu đến ô tìm kiếm và bảng
  const searchInput = document.getElementById("studentSearch");
  const tableBody = document.getElementById("studentTableBody");

  // Nếu không tìm thấy element thì dừng (tránh lỗi ở các trang khác)
  if (!searchInput || !tableBody) return;

  // Lắng nghe sự kiện gõ phím
  searchInput.addEventListener("keyup", (e) => {
    const searchText = e.target.value.toLowerCase();
    const rows = tableBody.getElementsByTagName("tr");

    Array.from(rows).forEach((row) => {
      // Lấy text của cột ID (cột thứ 2, index 1 vì cột 0 là STT)
      const idCell = row.getElementsByTagName("td")[1];

      if (idCell) {
        const idText = idCell.textContent || idCell.innerText;

        // Kiểm tra xem ID có chứa từ khóa tìm kiếm không
        if (idText.toLowerCase().indexOf(searchText) > -1) {
          row.style.display = ""; // Hiện
        } else {
          row.style.display = "none"; // Ẩn
        }
      }
    });
  });
});
