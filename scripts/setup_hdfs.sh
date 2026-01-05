#!/bin/bash

# --- CẤU HÌNH ---
# Đường dẫn thư mục chứa dữ liệu trên máy thật (tương đối so với file script)
LOCAL_DATA_DIR="$(cd "$(dirname "$0")/../data/raw" && pwd)"
# Đường dẫn đích trên HDFS
HDFS_DEST_DIR="/user/ie400/oulad_raw"

echo "========================================================"
echo "   BẮT ĐẦU QUÁ TRÌNH NẠP DỮ LIỆU (DATA INGESTION)"
echo "========================================================"

# 1. Kiểm tra và Khởi động Hadoop nếu chưa chạy
if ! jps | grep -q "NameNode"; then
    echo ">>> Hadoop chưa chạy. Đang khởi động..."
    start-all.sh
    sleep 10 # Đợi 10s để Hadoop khởi động xong hoàn toàn
else
    echo ">>> Hadoop đang chạy."
fi

# 2. Tạo thư mục trên HDFS
echo ">>> Đang tạo thư mục HDFS: $HDFS_DEST_DIR"
hdfs dfs -mkdir -p $HDFS_DEST_DIR

# 3. Upload dữ liệu
echo ">>> Đang upload file từ $LOCAL_DATA_DIR lên HDFS..."
# Chỉ upload các file .csv để tránh upload nhầm folder processed
hdfs dfs -put -f $LOCAL_DATA_DIR/*.csv $HDFS_DEST_DIR/

# 4. Kiểm tra kết quả
echo "========================================================"
echo "   KIỂM TRA DỮ LIỆU TRÊN HDFS"
echo "========================================================"
hdfs dfs -ls $HDFS_DEST_DIR/

echo ">>> Hoàn tất! Dữ liệu đã sẵn sàng trên HDFS."