#!/bin/bash

# scripts/start_services.sh

echo "------------------------------------------------------------"
echo ">>> [SYSTEM] KIỂM TRA VÀ KHỞI ĐỘNG HẠ TẦNG..."

# 1. KIỂM TRA HADOOP (NameNode)
# Hadoop khởi động lâu nên ta chỉ bật nếu nó chưa chạy
if ! jps | grep -q "NameNode"; then
    echo ">>> [START] Đang khởi động Hadoop (HDFS)..."
    start-all.sh
else
    echo "✔ Hadoop (HDFS) đã đang chạy."
fi

# 2. KIỂM TRA HBASE (HMaster)
# Tương tự, chỉ bật nếu chưa chạy
if ! jps | grep -q "HMaster"; then
    echo ">>> [START] Đang khởi động HBase..."
    start-hbase.sh
else
    echo "✔ HBase Master đã đang chạy."
fi

# 3. XỬ LÝ HBASE THRIFT (QUAN TRỌNG: FORCE RESTART)
# Với Thrift, ta luôn Kill và Bật lại để tránh lỗi "Broken Pipe" hoặc "Timeout"
echo ">>> [RESET] Đang làm mới HBase Thrift Server..."

# a. Tìm và diệt tiến trình Thrift cũ (nếu có)
# 'xargs -r' để không báo lỗi nếu không tìm thấy pid nào
jps | grep ThriftServer | awk '{print $1}' | xargs -r kill -9 2>/dev/null

# b. Đợi hệ điều hành giải phóng cổng (quan trọng)
echo "    -> Đang đợi giải phóng cổng 9090..."
sleep 2

# c. Khởi động lại
echo ">>> [START] Khởi động Thrift Server mới (Port 9090)..."
hbase thrift start -p 9090 --infoport 9095 > /dev/null 2>&1 &

echo "✔ Đã gửi lệnh khởi động Thrift."
echo "------------------------------------------------------------"