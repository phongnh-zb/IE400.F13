#!/bin/bash

# Lấy đường dẫn gốc của dự án
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "========================================================"
echo "   BẮT ĐẦU AUTOMATED PIPELINE"
echo "========================================================"

# 1. Chạy bước ETL (Làm sạch & Feature Engineering)
echo ">>> [STEP 1/2] Running ETL Job (etl_job.py)..."
spark-submit "$PROJECT_ROOT/src/etl_job.py"

# Kiểm tra nếu bước 1 thất bại thì dừng luôn
if [ $? -eq 0 ]; then
    echo ">>> ETL thành công!"
else
    echo ">>> LỖI: ETL Job thất bại. Dừng pipeline."
    exit 1
fi

echo "--------------------------------------------------------"

# 2. Chạy bước Huấn luyện Model
echo ">>> [STEP 2/2] Running Model Training (train_model.py)..."
spark-submit "$PROJECT_ROOT/src/train_model.py"

if [ $? -eq 0 ]; then
    echo ">>> Training thành công!"
else
    echo ">>> LỖI: Training Job thất bại."
    exit 1
fi

echo "========================================================"
echo "   PIPELINE HOÀN TẤT XUẤT SẮC"
echo "========================================================"