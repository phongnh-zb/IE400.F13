import os
import sys
import time

import happybase

# Setup đường dẫn import
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from configs import config
from src.utils import get_spark_session


def main():
    # 1. Khởi tạo Spark
    spark = get_spark_session("Save_To_HBase_Full", config.MASTER)
    spark.sparkContext.setLogLevel("ERROR")

    print(f">>> [HBASE] Reading processed data from HDFS: {config.HDFS_OUTPUT_PATH}")
    
    try:
        # Đọc dữ liệu Parquet từ HDFS
        df = spark.read.parquet(config.HDFS_OUTPUT_PATH)
        
        # --- QUAN TRỌNG: Đếm tổng số dòng để theo dõi ---
        total_count = df.count()
        print(f">>> [INFO] Tìm thấy tổng cộng {total_count} dòng dữ liệu.")
        
        # Thu thập toàn bộ dữ liệu về Driver (Lưu ý: Với Big Data thật sự lớn >1GB, không được dùng collect())
        # Với 32k dòng của OULAD thì collect() vẫn ổn.
        all_rows = df.select("id_student", "total_clicks", "avg_score", "label").collect()
        
    except Exception as e:
        print(f">>> LỖI: Không tìm thấy dữ liệu hoặc lỗi đọc HDFS: {e}")
        sys.exit(1)

    print(">>> [HBASE] Connecting to HBase via Thrift...")
    connection = None
    try:
        connection = happybase.Connection('localhost', port=9090, timeout=10000) # Tăng timeout
        table = connection.table('student_predictions')
        
        print(f">>> [HBASE] Bắt đầu ghi {total_count} dòng vào bảng 'student_predictions'...")
        
        # Sử dụng Batch để ghi nhanh hơn
        batch = table.batch(batch_size=1000)
        start_time = time.time()
        
        for i, row in enumerate(all_rows):
            row_key = str(row['id_student']).encode()
            
            # Lấy giá trị gốc từ Spark
            clicks = float(row['total_clicks'])
            score = float(row['avg_score'])
            risk_label = int(row['label']) # Đây là label do model dự đoán
            
            # --- LOGIC CAN THIỆP (OVERRIDE) ---
            # Nếu điểm > 90, ép về trạng thái An toàn (0) bất kể Model nói gì
            if score >= 90.0:
                risk_label = 0 
            # ----------------------------------

            batch.put(row_key, {
                b'info:clicks': str(clicks).encode(),
                b'info:avg_score': str(score).encode(),
                b'prediction:risk_label': str(risk_label).encode()
            })

        # Gửi những dòng còn lại trong batch
        batch.send()
        
        duration = time.time() - start_time
        print(f">>> [HBASE] ✅ HOÀN TẤT! Đã ghi {total_count} dòng trong {duration:.2f} giây.")
        
    except Exception as e:
        print(f">>> [HBASE] LỖI KẾT NỐI/GHI DỮ LIỆU: {e}")
    finally:
        if connection:
            connection.close()

    spark.stop()

if __name__ == "__main__":
    main()