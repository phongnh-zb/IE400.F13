import os
import sys

import happybase

# Setup đường dẫn import
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from configs import config
from src.utils import get_spark_session


def main():
    # 1. Khởi tạo Spark
    spark = get_spark_session("Save_To_HBase_HDFS", config.MASTER)
    spark.sparkContext.setLogLevel("ERROR")

    # --- SỬA LỖI TẠI ĐÂY ---
    # Đọc từ HDFS_OUTPUT_PATH thay vì PROCESSED_DATA_PATH
    print(f">>> [HBASE] Reading processed data from HDFS: {config.HDFS_OUTPUT_PATH}")
    
    try:
        df = spark.read.parquet(config.HDFS_OUTPUT_PATH)
    except Exception as e:
        print(f">>> LỖI: Không tìm thấy dữ liệu trên HDFS tại {config.HDFS_OUTPUT_PATH}")
        print("Hãy chắc chắn bước ETL đã chạy thành công.")
        sys.exit(1)

    # Lấy mẫu 1000 dòng để đẩy vào HBase (Demo)
    data_to_save = df.select("id_student", "total_clicks", "avg_score", "label").limit(1000).collect()

    print(">>> [HBASE] Connecting to HBase via Thrift...")
    try:
        # Kết nối đến localhost cổng 9090
        connection = happybase.Connection('localhost', port=9090)
        table = connection.table('student_predictions')
        
        print(f">>> [HBASE] Writing {len(data_to_save)} records...")
        batch = table.batch()
        
        for row in data_to_save:
            row_key = str(row['id_student']).encode()
            batch.put(row_key, {
                b'info:clicks': str(row['total_clicks']).encode(),
                b'info:avg_score': str(row['avg_score']).encode(),
                b'prediction:risk_label': str(row['label']).encode()
            })
        
        batch.send()
        connection.close()
        print(">>> [HBASE] SUCCESS: Data saved to HBase table 'student_predictions'")
        
    except Exception as e:
        print(f">>> [HBASE] LỖI KẾT NỐI: {e}")
        print("Gợi ý: Hãy kiểm tra xem Thrift Server đã bật chưa (hbase thrift start)")

    spark.stop()

if __name__ == "__main__":
    main()