import os
import sys

import happybase

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from configs import config
from src.utils import get_spark_session


def main():
    # 1. Đọc dữ liệu đã xử lý từ HDFS (File Parquet)
    spark = get_spark_session("Save_To_HBase", config.MASTER)
    print(f">>> Reading data from: {config.HDFS_OUTPUT_PATH}")
    df = spark.read.parquet(config.HDFS_OUTPUT_PATH)

    # Lấy mẫu khoảng 1000 sinh viên để demo (đẩy hết sẽ hơi lâu nếu chạy local)
    # Trong thực tế Big Data, ta dùng connector chuyên dụng, nhưng demo thì dùng cách này OK.
    data_to_save = df.select("id_student", "total_clicks", "avg_score", "label").limit(1000).collect()

    print(">>> Connecting to HBase via Thrift...")
    # Kết nối HBase (mặc định localhost cổng 9090)
    connection = happybase.Connection('localhost')
    table = connection.table('student_predictions')

    print(f">>> Writing {len(data_to_save)} records to HBase...")
    
    # Dùng batch để ghi cho nhanh
    batch = table.batch()
    
    for row in data_to_save:
        row_key = str(row['id_student']).encode() # ID làm khóa chính
        
        # Ghi dữ liệu vào các cột tương ứng
        batch.put(row_key, {
            b'info:clicks': str(row['total_clicks']).encode(),
            b'info:avg_score': str(row['avg_score']).encode(),
            b'prediction:risk_label': str(row['label']).encode()
            # Sau này khi có model dự đoán realtime, ta sẽ update cột prediction ở đây
        })
    
    batch.send()
    connection.close()
    
    print(">>> Successfully saved data to HBase table 'student_predictions'")
    spark.stop()

if __name__ == "__main__":
    main()