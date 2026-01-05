import os
import sys

import happybase

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from configs import config
from src.utils import get_spark_session


def main():
    # 1. Đọc dữ liệu từ thư mục Local 'data/processed'
    spark = get_spark_session("Save_To_HBase_Local", config.MASTER)
    
    # Config mới trỏ vào folder data/processed
    print(f">>> Reading processed data from: {config.PROCESSED_DATA_PATH}")
    
    try:
        df = spark.read.parquet(config.PROCESSED_DATA_PATH)
    except Exception as e:
        print(f"LỖI: Không tìm thấy dữ liệu tại {config.PROCESSED_DATA_PATH}")
        print("Bạn đã chạy 'python3 src/etl_job.py' chưa?")
        return

    # Lấy mẫu data để đẩy (Demo 1000 dòng)
    data_to_save = df.select("id_student", "total_clicks", "avg_score", "label").limit(1000).collect()

    print(">>> Connecting to HBase via Thrift...")
    try:
        connection = happybase.Connection('localhost')
        table = connection.table('student_predictions')
        
        print(f">>> Writing {len(data_to_save)} records to HBase...")
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
        print(">>> SUCCESS: Data saved to HBase!")
        
    except Exception as e:
        print(f"LỖI KẾT NỐI HBASE: {e}")
        print("Hãy đảm bảo bạn đã chạy lệnh: hbase thrift start")

    spark.stop()

if __name__ == "__main__":
    main()