import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql.functions import avg as _avg
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import when

from configs import config
from src.utils import get_spark_session


def main():
    spark = get_spark_session(config.APP_NAME, config.MASTER)
    
    # --- SỬA ĐỔI: Đọc từ đường dẫn Local ---
    print(f">>> Reading RAW data from: {config.RAW_DATA_PATH}")
    
    df_info = spark.read.csv(config.RAW_DATA_PATH + config.FILE_STUDENT_INFO, header=True, inferSchema=True)
    df_vle = spark.read.csv(config.RAW_DATA_PATH + config.FILE_STUDENT_VLE, header=True, inferSchema=True)
    df_assess = spark.read.csv(config.RAW_DATA_PATH + config.FILE_STUDENT_ASSESSMENT, header=True, inferSchema=True)

    # ... (Phần logic xử lý: Aggregating, Labeling, Joining GIỮ NGUYÊN KHÔNG ĐỔI) ...
    # Bạn copy lại đoạn logic xử lý ở các câu trả lời trước vào đây
    # Logic: df_clicks = ..., df_scores = ..., df_final = ...
    
    # (Để ngắn gọn mình viết tắt đoạn logic xử lý, bạn giữ nguyên code cũ nhé)
    print(">>> Processing data (Aggregating & Joining)...")
    df_clicks = df_vle.groupBy("id_student").agg(_sum("sum_click").alias("total_clicks"))
    df_scores = df_assess.groupBy("id_student").agg(_avg("score").alias("avg_score"))
    df_labeled = df_info.withColumn("label", when(col("final_result").isin("Pass", "Distinction"), 0).otherwise(1))
    
    df_final = df_labeled.join(df_clicks, "id_student", "left") \
                         .join(df_scores, "id_student", "left") \
                         .fillna(0, subset=["total_clicks", "avg_score"])

    # --- SỬA ĐỔI: Lưu xuống folder Local 'data/processed' ---
    print(f">>> Saving PROCESSED data to: {config.PROCESSED_DATA_PATH}")
    df_final.write.mode("overwrite").parquet(config.PROCESSED_DATA_PATH)
    print(">>> ETL Finished Successfully!")

    spark.stop()

if __name__ == "__main__":
    main()