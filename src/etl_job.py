# src/etl_job.py
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Import thêm hàm avg (trung bình)
from pyspark.sql.functions import avg as _avg
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import when

from configs import config
from src.utils import get_spark_session


def main():
    spark = get_spark_session(config.APP_NAME, config.MASTER)
    print(">>> Spark Session Created")

    # 1. Đọc dữ liệu (Thêm file assessment)
    print(f">>> Reading data from HDFS...")
    df_info = spark.read.csv(config.HDFS_BASE_PATH + config.FILE_STUDENT_INFO, header=True, inferSchema=True)
    df_vle = spark.read.csv(config.HDFS_BASE_PATH + config.FILE_STUDENT_VLE, header=True, inferSchema=True)
    df_assess = spark.read.csv(config.HDFS_BASE_PATH + config.FILE_STUDENT_ASSESSMENT, header=True, inferSchema=True)

    # 2. Xử lý Click (Như cũ)
    print(">>> Aggregating clicks...")
    df_clicks = df_vle.groupBy("id_student").agg(_sum("sum_click").alias("total_clicks"))

    # 3. Xử lý Điểm số (MỚI)
    print(">>> Aggregating scores...")
    # Tính điểm trung bình của từng sinh viên
    df_scores = df_assess.groupBy("id_student").agg(_avg("score").alias("avg_score"))

    # 4. Tạo nhãn (Như cũ)
    print(">>> Labeling...")
    df_labeled = df_info.withColumn("label", 
        when(col("final_result").isin("Pass", "Distinction"), 0)
        .otherwise(1)
    )

    # 5. Join 3 bảng lại với nhau (MỚI)
    print(">>> Joining all data...")
    # Join lần 1: Info + Clicks
    df_temp = df_labeled.join(df_clicks, "id_student", "left")
    
    # Join lần 2: Temp + Scores
    df_final = df_temp.join(df_scores, "id_student", "left")

    # Xử lý dữ liệu thiếu (Null Handling)
    # - Không click -> total_clicks = 0
    # - Không có điểm -> avg_score = 0 (Giả định sinh viên không nộp bài là 0 điểm)
    df_final = df_final.fillna(0, subset=["total_clicks", "avg_score"])

    # 6. Hiển thị và Lưu
    print("="*30)
    print(f"Total students: {df_final.count()}")
    # Xem thử dữ liệu có thêm cột avg_score chưa
    df_final.select("id_student", "total_clicks", "avg_score", "label").show(5)
    print("="*30)

    print(f">>> Saving processed data to: {config.HDFS_OUTPUT_PATH}")
    df_final.write.mode("overwrite").parquet(config.HDFS_OUTPUT_PATH)
    print(">>> Data saved successfully!")

    spark.stop()

if __name__ == "__main__":
    main()