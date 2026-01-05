# src/train_model.py
import os
import sys
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.ml.classification import (GBTClassifier, LogisticRegression,
                                       RandomForestClassifier)
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler

from configs import config
from src.utils import get_spark_session


def main():
    spark = get_spark_session("OULAD_Training", config.MASTER)
    spark.sparkContext.setLogLevel("ERROR")
    
    # Đọc từ HDFS
    print(f">>> [TRAIN] Reading processed data from: {config.HDFS_OUTPUT_PATH}")
    try:
        df = spark.read.parquet(config.HDFS_OUTPUT_PATH)
    except Exception as e:
        print(">>> LỖI: Không tìm thấy dữ liệu processed. Hãy đảm bảo bước ETL chạy thành công.")
        raise e
    
    # Feature Engineering
    assembler = VectorAssembler(inputCols=["total_clicks", "avg_score"], outputCol="features")
    data_vectorized = assembler.transform(df)
    train_data, test_data = data_vectorized.randomSplit([0.8, 0.2], seed=42)
    
    # Demo chạy 1 model GBT (Model tốt nhất) cho nhanh
    print(">>> [TRAIN] Training GBT Classifier...")
    gbt = GBTClassifier(labelCol="label", featuresCol="features", maxIter=10)
    model = gbt.fit(train_data)
    
    predictions = model.transform(test_data)
    evaluator = BinaryClassificationEvaluator(labelCol="label")
    auc = evaluator.evaluate(predictions)
    
    print("\n" + "="*40)
    print(f"KẾT QUẢ AUTOMATED PIPELINE:")
    print(f"Model: Gradient Boosted Trees")
    print(f"AUC Score: {auc:.4f}")
    print("="*40 + "\n")
    
    spark.stop()

if __name__ == "__main__":
    main()