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
    # 1. Khởi tạo Spark
    spark = get_spark_session("OULAD_Model_Comparison", config.MASTER)
    spark.sparkContext.setLogLevel("ERROR")
    
    print(f">>> Reading processed data from: {config.HDFS_OUTPUT_PATH}")
    df = spark.read.parquet(config.HDFS_OUTPUT_PATH)
    
    # 2. Chuẩn bị dữ liệu (VectorAssembler)
    assembler = VectorAssembler(
        inputCols=["total_clicks", "avg_score"], 
        outputCol="features"
    )
    data_vectorized = assembler.transform(df)

    # 3. Chia tập Train/Test (80% - 20%)
    train_data, test_data = data_vectorized.randomSplit([0.8, 0.2], seed=42)
    
    # Cache dữ liệu vào RAM để chạy nhiều model cho nhanh
    train_data.cache()
    test_data.cache()
    
    print(f">>> Data prepared. Training set: {train_data.count()}, Test set: {test_data.count()}")

    # 4. Định nghĩa danh sách các Model cần chạy
    # Lưu ý: GBT và RF là thuật toán cây, cần thời gian chạy lâu hơn LR
    models = [
        {
            "name": "Logistic Regression",
            "model": LogisticRegression(labelCol="label", featuresCol="features")
        },
        {
            "name": "Random Forest",
            # numTrees=20: Tạo 20 cây quyết định (tăng lên sẽ chính xác hơn nhưng chậm hơn)
            "model": RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=20)
        },
        {
            "name": "Gradient Boosted Trees (GBT)",
            # maxIter=10: Lặp 10 lần sửa lỗi (tăng lên sẽ chính xác hơn)
            "model": GBTClassifier(labelCol="label", featuresCol="features", maxIter=10)
        }
    ]

    # 5. Vòng lặp huấn luyện và đánh giá
    results = []
    evaluator = BinaryClassificationEvaluator(labelCol="label")

    print("\n" + "="*60)
    print(f"{'MODEL NAME':<30} | {'AUC SCORE':<10} | {'TIME (s)':<10}")
    print("-" * 60)

    for item in models:
        name = item["name"]
        algorithm = item["model"]
        
        start_time = time.time()
        
        # Train
        trained_model = algorithm.fit(train_data)
        
        # Predict
        predictions = trained_model.transform(test_data)
        
        # Evaluate
        auc = evaluator.evaluate(predictions)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Lưu kết quả
        results.append((name, auc, duration))
        
        print(f"{name:<30} | {auc:.4f}     | {duration:.2f}s")

    print("="*60)

    # 6. Kết luận
    # Sắp xếp xem model nào tốt nhất (AUC cao nhất)
    best_model = sorted(results, key=lambda x: x[1], reverse=True)[0]
    
    print(f"\n>>> KẾT LUẬN: Mô hình tốt nhất là '{best_model[0]}' với AUC = {best_model[1]:.4f}")
    
    spark.stop()

if __name__ == "__main__":
    main()