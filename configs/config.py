# configs/config.py

# Đường dẫn HDFS (Lưu ý: localhost:9000 là mặc định của Hadoop)
HDFS_BASE_PATH = "hdfs://localhost:9000/user/phongnh.zb/ie400_raw/"
HDFS_OUTPUT_PATH = "hdfs://localhost:9000/user/phongnh.zb/ie400_processed/"

# Tên các file
FILE_STUDENT_INFO = "studentInfo.csv"
FILE_STUDENT_VLE = "studentVle.csv"
FILE_STUDENT_ASSESSMENT = "studentAssessment.csv" 
# ---------------------

# Cấu hình Spark
APP_NAME = "OULAD_Dropout_Prediction"
MASTER = "local[*]"
