# configs/config.py
import os

# --- CẤU HÌNH HDFS (Dùng cho Pipeline tự động) ---
# Địa chỉ HDFS mặc định
HDFS_NAMENODE = "hdfs://localhost:9000"

# Đường dẫn Input (Nơi setup_hdfs.sh đã upload file lên)
HDFS_BASE_PATH = f"{HDFS_NAMENODE}/user/ie400/oulad_raw/"

# Đường dẫn Output (Nơi etl_job.py sẽ lưu file sau xử lý)
HDFS_OUTPUT_PATH = f"{HDFS_NAMENODE}/user/ie400/oulad_processed/"

# Tên các file
FILE_STUDENT_INFO = "studentInfo.csv"
FILE_STUDENT_VLE = "studentVle.csv"
FILE_STUDENT_ASSESSMENT = "studentAssessment.csv"

# Spark Config
APP_NAME = "OULAD_Pipeline"
MASTER = "local[*]"