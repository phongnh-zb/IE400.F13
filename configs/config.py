import os

# Lấy đường dẫn gốc của dự án
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# --- SỬA LỖI TẠI ĐÂY ---
# Thêm tiền tố "file://" để Spark biết đây là file trên ổ cứng, không phải HDFS

# 1. Đường dẫn Input (Folder data chứa CSV)
RAW_DATA_PATH = "file://" + os.path.join(BASE_DIR, "data") + "/raw/"

# 2. Đường dẫn Output (Folder processed chứa Parquet)
PROCESSED_DATA_PATH = "file://" + os.path.join(BASE_DIR, "data", "processed") + "/"

# Tên các file CSV
FILE_STUDENT_INFO = "studentInfo.csv"
FILE_STUDENT_VLE = "studentVle.csv"
FILE_STUDENT_ASSESSMENT = "studentAssessment.csv"

# Cấu hình Spark
APP_NAME = "OULAD_Local_Processing"
MASTER = "local[*]"