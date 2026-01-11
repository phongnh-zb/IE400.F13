import os
import socket
import subprocess
import sys
import time

# --- CẤU HÌNH MÀU SẮC ---
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
RESET = '\033[0m'
BLUE = '\033[94m'

def print_header(step_name):
    print("\n" + "="*60)
    print(f"{BLUE}>>> BƯỚC: {step_name}{RESET}")
    print("="*60)

def run_command(command, step_name):
    """Hàm chạy lệnh shell và kiểm tra lỗi"""
    print_header(step_name)
    start_time = time.time()
    try:
        # Chạy lệnh và hiển thị output trực tiếp
        result = subprocess.run(command, shell=True, check=True, text=True)
        duration = time.time() - start_time
        print(f"\n{GREEN}✔ HOÀN THÀNH: {step_name} trong {duration:.2f} giây.{RESET}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n{RED}✘ THẤT BẠI: {step_name} gặp lỗi! (Code: {e.returncode}){RESET}")
        return False

def wait_for_service(host, port, service_name, timeout=60):
    """
    Hàm chờ thông minh: Đợi cho đến khi Port mở (TCP Connect thành công)
    """
    print(f"⏳ Đang đợi {service_name} khởi động hoàn toàn (Port {port})...", end="", flush=True)
    start_wait = time.time()
    
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                print(f"\n{GREEN}✔ {service_name} đã sẵn sàng!{RESET}")
                return True
            
        except Exception:
            pass

        # Kiểm tra timeout
        if time.time() - start_wait > timeout:
            print(f"\n{RED}✘ LỖI: Quá thời gian chờ {service_name}. Hãy kiểm tra log.{RESET}")
            return False
        
        # Đợi 1s rồi thử lại
        time.sleep(1)
        print(".", end="", flush=True)

def main():
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    total_start = time.time()

    print(f"{YELLOW}>>> BẮT ĐẦU QUY TRÌNH BIG DATA PIPELINE{RESET}")
    print(f"    Thư mục dự án: {PROJECT_ROOT}\n")

    # --- BƯỚC 0: KHỞI ĐỘNG HẠ TẦNG (SERVICES) ---
    # 1. Gọi script shell để bật Hadoop/HBase/Thrift nếu chưa chạy
    cmd_services = f"bash {PROJECT_ROOT}/scripts/start_services.sh"
    if not run_command(cmd_services, "0. KHỞI ĐỘNG SERVICES (HADOOP/HBASE)"):
        sys.exit(1)

    # 2. Quan trọng: Python phải đợi Thrift (Port 9090) thực sự mở kết nối
    # Vì script shell chỉ gửi lệnh "start" rồi thoát, lúc đó Java vẫn đang loading ngầm.
    if not wait_for_service('localhost', 9090, "HBase Thrift Server"):
        print(f"{RED}Dừng chương trình do hạ tầng chưa sẵn sàng.{RESET}")
        sys.exit(1)

    # --- BƯỚC 1: DATA INGESTION ---
    cmd_ingest = f"bash {PROJECT_ROOT}/scripts/setup_hdfs.sh"
    if not run_command(cmd_ingest, "1. NẠP DỮ LIỆU LÊN HDFS"): sys.exit(1)

    # --- BƯỚC 2: ETL PROCESSING (Spark) ---
    cmd_etl = f"python3 {PROJECT_ROOT}/src/etl_job.py"
    if not run_command(cmd_etl, "2. XỬ LÝ DỮ LIỆU (ETL - SPARK)"): sys.exit(1)

    # --- BƯỚC 3: MODEL TRAINING (Spark ML) ---
    cmd_train = f"python3 {PROJECT_ROOT}/src/train_model.py"
    if not run_command(cmd_train, "3. HUẤN LUYỆN MÔ HÌNH (TRAINING)"): sys.exit(1)

    # --- BƯỚC 4: SAVE TO HBASE ---
    cmd_hbase = f"python3 {PROJECT_ROOT}/src/save_to_hbase.py"
    if not run_command(cmd_hbase, "4. LƯU KẾT QUẢ VÀO HBASE"): sys.exit(1)

    # --- TỔNG KẾT ---
    total_duration = time.time() - total_start
    print("\n" + "="*60)
    print(f"{GREEN}🎉 SUCCESS! TOÀN BỘ PIPELINE HOÀN THÀNH TRONG {total_duration:.2f}s{RESET}")
    print("="*60)

if __name__ == "__main__":
    main()