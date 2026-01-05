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

def print_header(step_name):
    print("\n" + "="*60)
    print(f"{YELLOW}>>> ĐANG CHẠY BƯỚC: {step_name}{RESET}")
    print("="*60)

def run_command(command, step_name):
    """Hàm chạy lệnh shell và kiểm tra lỗi"""
    print_header(step_name)
    start_time = time.time()
    try:
        result = subprocess.run(command, shell=True, check=True, text=True)
        duration = time.time() - start_time
        print(f"\n{GREEN}✔ HOÀN THÀNH: {step_name} trong {duration:.2f} giây.{RESET}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n{RED}✘ THẤT BẠI: {step_name} gặp lỗi!{RESET}")
        return False

# --- HÀM MỚI: KIỂM TRA VÀ TỰ KHỞI ĐỘNG THRIFT ---
def check_and_start_thrift():
    print_header("KIỂM TRA HBASE THRIFT SERVER")
    
    # 1. Kiểm tra xem cổng 9090 có đang mở không
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('localhost', 9090))
    sock.close()
    
    if result == 0:
        print(f"{GREEN}✔ Thrift Server đang chạy (Port 9090 Open).{RESET}")
        return True
    
    # 2. Nếu chưa chạy, thực hiện khởi động
    print(f"{YELLOW}⚠ Thrift Server chưa bật. Đang tiến hành khởi động tự động...{RESET}")
    try:
        # Chạy lệnh khởi động ngầm (Background)
        # Redirect log ra /dev/null để không làm rối màn hình
        subprocess.Popen("hbase thrift start > /dev/null 2>&1 &", shell=True)
        
        # 3. Vòng lặp chờ (Polling): Đợi tối đa 30 giây để Server khởi động xong
        print("⏳ Đang đợi Thrift Server khởi động...", end="", flush=True)
        for i in range(30):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            res = sock.connect_ex(('localhost', 9090))
            sock.close()
            
            if res == 0:
                print(f"\n{GREEN}✔ Thrift Server đã khởi động thành công!{RESET}")
                return True
            
            time.sleep(1) # Đợi 1 giây rồi kiểm tra lại
            print(".", end="", flush=True)
            
        print(f"\n{RED}✘ LỖI: Quá thời gian chờ (Timeout). Thrift Server không phản hồi.{RESET}")
        return False
        
    except Exception as e:
        print(f"\n{RED}✘ LỖI: Không thể khởi động Thrift: {e}{RESET}")
        return False

def main():
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    total_start = time.time()

    # --- BƯỚC 1: DATA INGESTION ---
    cmd_ingest = f"bash {PROJECT_ROOT}/scripts/setup_hdfs.sh"
    if not run_command(cmd_ingest, "1. NẠP DỮ LIỆU LÊN HDFS"): sys.exit(1)

    # --- BƯỚC 2: ETL PROCESSING ---
    cmd_etl = f"spark-submit {PROJECT_ROOT}/src/etl_job.py"
    if not run_command(cmd_etl, "2. XỬ LÝ DỮ LIỆU (ETL)"): sys.exit(1)

    # --- BƯỚC 3: MODEL TRAINING ---
    cmd_train = f"spark-submit {PROJECT_ROOT}/src/train_model.py"
    if not run_command(cmd_train, "3. HUẤN LUYỆN MÔ HÌNH (TRAINING)"): sys.exit(1)

    # --- BƯỚC 4: SAVE TO HBASE (CÓ TỰ ĐỘNG START) ---
    # Gọi hàm kiểm tra trước
    if check_and_start_thrift():
        cmd_hbase = f"python3 {PROJECT_ROOT}/src/save_to_hbase.py"
        run_command(cmd_hbase, "4. LƯU KẾT QUẢ VÀO HBASE")
    else:
        print(f"{RED}⚠ Bỏ qua bước lưu vào HBase do lỗi kết nối.{RESET}")

    # --- TỔNG KẾT ---
    total_duration = time.time() - total_start
    print("\n" + "="*60)
    print(f"{GREEN}🎉 DONE! TỔNG THỜI GIAN: {total_duration:.2f}s{RESET}")
    print("="*60)

if __name__ == "__main__":
    main()