import logging
import math
import threading
import time

import happybase
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

# --- CONFIG ---
logging.basicConfig(level=logging.INFO)
logger = app.logger

HBASE_HOST = 'localhost'
HBASE_PORT = 9090
TABLE_NAME = 'student_predictions'
CACHE_INTERVAL = 600  # 10 phút (600 giây)

# --- GLOBAL CACHE (BỘ NHỚ ĐỆM) ---
# Biến này sẽ lưu toàn bộ dữ liệu trong RAM
SYSTEM_CACHE = {
    "data": [],         # Danh sách sinh viên
    "last_updated": None,
    "is_ready": False   # Cờ báo hiệu cache đã tải xong lần đầu chưa
}

def fetch_all_data_from_hbase():
    """
    Hàm này chạy NGẦM (Background).
    Nhiệm vụ: Quét toàn bộ HBase và lưu vào RAM.
    """
    connection = None
    data_buffer = []
    
    try:
        logger.info(">>> [CACHE] Bắt đầu đồng bộ dữ liệu từ HBase...")
        start_time = time.time()
        
        # Kết nối HBase (Timeout cao vì chạy ngầm, không sợ user chờ)
        connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, timeout=60000)
        connection.open()
        table = connection.table(TABLE_NAME)
        
        # Quét toàn bộ bảng (Full Scan)
        # Vì chạy ngầm nên ta có thể thoải mái lấy hết dữ liệu
        for key, value in table.scan():
            try:
                data_buffer.append({
                    'id': key.decode('utf-8'),
                    'clicks': float(value.get(b'info:clicks', b'0')),
                    'score': float(value.get(b'info:avg_score', b'0')),
                    'risk': int(value.get(b'prediction:risk_label', b'0'))
                })
            except Exception:
                continue

        # Cập nhật vào biến Global
        SYSTEM_CACHE["data"] = data_buffer
        SYSTEM_CACHE["last_updated"] = time.strftime("%H:%M:%S")
        SYSTEM_CACHE["is_ready"] = True
        
        duration = time.time() - start_time
        logger.info(f">>> [CACHE] ✅ Đã cập nhật xong {len(data_buffer)} bản ghi trong {duration:.2f}s.")
        
    except Exception as e:
        logger.error(f">>> [CACHE] ❌ Lỗi cập nhật Cache: {e}")
    finally:
        if connection: connection.close()

def background_scheduler():
    """Luồng chạy vĩnh viễn: Cứ 10 phút chạy cập nhật 1 lần"""
    while True:
        fetch_all_data_from_hbase()
        logger.info(f">>> [SCHEDULER] Ngủ {CACHE_INTERVAL} giây trước lần cập nhật tiếp theo...")
        time.sleep(CACHE_INTERVAL)

# --- KHỞI ĐỘNG LUỒNG CACHE NGAY KHI APP CHẠY ---
# Daemon=True để luồng tự tắt khi App tắt
t = threading.Thread(target=background_scheduler, daemon=True)
t.start()


# --- CÁC HÀM XỬ LÝ TRÊN RAM (CỰC NHANH) ---
def get_data_from_memory(page=1, page_size=50, search_query="", sort_by="id", order="asc"):
    """
    Hàm xử lý logic dữ liệu trong RAM:
    1. Search -> 2. Sort -> 3. Paginate
    """
    # 1. Lấy dữ liệu thô từ Cache
    if not SYSTEM_CACHE["is_ready"]:
        return {'data': [], 'total_pages': 0, 'total_records': 0, 'page': 1}

    all_data = SYSTEM_CACHE["data"]
    
    # 2. FILTER (Tìm kiếm)
    if search_query:
        q = search_query.lower()
        # Tìm trong ID hoặc Risk Label (nếu muốn)
        filtered_data = [x for x in all_data if q in x['id'].lower()]
    else:
        filtered_data = list(all_data) # Copy list để không ảnh hưởng Cache gốc

    # 3. SORT (Sắp xếp)
    # sort_by: 'id', 'clicks', 'score', 'risk'
    # order: 'asc', 'desc'
    reverse = (order == 'desc')
    
    try:
        # Sử dụng lambda để chọn key sắp xếp
        filtered_data.sort(key=lambda x: x.get(sort_by, 0), reverse=reverse)
    except Exception as e:
        logger.error(f"Lỗi sort: {e}")

    # 4. PAGINATION (Phân trang)
    total_records = len(filtered_data)
    total_pages = math.ceil(total_records / page_size) if total_records > 0 else 1
    
    # Validate trang
    if page < 1: page = 1
    if page > total_pages: page = total_pages
    
    start = (page - 1) * page_size
    end = start + page_size
    
    paginated_data = filtered_data[start:end]
    
    return {
        'data': paginated_data,
        'page': page,
        'total_pages': total_pages,
        'total_records': total_records
    }
    
def get_student_by_id(student_id):
    """
    Find a specific student in the RAM Cache.
    """
    if not SYSTEM_CACHE["is_ready"]:
        return None
        
    # Search in the list (Linear search is very fast for 30k items in RAM)
    # If the list gets huge (1M+), we would convert this to a Dictionary {id: data}
    for st in SYSTEM_CACHE["data"]:
        if st['id'] == student_id:
            return st
            
    return None

# --- ROUTES ---

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/students')
def students():
    if not SYSTEM_CACHE["is_ready"]:
        return render_template('loading.html')

    # Lấy tham số từ URL
    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('page_size', 50, type=int)
    search = request.args.get('search', '', type=str)
    
    # Tham số mới cho Sorting
    sort_by = request.args.get('sort_by', 'id', type=str)   # Mặc định sort theo ID
    order = request.args.get('order', 'asc', type=str)      # Mặc định tăng dần

    # Gọi hàm xử lý
    result = get_data_from_memory(page, page_size, search, sort_by, order)
    
    return render_template(
        'students.html', 
        students=result['data'],
        
        # Pagination Params
        page=result['page'],
        total_pages=result['total_pages'],
        total_records=result['total_records'],
        page_size=page_size,
        
        # Filter/Sort Params (Để giữ trạng thái trên UI)
        search=search,
        sort_by=sort_by,
        order=order,
        
        last_updated=SYSTEM_CACHE["last_updated"]
    )
    
@app.route('/api/student/<student_id>')
def api_student_detail(student_id):
    """API trả về JSON chi tiết sinh viên cho Modal"""
    student = get_student_by_id(student_id)
    
    if not student:
        return jsonify({'error': 'Not found'}), 404
        
    # Tạo Recommendation Logic (Copy từ logic cũ)
    recommendations = []
    if student['risk'] == 1:
        recommendations.append("Cần liên hệ cố vấn học tập ngay.")
        recommendations.append("Xem lại log 'Vle' vì tương tác thấp.")
        recommendations.append("Sắp xếp buổi mentoring 1-1.")
    else:
        recommendations.append("Duy trì phong độ hiện tại!")
        recommendations.append("Khuyến khích tham gia nhóm đôi bạn cùng tiến.")

    # Trả về JSON gộp cả info và recommendation
    return jsonify({
        'info': student,
        'recommendations': recommendations
    })

@app.route('/api/realtime-data')
def realtime_data():
    """API Dashboard: Lấy ngay từ RAM"""
    if not SYSTEM_CACHE["is_ready"]:
        return jsonify({'raw_data': [], 'summary': {'total':0, 'risk':0, 'safe':0}})

    data_sample = SYSTEM_CACHE["data"]
    
    total = len(data_sample)
    risk = sum(1 for x in data_sample if x['risk'] == 1)
    safe = total - risk
    
    return jsonify({
        'raw_data': data_sample,
        'summary': {
            'total': len(SYSTEM_CACHE["data"]), # Tổng toàn bộ DB
            'risk': risk,
            'safe': safe,
            'last_updated': SYSTEM_CACHE["last_updated"]
        }
    })

if __name__ == '__main__':
    # use_reloader=False để tránh chạy 2 luồng background
    app.run(debug=True, port=5001, use_reloader=False)