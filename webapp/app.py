import math

import happybase
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

# HBase Config
HBASE_HOST = 'localhost'
HBASE_PORT = 9090
TABLE_NAME = 'student_predictions'

def get_hbase_data(limit=None):
    """
    Hàm lấy dữ liệu từ HBase.
    - limit=None: Lấy TOÀN BỘ dữ liệu (Full Scan).
    - limit=Number: Lấy giới hạn số dòng.
    """
    data = []
    connection = None
    try:
        # Kết nối với timeout lâu hơn một chút vì load nhiều dữ liệu
        connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, timeout=10000)
        connection.open()
        table = connection.table(TABLE_NAME)
        
        print(f"DEBUG: Đang quét HBase (Limit: {limit})...")
        
        # Scan HBase
        # Nếu limit=None, happybase sẽ scan hết bảng
        scanner = table.scan(limit=limit)
        
        for key, value in scanner:
            try:
                # Decode dữ liệu
                student_id = key.decode('utf-8')
                clicks = float(value.get(b'info:clicks', b'0'))
                score = float(value.get(b'info:avg_score', b'0'))
                risk_val = value.get(b'prediction:risk_label', b'0')
                risk = int(risk_val)
                
                data.append({
                    'id': student_id,
                    'clicks': clicks,
                    'score': score,
                    'risk': risk
                })
            except Exception as row_error:
                print(f"Lỗi đọc dòng {key}: {row_error}")
                continue
                
        print(f"DEBUG: Đã tải xong {len(data)} dòng.")
        
    except Exception as e:
        print(f"LỖI KẾT NỐI HBASE: {e}")
    finally:
        if connection:
            connection.close()
            
    return data

def get_hbase_paginated(page=1, page_size=50, search_query=""):
    """
    Phân trang Server-side có đếm tổng số lượng bản ghi.
    """
    data = []
    connection = None
    total_records = 0
    total_pages = 0
    
    try:
        connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, timeout=10000)
        connection.open()
        table = connection.table(TABLE_NAME)
        
        # BƯỚC 1: LẤY DANH SÁCH TẤT CẢ KEYS (ID) KHỚP ĐIỀU KIỆN
        # Chúng ta chỉ quét keys để đếm cho nhanh, chưa lấy dữ liệu chi tiết
        all_matching_keys = []
        
        # scan() trả về generator, chúng ta duyệt qua để lọc ID
        # Sử dụng columns=[] để chỉ lấy RowKey (tối ưu tốc độ) hoặc lấy 1 cột nhỏ
        for key, _ in table.scan(columns=[b'prediction:risk_label']):
            student_id = key.decode('utf-8')
            
            # Lọc theo Search Query (nếu có)
            if search_query and search_query.lower() not in student_id.lower():
                continue
                
            all_matching_keys.append(student_id)
            
        # BƯỚC 2: TÍNH TOÁN PHÂN TRANG
        total_records = len(all_matching_keys)
        if total_records > 0:
            total_pages = math.ceil(total_records / page_size)
        else:
            total_pages = 1

        # Đảm bảo trang hiện tại hợp lệ
        if page < 1: page = 1
        if page > total_pages: page = total_pages

        # Xác định chỉ số bắt đầu và kết thúc (Slicing)
        start_index = (page - 1) * page_size
        end_index = start_index + page_size
        
        # Cắt lấy danh sách ID của trang hiện tại
        page_keys = all_matching_keys[start_index:end_index]
        
        # BƯỚC 3: TRUY VẤN CHI TIẾT (GET BATCH)
        # Chỉ lấy dữ liệu đầy đủ cho các ID trong trang này
        if page_keys:
            # Chuyển đổi string ID về bytes để query HBase
            keys_as_bytes = [k.encode('utf-8') for k in page_keys]
            rows = table.rows(keys_as_bytes)
            
            # table.rows trả về list tuple (key, data), ta map lại vào dict
            for key, value in rows:
                data.append({
                    'id': key.decode('utf-8'),
                    'clicks': float(value.get(b'info:clicks', b'0')),
                    'score': float(value.get(b'info:avg_score', b'0')),
                    'risk': int(value.get(b'prediction:risk_label', b'0'))
                })

    except Exception as e:
        print(f"HBase Error: {e}")
    finally:
        if connection: connection.close()

    return {
        'data': data,
        'total_records': total_records,
        'total_pages': total_pages,
        'current_page': page
    }

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/students')
def students():
    # Mặc định page_size là 50 theo yêu cầu
    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('page_size', 50, type=int)
    search = request.args.get('search', '', type=str)

    # Gọi hàm xử lý
    result = get_hbase_paginated(page, page_size, search)
    
    return render_template(
        'students.html', 
        students=result['data'],
        page=result['current_page'],
        page_size=page_size,
        search=search,
        total_pages=result['total_pages'],
        total_records=result['total_records']
    )

@app.route('/api/realtime-data')
def realtime_data():
    """API cho biểu đồ"""
    # Biểu đồ cũng nên vẽ hết để chính xác, hoặc limit lớn (vd: 10000)
    data = get_hbase_data(limit=None)
    
    total = len(data)
    risk = sum(1 for x in data if x['risk'] == 1)
    safe = total - risk
    
    return jsonify({
        'raw_data': data,
        'summary': {
            'total': total,
            'risk': risk,
            'safe': safe
        }
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)