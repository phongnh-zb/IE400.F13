# webapp/app.py
import happybase
from flask import Flask, jsonify, render_template

app = Flask(__name__)

# Cấu hình HBase
HBASE_HOST = 'localhost'
HBASE_PORT = 9090
TABLE_NAME = 'student_predictions'

def get_hbase_data(limit=100):
    """Hàm lấy dữ liệu từ HBase có tham số giới hạn"""
    data = []
    connection = None
    try:
        connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, timeout=5000)
        connection.open()
        table = connection.table(TABLE_NAME)
        
        # Scan dữ liệu
        for key, value in table.scan(limit=limit):
            student_id = key.decode('utf-8')
            clicks = float(value.get(b'info:clicks', b'0'))
            score = float(value.get(b'info:avg_score', b'0'))
            risk = int(value.get(b'prediction:risk_label', b'0'))
            
            data.append({
                'id': student_id,
                'clicks': clicks,
                'score': score,
                'risk': risk
            })
    except Exception as e:
        print(f"Lỗi HBase: {e}")
    finally:
        if connection: connection.close()
    return data

@app.route('/')
def index():
    """Trang Dashboard (Biểu đồ)"""
    return render_template('index.html')

@app.route('/students')
def students():
    """Trang Danh sách chi tiết (Table)"""
    # Lấy dữ liệu server-side để hiển thị lên bảng
    students_list = get_hbase_data(limit=100) 
    return render_template('students.html', students=students_list)

@app.route('/api/realtime-data')
def realtime_data():
    """API cho Dashboard"""
    data = get_hbase_data(limit=500) # Lấy nhiều hơn cho biểu đồ
    total = len(data)
    risk = sum(1 for x in data if x['risk'] == 1)
    safe = total - risk
    
    return jsonify({
        'raw_data': data,
        'summary': {'total': total, 'risk': risk, 'safe': safe}
    })

if __name__ == '__main__':
    app.run(debug=True, port=5001)