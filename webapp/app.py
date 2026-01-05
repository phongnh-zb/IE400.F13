# webapp/app.py
import happybase
from flask import Flask, jsonify, render_template

app = Flask(__name__)

# Cấu hình kết nối HBase
HBASE_HOST = 'localhost'
TABLE_NAME = 'student_predictions'

def get_hbase_data():
    """Hàm kết nối và lấy dữ liệu từ HBase"""
    connection = happybase.Connection(HBASE_HOST)
    table = connection.table(TABLE_NAME)
    
    # Lấy 500 sinh viên đầu tiên để hiển thị (Scan)
    # Trong thực tế Big Data, ta sẽ query theo ID hoặc filter
    data = []
    try:
        for key, value in table.scan(limit=500):
            # Decode dữ liệu từ byte sang string/float
            student_id = key.decode('utf-8')
            clicks = float(value.get(b'info:clicks', b'0'))
            score = float(value.get(b'info:avg_score', b'0'))
            risk = int(value.get(b'prediction:risk_label', b'0')) # 0: Normal, 1: Risk
            
            data.append({
                'id': student_id,
                'clicks': clicks,
                'score': score,
                'risk': risk
            })
    except Exception as e:
        print(f"Error reading HBase: {e}")
    finally:
        connection.close()
        
    return data

@app.route('/')
def index():
    """Trang chủ hiển thị Dashboard"""
    return render_template('index.html')

@app.route('/api/realtime-data')
def realtime_data():
    """API trả về JSON để Chart.js cập nhật"""
    data = get_hbase_data()
    
    # Tính toán thống kê nhanh
    total_students = len(data)
    at_risk_count = sum(1 for x in data if x['risk'] == 1)
    safe_count = total_students - at_risk_count
    
    return jsonify({
        'raw_data': data,
        'summary': {
            'total': total_students,
            'risk': at_risk_count,
            'safe': safe_count
        }
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)