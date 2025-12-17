import matplotlib.pyplot as plt
import numpy as np

# Dữ liệu từ kết quả chạy của bạn
models = ['Logistic Regression', 'Random Forest', 'GBT']
auc_scores = [0.8553, 0.8634, 0.8670]
times = [2.60, 1.61, 2.37]

x = np.arange(len(models))  # Vị trí các nhãn
width = 0.35  # Độ rộng cột

fig, ax1 = plt.subplots(figsize=(10, 6))

# Vẽ biểu đồ cột cho AUC (Trục trái)
rects1 = ax1.bar(x - width/2, auc_scores, width, label='AUC Score', color='#4CAF50')
ax1.set_ylabel('AUC Score', color='#4CAF50', fontsize=12)
ax1.set_ylim(0.8, 0.9)  # Zoom vào khoảng 0.8 - 0.9 để thấy sự chênh lệch rõ hơn
ax1.tick_params(axis='y', labelcolor='#4CAF50')

# Tạo trục thứ 2 cho Thời gian (Trục phải)
ax2 = ax1.twinx()
rects2 = ax2.plot(x, times, label='Training Time (s)', color='#FF5722', marker='o', linewidth=2, markersize=8)
ax2.set_ylabel('Time (seconds)', color='#FF5722', fontsize=12)
ax2.set_ylim(0, 4)
ax2.tick_params(axis='y', labelcolor='#FF5722')

# Thêm nhãn và tiêu đề
ax1.set_xticks(x)
ax1.set_xticklabels(models, fontsize=11)
plt.title('Comparison of Big Data Models: AUC vs Time', fontsize=14, fontweight='bold')

# Hiển thị giá trị lên đầu cột AUC
def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        ax1.annotate(f'{height:.4f}',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom', fontweight='bold')

autolabel(rects1)

# Lưu thành file ảnh
plt.tight_layout()
plt.savefig('model_comparison.png')
print(">>> Đã lưu biểu đồ vào file 'model_comparison.png'")