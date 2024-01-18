from spark.utils import *
from spark.libs import *

# Dùng model sử dụng cột 'words' để dự đoán
# Input: Đường dẫn của file dữ liệu .csv để dự đoán, đường dẫn của model dự đoán bằng cột 'words', một spark_session đã được tạo
# Output: Kết quả dự đoán của model
# Điều kiện để chạy: Dữ liệu đưa vào phải qua bước Preprocessing thì mới chạy được, đã có model dự đoán bằng cột 'words' sẵn
def predict_model_words(spark_session, data_input_path, model_predict_path):
    # Đọc các dữ liệu từ đường dẫn sang dataframe
    test_data = read_csv (spark_session, data_input_path)

    # Thực hiện tokenizer cột 'text' thành các từ riêng và lưu vào cột 'words'.
    # VD: 'Tôi là AI' -> 'Tôi', 'là', 'AI'
    test_data = tokenize(test_data)

    # Load the trained model
    predict_model = PipelineModel.load(model_predict_path)

    # Dự đoán trên dữ liệu kiểm thử và tạo cột 'label_pred'
    predictions = predict_model.transform(test_data)
    
    return predictions

def calc_predict_acc (spark_session, data_input_path, **kwargs):
    ti = kwargs['ti']
    model_predict_path = ti.xcom_pull(key='model_output_path')

    # Lấy kết quả dự đoán
    predictions = predict_model_words(spark_session, data_input_path, model_predict_path)

    # Tổng số hàng
    total_rows = predictions.count()

    # Đối chiếu và đếm số lượng giá trị giống nhau
    matching_rows = predictions.filter(col('label') == col('label_pred'))
    count_matching_rows = matching_rows.count()

    # Tính phần trăm giống nhau
    percentage_matching = (count_matching_rows/ total_rows) * 100

    # Hiển thị kết quả
    print('-----------------------------------------')
    print('DỰ ĐOÁN DỰA TRÊN ACCURACY')
    print(f'Số lượng hàng giống nhau: {count_matching_rows}')
    print(f'Tổng số hàng: {total_rows}')
    print(f'Phần trăm giống nhau: {percentage_matching}%')
    print('-----------------------------------------')

    # Push predict_acc_result to xcom
    ti.xcom_push(key='predict_acc_result', value={
        'percentage_matching':percentage_matching,
        'model_path':model_predict_path,
    })

# Dự đoán bằng model SVM    
# predictions = predict_model_words('./data/preprocessed.csv', './model/SVM_words', spark)

# In kết quả qua thang đo Accuracy
# export_result_Acc (predictions)

# In kết quả qua thang đo F1 manual
# export_result_F1_manual (predictions)

# In kết quả qua thang đo F1
# export_result_F1 (predictions)

# In kết quả qua thang đo F1 cross-validation
# export_result_F1cross_words ('./data/preprocessed.csv', './model/SVM_words', spark)