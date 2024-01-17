# PREPROCESSING: Thực hiện các phép biến đổi trên cột 'text'
from spark.utils import *
from spark.libs import *

# Input: Đường dẫn của file dữ liệu .csv cần tiền xử lý, Đường dẫn của file dữ liệu .csv đã tiền xử lý, một sparksession đã được tạo, dữ liệu chuẩn để tiền xử lý học theo
# Output: Lưu ra thành một file dữ liệu .csv bên trong đường dẫn
# Điều kiện để chạy: Dữ liệu đưa vào phải có cột text, cột label
def preprocess(spark_session, data_input_path, df_fit_path, data_output_path):
    # Đọc các dữ liệu từ đường dẫn sang dataframe
    df = read_csv (spark_session, data_input_path)
    df_fit = read_csv (spark_session, df_fit_path)

    # PREPROCESSING
    # Thay đổi giá trị trong cột 'text', cụ thể xem ở từng bước
    df = (
        df \
        # Chuyển đổi về chữ thường
        .withColumn('text', lower(col('text'))) \
        
        # Loại bỏ các ký tự trong dấu ngoặc vuông
        .withColumn('text', regexp_replace(col('text'), '\[.*?\]', '')) \
        
        # Loại bỏ URL
        .withColumn('text', regexp_replace(col('text'), 'https?://\S+|www\.\S+', '')) \
        
        # Loại bỏ các thẻ HTML
        .withColumn('text', regexp_replace(col('text'), '<.*?>+', '')) \
        
        # Loại bỏ dấu câu
        .withColumn('text', regexp_replace(col('text'), '[%s]' % re.escape(string.punctuation), '')) \
        
        # Loại bỏ dấu xuống dòng
        .withColumn('text', regexp_replace(col('text'), '\n', '')) \
        
        # Loại bỏ từ có chứa số
        .withColumn('text', regexp_replace(col('text'), '\w*\d\w*', ''))
    )

    # Cấu tạo biến tokenizer có chức năng tách đoạn văn bản trong cột 'text' thành các từ riêng và lưu vào cột 'words'. VD: 'Tôi là AI' -> 'Tôi', 'là', 'AI'
    tokenizer = RegexTokenizer(inputCol='text', outputCol='words', pattern='\\W')

    # Cấu tạo biến remover loại bỏ các từ được coi là StopWords trong cột 'words' là lưu vào cột 'filtered'
    remover = StopWordsRemover(inputCol='words', outputCol='filtered')

    # Cấu tạo biến pineline như thứ tự hoạt động các chức năng
    pipeline = Pipeline(stages=[tokenizer, remover])

    # Thực hiện tokenizer và remover
    df = pipeline.fit(df_fit).transform(df)

    # Xóa các hàng có giá trị null từ DataFrame
    df = df.na.drop()

    # Chuyển đổi cột 'words' (kiểu ARRAY<STRING>) thành cột 'words_str' (chuỗi)
    df = df.withColumn('words_str', concat_ws(' ', 'words'))

    # Chuyển đổi cột 'filtered' (kiểu ARRAY<STRING>) thành cột 'filtered_str' (chuỗi)
    df = df.withColumn('filtered_str', concat_ws(' ', 'filtered'))

    # Loại bỏ cột 'words' và 'filters' vì 2 cột này có nếu kiểu ARRAY<STRING> không thể xuất file csv
    df = df.drop('words', 'filtered')

    # Xuất file dữ liệu vào đường dẫn /data/preprocessed 
    write_csv (df, data_output_path)