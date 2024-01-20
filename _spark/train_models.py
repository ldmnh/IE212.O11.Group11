# Import libs
from pyspark.ml import Pipeline
from pyspark.ml.feature import Word2Vec
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.classification import LinearSVC

# Import custom modules
from _spark.utils import read_csv, tokenize, load_model_W2V

# Training model Word2Vec để sử dụng trong quá trình dự đoán
# Input: Đường dẫn của file dữ liệu .csv để training cho model W2V, đường dẫn xuất và lưu của model W2V, một sparksession đã được tạo
# Output: Lưu ra thành một model đã được huấn luyện bên trong đường dẫn
# Điều kiện để chạy: Dữ liệu đưa vào phải qua bước Preprocessing thì mới chạy được
def W2V(data_input_path, model_output_path):
    # Đọc các dữ liệu từ đường dẫn sang dataframe
    df = read_csv (data_input_path)

    # Thực hiện tokenizer cột 'text' thành các từ riêng và lưu vào cột 'words'.
    # VD: 'Tôi là AI' -> 'Tôi', 'là', 'AI'
    df = tokenize(df)

    # TRAINING MODEL W2V
    # 'words' là cột chứa danh sách từ đã được tách
    sentences = df.select('words')

    # Khởi tạo và cấu hình mô hình Word2Vec
    word2Vec = Word2Vec(vectorSize=100, minCount=1, inputCol='words', outputCol='wordEmbeddings')

    # Huấn luyện mô hình Word2Vec trên dữ liệu
    w2v_model = word2Vec.fit(sentences)

    # Lưu model vào đường dẫn /models/W2V
    w2v_model_path = model_output_path
    w2v_model.write().overwrite().save(w2v_model_path)

# Training model SVM với cột 'words' để sử dụng trong quá trình dự đoán
# Input: Đường dẫn của file dữ liệu .csv để training cho model SVM, đường dẫn xuất và lưu của model SVM, một sparksession đã được tạo
# Output: Lưu ra thành một model đã được huấn luyện bên trong đường dẫn
# Điều kiện để chạy: Dữ liệu đưa vào phải qua bước Preprocessing thì mới chạy được, đã có model W2V sẵn
def SVM(data_input_path, model_w2v_path, model_output_path, **kwargs):
    # Đọc các dữ liệu từ đường dẫn sang dataframe
    df = read_csv (data_input_path)

    # Thực hiện tokenizer cột 'text' thành các từ riêng và lưu vào cột 'words'.
    # VD: 'Tôi là AI' -> 'Tôi', 'là', 'AI'
    df = tokenize(df)

    # Load the trained model
    w2v_model = load_model_W2V(model_w2v_path)

    # TRAINING MODEL SVM
    # Tạo một mô hình SVM
    svm = LinearSVC(featuresCol='wordEmbeddings', labelCol='label', predictionCol='label_pred')

    # Tạo pipeline với mô hình SVM
    pipeline_svm_words = Pipeline(stages=[w2v_model, svm])

    # Huấn luyện pipeline trên dữ liệu huấn luyện
    model_svm_words = pipeline_svm_words.fit(df)

    # Lưu model vào đường dẫn /models/SVM
    SVM_words_model_path = model_output_path
    model_svm_words.write().overwrite().save(SVM_words_model_path)

    # Push model_output_path to xcom
    ti = kwargs['ti']
    ti.xcom_push(key='model_output_path', value=model_output_path)

# Training model RandomForest với cột 'words' để sử dụng trong quá trình dự đoán
# Input: Đường dẫn của file dữ liệu .csv để training cho model RandomForest, đường dẫn xuất và lưu của model RandomForest, một sparksession đã được tạo
# Output: Lưu ra thành một model đã được huấn luyện bên trong đường dẫn
# Điều kiện để chạy: Dữ liệu đưa vào phải qua bước Preprocessing thì mới chạy được, đã có model W2V sẵn
def RandomForest(data_input_path, model_w2v_path, model_output_path, **kwargs):
    # Đọc các dữ liệu từ đường dẫn sang dataframe
    df = read_csv (data_input_path)

    # Thực hiện tokenizer cột 'text' thành các từ riêng và lưu vào cột 'words'.
    # VD: 'Tôi là AI' -> 'Tôi', 'là', 'AI'
    df = tokenize(df)

    # Load the trained model
    w2v_model = load_model_W2V(model_w2v_path)

    # TRAINING MODEL RandomForest
    # Tạo một mô hình RandomForest
    rf = RandomForestClassifier(featuresCol='wordEmbeddings', labelCol='label', predictionCol='label_pred')

    # Tạo pipeline với mô hình RandomForest
    pipeline_rf_words = Pipeline(stages=[w2v_model, rf])

    # Huấn luyện pipeline trên dữ liệu huấn luyện
    model_rf_words = pipeline_rf_words.fit(df)

    # Lưu model vào đường dẫn /models/RandomForest
    RandomForest_words_model_path = model_output_path
    model_rf_words.write().overwrite().save(RandomForest_words_model_path)

    # Push model_output_path to xcom
    ti = kwargs['ti']
    ti.xcom_push(key='model_output_path', value=model_output_path)

# Training model LogisticRegression với cột 'words' để sử dụng trong quá trình dự đoán
# Input: Đường dẫn của file dữ liệu .csv để training cho model LogisticRegression, đường dẫn xuất và lưu của model LogisticRegression, một sparksession đã được tạo
# Output: Lưu ra thành một model đã được huấn luyện bên trong đường dẫn
# Điều kiện để chạy: Dữ liệu đưa vào phải qua bước Preprocessing thì mới chạy được, đã có model W2V sẵn
def LR(data_input_path, model_w2v_path, model_output_path, **kwargs):
    # Đọc các dữ liệu từ đường dẫn sang dataframe
    df = read_csv (data_input_path)

    # Thực hiện tokenizer cột 'text' thành các từ riêng và lưu vào cột 'words'.
    # VD: 'Tôi là AI' -> 'Tôi', 'là', 'AI'
    df = tokenize(df)

    # Load the trained model
    w2v_model = load_model_W2V(model_w2v_path)

    # TRAINING MODEL LogisticRegression
    # Tạo một mô hình LogisticRegression
    lr = LogisticRegression(featuresCol='wordEmbeddings', labelCol='label', predictionCol='label_pred')

    # Tạo pipeline với mô hình LogisticRegression
    pipeline_lr_words = Pipeline(stages=[w2v_model, lr])

    # Huấn luyện pipeline trên dữ liệu huấn luyện
    model_lr_words = pipeline_lr_words.fit(df)

    # Lưu model vào đường dẫn /models/LogisticRegression
    LogisticRegression_words_model_path = model_output_path
    model_lr_words.write().overwrite().save(LogisticRegression_words_model_path)

    # Push model_output_path to xcom
    ti = kwargs['ti']
    ti.xcom_push(key='model_output_path', value=model_output_path)

# Training model GradientBoosted với cột 'words' để sử dụng trong quá trình dự đoán
# Input: Đường dẫn của file dữ liệu .csv để training cho model GradientBoosted, đường dẫn xuất và lưu của model GradientBoosted, một sparksession đã được tạo
# Output: Lưu ra thành một model đã được huấn luyện bên trong đường dẫn
# Điều kiện để chạy: Dữ liệu đưa vào phải qua bước Preprocessing thì mới chạy được, đã có model W2V sẵn
def GradientBoosted(data_input_path, model_w2v_path, model_output_path, **kwargs):
    # Đọc các dữ liệu từ đường dẫn sang dataframe
    df = read_csv (data_input_path)

    # Thực hiện tokenizer cột 'text' thành các từ riêng và lưu vào cột 'words'.
    # VD: 'Tôi là AI' -> 'Tôi', 'là', 'AI'
    df = tokenize(df)

    # Load the trained model
    w2v_model = load_model_W2V(model_w2v_path)

    # TRAINING MODEL GradientBoosted
    # Tạo một mô hình GradientBoosted
    gbt = GBTClassifier(featuresCol='wordEmbeddings', labelCol='label', predictionCol='label_pred')

    # Tạo pipeline với mô hình GradientBoosted
    pipeline_gbt_words = Pipeline(stages=[w2v_model, gbt])

    # Huấn luyện pipeline trên dữ liệu huấn luyện
    model_gbt_words = pipeline_gbt_words.fit(df)

    # Lưu model vào đường dẫn ./model/GradientBoosted_word
    GradientBoosted_words_model_path = model_output_path
    model_gbt_words.write().overwrite().save(GradientBoosted_words_model_path)

    # Push model_output_path to xcom
    ti = kwargs['ti']
    ti.xcom_push(key='model_output_path', value=model_output_path)

# Training model DecisionTrees với cột 'words' để sử dụng trong quá trình dự đoán
# Input: Đường dẫn của file dữ liệu .csv để training cho model DecisionTrees, đường dẫn xuất và lưu của model DecisionTrees, một sparksession đã được tạo
# Output: Lưu ra thành một model đã được huấn luyện bên trong đường dẫn
# Điều kiện để chạy: Dữ liệu đưa vào phải qua bước Preprocessing thì mới chạy được, đã có model W2V sẵn
def DecisionTrees(data_input_path, model_w2v_path, model_output_path, **kwargs):
    
    # Đọc các dữ liệu từ đường dẫn sang dataframe
    df = read_csv (data_input_path)

    # Thực hiện tokenizer cột 'text' thành các từ riêng và lưu vào cột 'words'.
    # VD: 'Tôi là AI' -> 'Tôi', 'là', 'AI'
    df = tokenize(df)

    # Load the trained model
    w2v_model = load_model_W2V(model_w2v_path)

    # TRAINING MODEL DecisionTrees
    # Tạo một mô hình DecisionTrees
    dt = DecisionTreeClassifier(featuresCol='wordEmbeddings', labelCol='label', predictionCol='label_pred')

    # Tạo pipeline với mô hình DecisionTrees
    pipeline_dt_words = Pipeline(stages=[w2v_model, dt])

    # Huấn luyện pipeline trên dữ liệu huấn luyện
    model_dt_words = pipeline_dt_words.fit(df)

    # Lưu model vào đường dẫn ./model/DecisionTrees_word
    DecisionTrees_words_model_path = model_output_path
    model_dt_words.write().overwrite().save(DecisionTrees_words_model_path)

    # Push model_output_path to xcom
    ti = kwargs['ti']
    ti.xcom_push(key='model_output_path', value=model_output_path)