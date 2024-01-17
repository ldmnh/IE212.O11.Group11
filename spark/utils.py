from spark.libs import *

def create_spark_session (master_host, app_name):
    spark = SparkSession.builder.master(master_host).appName(app_name).getOrCreate()

    return spark

def read_csv (spark_session, input_path):
    df_input = spark_session.read.csv(input_path, header=True, inferSchema=True)
    df_input = df_input.withColumn('label', col('label').cast('int'))

    return df_input

def write_csv (df_output, output_path):
    df_output.write.csv(output_path, header=True, mode='overwrite')

def stop_spark_session (spark_session):
    if spark_session:
        spark_session.stop()

def tokenize (data_input_df):
    # Cấu tạo biến tokenizer có chức năng tách đoạn văn bản trong cột 'text' thành các từ riêng và lưu vào cột 'words'. VD: 'Tôi là AI' -> 'Tôi', 'là', 'AI'
    tokenizer = RegexTokenizer(inputCol='words_str', outputCol='words', pattern='\\W')

    # Cấu tạo biến pineline như thứ tự hoạt động các chức năng
    pipeline = Pipeline(stages=[tokenizer])
    
    # Thực hiện tokenizer và remover
    data_input_df = pipeline.fit(data_input_df).transform(data_input_df)

    return data_input_df

def load_model_W2V (w2v_model_path):
    # Load the trained model
    model_w2v_path = w2v_model_path
    w2v_model = Word2VecModel.load(model_w2v_path)

    return w2v_model

def export_result_Acc (predictions):
    # Tổng số hàng
    total_rows = predictions.count()

    # Đối chiếu và đếm số lượng giá trị giống nhau
    matching_rows = predictions.filter(col('label') == col('label_pred'))
    count_matching_rows = matching_rows.count()

    # Tính phần trăm giống nhau
    percentage_matching = (count_matching_rows/ total_rows) * 100

    # Hiển thị kết quả
    print('-----------------------------------------')
    print('DỰ ĐOÁN DỰ TRÊN ACCURACY')
    print(f'Số lượng hàng giống nhau: {count_matching_rows}')
    print(f'Tổng số hàng: {total_rows}')
    print(f'Phần trăm giống nhau: {percentage_matching}%')
    print('-----------------------------------------')

def export_result_F1 (predictions):
    # Tính F1 score
    evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='label_pred', metricName='f1')
    f1_score = evaluator.evaluate(predictions)

    # In ra F1 score
    print('-----------------------------------------')
    print('DỰ ĐOÁN DỰ TRÊN F1')
    print('F1 Score: {}'.format(f1_score))
    print('-----------------------------------------')

def export_result_F1_cross_words (training_data_path, model_predict_path, sparksession):
    # Đọc các dữ liệu từ đường dẫn sang dataframe
    training_data = read_csv (sparksession, training_data_path)

    # Thực hiện tokenizer cột 'text' thành các từ riêng và lưu vào cột 'words'. VD: 'Tôi là AI' -> 'Tôi', 'là', 'AI'
    training_data = tokenize(training_data)

    # Load the trained model
    w2v_model = load_model_W2V('./model/w2v_model')

    # Load the trained model
    predict_model = PipelineModel.load(model_predict_path)

    # Extract the classifier model from the pipeline
    classifier_model = predict_model.stages[-1]

    # Create a new pipeline with your classifier model
    new_pipeline = Pipeline(stages=[w2v_model, classifier_model])

    # Cài dặt phương thưc tính F1 score
    evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='label_pred', metricName='f1')

    # Define the parameter grid for cross-validation
    paramGrid = ParamGridBuilder().build()

    # Create CrossValidator with the pipeline, parameter grid, and evaluator
    crossval = CrossValidator(
        estimator=new_pipeline,
        estimatorParamMaps=paramGrid,
        evaluator=evaluator,
        numFolds=10,
        seed= 19
    )

    # Run cross-validation on the training data
    cv_model = crossval.fit(training_data)

    # Get the F1 score from the best model
    f1_score = cv_model.avgMetrics[0]

    # Print the F1 score
    print('-----------------------------------------')
    print('DỰ ĐOÁN DỰ TRÊN F1 CROSS-VALIDATION')
    print('Cross-validated F1 Score: {:.4f}'.format(f1_score))
    print('-----------------------------------------')

def export_result_F1_manual (predictions):
    # Tính True Positive, False Positive và False Negative
    true_positive = predictions.filter((col('label') == 1) & (col('label_pred') == 1)).count()
    false_positive = predictions.filter((col('label') == 0) & (col('label_pred') == 1)).count()
    true_negative = predictions.filter((col('label') == 0) & (col('label_pred') == 0)).count()
    false_negative = predictions.filter((col('label') == 1) & (col('label_pred') == 0)).count()

    # Tính Precision và Recall
    precision = true_positive / (true_positive + false_positive) if (true_positive + false_positive) != 0 else 0.0
    recall = true_positive / (true_positive + false_negative) if (true_positive + false_negative) != 0 else 0.0

    # In kết quả
    print('-----------------------------------------')
    print('DỰ ĐOÁN DỰA TRÊN F1 MANUAL')
    print('True Positive:', true_positive)
    print('False Positive:', false_positive)
    print('True Negative:', true_negative)
    print('False Negative:', false_negative)
    print('Precision:', precision)
    print('Recall:', recall)
    print('F1-score:', 2 * precision * recall / (precision + recall))
    print('-----------------------------------------')