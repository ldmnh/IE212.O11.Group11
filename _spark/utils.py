# Import libs
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer
from pyspark.ml.feature import Word2VecModel

# Import custom modules
from _constants import *

def read_csv (input_path):
    spark_session = SparkSession.builder \
        .master(SPARK_MASTER_HOST) \
        .appName(SPARK_OFFLINE_APP_NAME) \
        .config('spark.dynamicAllocation.enabled','true') \
        .getOrCreate()

    df_input = spark_session.read.csv(input_path, header=True, inferSchema=True)
    df_input = df_input.withColumn('label', col('label').cast('int'))

    return df_input

def write_csv (df_output, output_path):
    df_output.write.csv(output_path, header=True, mode='overwrite')

def tokenize (data_input_df):
    # Cấu tạo biến tokenizer có chức năng tách đoạn văn bản trong cột 'text' thành các từ riêng và lưu vào cột 'words'.
    # VD: 'Tôi là AI' -> 'Tôi', 'là', 'AI'
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