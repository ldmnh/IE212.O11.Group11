# Import libs
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

# Import custom modules
from _constants import *
from _spark.predict import predict_stream
from _spark.preprocess import preprocess_df

def structured_stream():
    # Spark Session
    spark_session = SparkSession.builder \
        .master(SPARK_MASTER_HOST) \
        .appName(SPARK_ONLINE_APP_NAME) \
        .config('spark.jars.packages',','.join(SPARK_STREAM_PACKAGE)) \
        .config('spark.dynamicAllocation.enabled','true') \
        .getOrCreate()

    # Read streaming dataframe from kafka
    streaming_df = (
        spark_session.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', BOOTSTRAP_SERVERS) \
            .option('subscribe', KAFKA_TEST_TOPIC) \
            .option('startingOffsets', 'earliest') \
            .load()
    )

    # Extract the JSON string from the Kafka message
    json_column = col('value').cast('string')

    # Transfer dataframe use infer schema
    infer_schema_df = (streaming_df.select(from_json(json_column, 'map<string, string>').alias('parsed_data')))

    # Select individual fields from the map
    streaming_df = infer_schema_df.selectExpr(
        'parsed_data.id',
        'parsed_data.subreddit',
        'parsed_data.post_id',
        'parsed_data.sentence_range',
        'parsed_data.text',
        'parsed_data.social_timestamp',
    )

    # Preprocess
    preprocessed_df = preprocess_df(streaming_df, TRAIN_SET_PATH)

    # Choose best model
    folder_path = os.path.expanduser(f'~/code/IE212.O11.Group11/models')
    folders = os.listdir(folder_path)
    best_model_folders = [folder for folder in folders if folder.startswith("Best_model_")]
    best_model_path = os.path.join(folder_path, best_model_folders[0])
    
    # Predict
    predicted_df = predict_stream(preprocessed_df, best_model_path)

    # Write predicted to csv
    predicted_df.writeStream \
            .format('csv') \
            .trigger(processingTime='20 seconds') \
            .option('header', 'true') \
            .option('path', PREDICT_RESULT_CSV_PATH) \
            .option('checkpointLocation', PREDICT_RESULT_CSV_CHECKPOINT_PATH) \
            .option('format', 'append') \
            .outputMode('append') \
            .start() \
            .awaitTermination()