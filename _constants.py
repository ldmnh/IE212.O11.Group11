import os

TEST_SET_PATH = os.path.expanduser('~/code/IE212.O11.Group11/data/dreaddit-test.csv')
TEST_PREPROCESSED_PATH = os.path.expanduser('~/code/IE212.O11.Group11/data/test-preprocessed')

TRAIN_SET_PATH = os.path.expanduser('~/code/IE212.O11.Group11/data/dreaddit-train.csv')
TRAIN_PREPROCESSED_PATH = os.path.expanduser('~/code/IE212.O11.Group11/data/train-preprocessed')

W2V_MODEL_PATH = os.path.expanduser('~/code/IE212.O11.Group11/models/W2V')
SVM_MODEL_PATH = os.path.expanduser('~/code/IE212.O11.Group11/models/SVM')
RANDOM_FOREST_MODEL_PATH = os.path.expanduser('~/code/IE212.O11.Group11/models/RandomForest')
LOGISTIC_REGRESSION_MODEL_PATH = os.path.expanduser('~/code/IE212.O11.Group11/models/LogisticRegression')
GRADIENT_BOOSTED_MODEL_PATH = os.path.expanduser('~/code/IE212.O11.Group11/models/GradientBoosted')
DECISION_TREES_MODEL_PATH = os.path.expanduser('~/code/IE212.O11.Group11/models/DecisionTrees')

BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TEST_TOPIC = 'test-set'
SCALA_VERSION = '2.12'
SPARK_VERSION = '3.5.0'
KAFKA_VERSION = '3.6.0'

SPARK_STREAM_PACKAGE = [
    f'org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}',
    f'org.apache.kafka:kafka-clients:{KAFKA_VERSION}'
]

SPARK_MASTER_HOST = 'spark://127.0.0.1:1909'
SPARK_OFFLINE_APP_NAME = 'Offline System - Training & Choose Best Model'
SPARK_ONLINE_APP_NAME = 'Online System - Real-time Stress Prediction'

PREDICT_RESULT_CSV_PATH = os.path.expanduser('~/code/IE212.O11.Group11/data/predicted-result')
PREDICT_RESULT_CSV_CHECKPOINT_PATH = os.path.expanduser('~/code/IE212.O11.Group11/data/predicted-result/checkpoint')