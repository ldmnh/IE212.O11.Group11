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
SCALA_VERSION = '2.12'
SPARK_VERSION = '3.5.0'
KAFKA_VERSION = '3.6.0'
KAFKA_TEST_TOPIC = 'test-set'
KAFKA_CRAWL_TOPIC = 'crawl-set'
KAFKA_DETECTED_TOPIC = 'detected-result'

SPARK_STREAM_PACKAGE = [
    f'org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}',
    f'org.apache.kafka:kafka-clients:{KAFKA_VERSION}'
]

SPARK_MASTER_HOST = 'spark://127.0.0.1:1909'
SPARK_OFFLINE_APP_NAME = 'Offline System - Training & Choose Best Model'
SPARK_ONLINE_APP_NAME = 'Online System - Real-time Stress Detection'

DETECTED_RESULT_CSV_PATH = os.path.expanduser('~/code/IE212.O11.Group11/data/detected-result')
DETECTED_RESULT_CSV_CHECKPOINT_PATH = os.path.expanduser('~/code/IE212.O11.Group11/checkpoints/detected-result')

SUBREDDITS = [
    'domesticviolence',
    # 'survivorsofabuse', # Private subreddit
    'anxiety',
    'stress',
    'almosthomeless',
    'assistance',
    # 'food_pantry', # Deleted subreddit
    'homeless',
    'ptsd',
    'relationships'
]
REDDIT_CLIENT_ID = 'your_client_id'
REDDIT_CLIENT_SECRET = 'your_secret_id'
REDDIT_USER_AGENT = 'Stress Detection Crawling by u/your_user_name'
REDDIT_USER_NAME = 'your_user_name'
REDDIT_PASSWORD = 'your_password'

CRAWL_LIMIT = 200
CRAWL_TRIGGER_LIMIT = 1
CRAWL_TRIGGER_TIME = 2
STREAM_TRIGGER_TIME = '1 seconds'
