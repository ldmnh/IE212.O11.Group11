# Import libs
import csv, json
from time import sleep
from kafka import KafkaProducer
import praw

# Import custom modules
from _constants import *

def produce_csv():
    producer=KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    csv_file_path=TEST_SET_PATH

    # Read the CSV file and publish each row to the Kafka topic
    with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            # Convert the row dictionary to a JSON string
            json_message = json.dumps(row)
            
            # Publish the JSON message to the Kafka topic
            producer.send(topic=KAFKA_TEST_TOPIC, value=json_message.encode('utf-8'))

            print('INSERTED: ', json_message)
            sleep(CRAWL_TRIGGER_TIME)

def produce_praw():
    # Create kafka producer
    producer=KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

    # Use PRAW API
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
        username=REDDIT_USER_NAME,
        password=REDDIT_PASSWORD,
    )

    count = 0
    # Crawling
    while count < 500:
        for sub in SUBREDDITS:
            subreddit = reddit.subreddit(sub)
            posts = subreddit.random_rising(limit=CRAWL_LIMIT)

            print('Subreddit: r/', sub)
            print('-------------------------------------------')
            for post in posts:
                count = count + 1
                post_info = {
                    'subreddit': post.subreddit.display_name,
                    'post_id': post.id,
                    'text': post.selftext,
                    'social_timestamp': post.created_utc,
                    'url': post.url,
                }

                post_json = json.dumps(post_info)
                producer.send(topic=KAFKA_CRAWL_TOPIC, value=post_json.encode('utf-8'))
                print('INSERTED: ', post_json)
            print('-------------------------------------------')
        sleep(CRAWL_TRIGGER_TIME)