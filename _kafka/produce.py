# Import libs
import csv, json
from time import sleep
from kafka import KafkaProducer

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
            producer.send(topic=KAFKA_TEST_TOPIC,value=json_message.encode('utf-8'))

            print('INSERTED: ', json_message)
            sleep(1)