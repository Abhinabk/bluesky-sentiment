from config import BOOTSTRAP_SERVERS, GROUP_ID, TOPICS
from confluent_kafka import Consumer,KafkaError
import  logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s-%(levelname)s-%(name)s: %(message)s"
)
logger = logging.getLogger(__name__)
consumer = Consumer({
    'bootstrap.servers': BOOTSTRAP_SERVERS[0],
    'group.id':GROUP_ID,
    'auto.offset.reset': 'earliest'
})
def basic_consumer_loop():
    try:
        consumer.subscribe([TOPICS.enriched])
        while True:
            msg = consumer.poll(1.0)
           # no msg has come yet
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
            print(f"{msg.value().decode()}")
    finally:
        consumer.close()

if __name__ == "__main__":
    basic_consumer_loop()



