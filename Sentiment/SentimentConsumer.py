from KafkaWrappers.ProducerWrapper import KafkaProducer
from config import BOOTSTRAP_SERVERS, GROUP_ID, TOPICS
from confluent_kafka import Consumer,KafkaError
import  logging
import  json
from transformers import pipeline

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s-%(levelname)s-%(name)s: %(message)s"
)
logger = logging.getLogger(__name__)
consumer = Consumer({
    'bootstrap.servers': BOOTSTRAP_SERVERS[0],
    'group.id':GROUP_ID,
    'auto.offset.reset': 'earliest'
})
class Sentiment:
    def __init__(self,model="distilbert-base-uncased-finetuned-sst-2-english"):
        self.classifier = pipeline("text-classification",
                                   model = model,
                                   truncation=True)
        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            enable_idempotence=True,
            value_serializer=lambda value,ctx: json.dumps(value).encode("utf-8"),
            key_serializer= None
        )


    def scorer(self,text:str)-> dict[str,str]:
        result = self.classifier(text)[0]
        return result

    def add_to_topic(self,record: dict):
        '''Uses the producer to add records to the topic'''
        # create the producer
        self.producer.produce_msg(
            topic=TOPICS.scored,
            value= record,
            key = None
        )

def basic_consumer_loop()->None:
    sentiment= Sentiment()
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
                    continue
            post =json.loads(msg.value().decode())
            score = sentiment.scorer(post.get("text"))
            post["sentiment"] = score["label"]
            post["score"] = score["score"]
            logger.info(post)
            sentiment.add_to_topic(post)
    finally:
        sentiment.producer.flush()
        consumer.close()



if __name__ == "__main__":
  basic_consumer_loop()



