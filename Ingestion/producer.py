from Ingestion.KafkaWrappers.AdminWrapper import KafkaAdmin
from Ingestion.KafkaWrappers.ProducerWrapper import KafkaProducer
from Ingestion.feed import get_feed
from  config import  BOOTSTRAP_SERVERS,RAW_TOPIC
import json
import logging
import  asyncio

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s-%(levelname)s-%(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

#create the topic
admin = KafkaAdmin(BOOTSTRAP_SERVERS)
admin.create_topics(RAW_TOPIC)

# create the producer
producerObj = KafkaProducer(BOOTSTRAP_SERVERS,
                            enable_idempotence=True,
                            value_serializer=lambda value, ctx: json.dumps(value).encode("utf-8"),
                            key_serializer=lambda key, ctx: key.encode("utf-8")
                            )
# send the data to the topic
async def produce(producer:KafkaProducer):
    try:
        async for items in get_feed():
            producer.produce_msg(topic=RAW_TOPIC[0],value=items,key=None)
    except KeyboardInterrupt:
        print("Interrupt")
    finally:
        producer.flush()

def main():
    asyncio.run(produce(producerObj))

if __name__ == "__main__":
    main()


