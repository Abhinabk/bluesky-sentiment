from encodings import utf_8

from Ingestion.KafkaWrappers.AdminWrapper import KafkaAdmin
from Ingestion.KafkaWrappers.ProducerWrapper import KafkaProducer
from Ingestion.feed import get_feed
from config import BOOTSTRAP_SERVERS, RAW_TOPIC
import json
import logging
import asyncio

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s-%(levelname)s-%(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# create the topic
admin = KafkaAdmin(BOOTSTRAP_SERVERS)
admin.create_topics(RAW_TOPIC)

# create the producer
producerObj = KafkaProducer(BOOTSTRAP_SERVERS,
                            enable_idempotence=True,
                            value_serializer=lambda value, ctx: json.dumps(value).encode("utf-8"),
                            key_serializer=lambda key, ctx: key.encode("utf-8")
                            )

def flat_events(event: dict)->dict:
    # flattens the events to match the avro schema
    # absent = None, empty = "" (get will return None for missing fields and the blusky returns "" for empty fields)
    return_events = dict()
    return_events["did"] = event.get("did")
    return_events["rkey"] = event.get("commit", {}).get("rkey")
    return_events["createdAt"] = event.get("commit", {}).get("record", {}).get("createdAt")
    return_events["time_us"] = event.get("time_us")
    return_events["text"] =  event.get("commit", {}).get("record", {}).get("text")
    return_events["langs"] = event.get("commit", {}).get("record", {}).get("langs")
    return  return_events
# send the data to the topic
async def produce(producer: KafkaProducer):
    try:
        async for item in get_feed():
            item = flat_events(item)
            producer.produce_msg(topic=RAW_TOPIC[0], value=item, key=None)
    finally:
        producer.flush()


def main():
    try:
        asyncio.run(produce(producerObj))
    except KeyboardInterrupt:
        print("Interrupt")


if __name__ == "__main__":
    main()
