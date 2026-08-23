import pathlib

from KafkaWrappers.AdminWrapper import KafkaAdmin
from KafkaWrappers.ProducerWrapper import KafkaProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from config import SCHEMA_REGISTRY_URL
from Ingestion.feed import get_feed
from config import BOOTSTRAP_SERVERS, TOPICS
import logging
import asyncio

from confluent_kafka.schema_registry import SchemaRegistryClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s-%(levelname)s-%(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# create the topic
admin = KafkaAdmin(BOOTSTRAP_SERVERS)
admin.create_topics([TOPICS.raw,TOPICS.enriched,TOPICS.scored])
admin.create_topics([TOPICS.brands],cleanup_policy="compact")



def flat_events(event: dict) -> dict:
    # flattens the events to match the avro schema
    # absent = None, empty = "" (get will return None for missing fields and the blusky returns "" for empty fields)
    return_events = dict()
    return_events["did"] = event.get("did")
    return_events["rkey"] = event.get("commit", {}).get("rkey")
    return_events["createdAt"] = event.get("commit", {}).get("record", {}).get("createdAt")
    return_events["time_us"] = event.get("time_us")
    return_events["text"] = event.get("commit", {}).get("record", {}).get("text")
    return_events["langs"] = event.get("commit", {}).get("record", {}).get("langs")
    return return_events

#create the schema registry client
schema_registry_conf = {'url':SCHEMA_REGISTRY_URL[0]}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

#serialize the schema to Avro
schema_file =  pathlib.Path(__file__).resolve().parent.parent/"Schemas"/"post.avsc"
avro_schema =schema_file.read_text(encoding="utf-8")
avro_serializer = AvroSerializer(schema_registry_client,avro_schema)


# create the producer
producerObj = KafkaProducer(BOOTSTRAP_SERVERS,
                            enable_idempotence=True,
                            value_serializer=avro_serializer,
                            key_serializer=None
                            )

# send the data to the topic
async def produce(producer: KafkaProducer):
    try:
        async for item in get_feed():
            item = flat_events(item)
            producer.produce_msg(topic=TOPICS.raw, value=item, key=None)
            logger.info(f"Producing to {TOPICS.raw}")
    finally:
        producer.flush()

def main():
    try:
        asyncio.run(produce(producerObj))
    except KeyboardInterrupt:
        print("Interrupt")


if __name__ == "__main__":
    main()
