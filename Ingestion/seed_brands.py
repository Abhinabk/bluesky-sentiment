from KafkaWrappers.AdminWrapper import KafkaAdmin
from KafkaWrappers.ProducerWrapper import KafkaProducer
from config import BOOTSTRAP_SERVERS, TOPICS
from confluent_kafka.serialization import StringSerializer

admin = KafkaAdmin(BOOTSTRAP_SERVERS)

#uses sigle partion multiple partition file for globalktable but probalem for ktable as brands
# would be sharaded across task adn task would only see the brads on its own partition
admin.create_topics([TOPICS.brands],cleanup_policy="compact",num_partitions=1)

brands = ["love", "sad", "good", "happy", "angry"]

producer= KafkaProducer(BOOTSTRAP_SERVERS,
                            enable_idempotence=True,
                            value_serializer=StringSerializer(),
                            key_serializer=StringSerializer())

# run once after first setup or after wiping volumes; do NOT run after live brand edit
# side effect if run during live will replace tombstone 
def init_brands():                          
    for items in brands:
        producer.produce_msg(
            topic=TOPICS.brands,key=items,value='true'
        )

    producer.flush()

if __name__ == "__main__":
    init_brands()

