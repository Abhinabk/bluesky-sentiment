from confluent_kafka import Producer, KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField
import logging

# log config
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s-%(levelname)s-%(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


class KafkaProducer:
    def __init__(
        self,
        bootstrap_servers: str | list[str],
        *,
        client_id: str = "producer",
        acks: str = "all",
        enable_idempotence: bool = True,
        linger_ms: int = 10,
        batch_size: int = 64_000,
        compression_type: str = "snappy",
        key_serializer=None,
        value_serializer=None,
    ):
        if isinstance(bootstrap_servers, list):
            bootstrap_servers = ",".join(bootstrap_servers)

        self.config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "acks": acks,
            "enable.idempotence": enable_idempotence,
            "linger.ms": linger_ms,
            "batch.size": batch_size,
            "compression.type": compression_type,
        }

        self.producer = Producer(self.config)
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer
        self.msg_send = 0
        self.msg_failed = 0

        logger.info(f"Kafka producer initialized with brokers: {bootstrap_servers}")

    def delivery_report(self, err, msg):
        if err:
            logger.error(f"Delivery failed: {err}")
            self.msg_failed += 1
        else:
            logger.info(
                f"Record successfully delivered to topic={msg.topic()}\n"
                f"Partition={msg.partition()} offset={msg.offset()}",
            )

    def produce_msg(self, topic: str, value: object, key: object|None) -> None:
        record_key = key
        record_value = value

        try:
            if self.key_serializer and key is not None:
                # expects any that can be called with (value,ctx) value=inte, to encode and ctx=metadata
                record_key = self.key_serializer(
                    key, SerializationContext(topic, MessageField.KEY)
                )

            if self.value_serializer and value is not None:
                record_value = self.value_serializer(
                    value, SerializationContext(topic, MessageField.VALUE)
                )
            self.producer.produce(
                topic=topic,
                key=record_key,
                value=record_value,
                callback=self.delivery_report,
            )
            self.producer.poll(0)

        except BufferError:
            #TODO subtle bug here it polls once for (0.5s) then moves on what if buffer still not empty?

            logger.warning("Local buffer is full, poll briefly and retry.")
            self.producer.poll(0.5)
            self.producer.produce(
                topic=topic,
                key=record_key,
                value=record_value,
                callback=self.delivery_report,
            )
        except KafkaException as e:
            logger.error(f"Failed to produce message: {e}")

    def flush(self, timeout: float = 10.0):
        logger.info("Flushing pending messages before shutdown.")
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            logger.warning(f"{remaining} message(s) still pending after timeout")
        else:
            logger.info("All of the messages were successfully delivered.")
