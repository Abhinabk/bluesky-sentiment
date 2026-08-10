# Create the Avro schema 
- added  the doc key to describe what each key means 
# Flatten the events generated form bluesky 
- the events are nested and doesn't fit the schema, need to flatten them and keep only 
  the important ones
- Now i have introduced a processing layer before topic is sent to the broker by defualt kafka has 
    At-Least_once delivery semantics so duplicates may occur (may need to think about Exactly-Once ot Atmost once
  for my use case )   enabled idempotency to use Exactly-Once semantics 
# Create the Schema Registry 
- the producer encodes the key/values using th uuid generated form the Schema Registry
- the consumer/processor can decode the key/value by referring the uuid with corresponding schema
- missing modules need to be added [SchemaRegistry,FastAvro] 
