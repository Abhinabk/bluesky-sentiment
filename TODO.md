# Create the Avro schema 
- added  the doc key to describe what each key means 
# Flatten the events generated form bluesky 
- the events are nested and doesn't fit the schema, need to flatten them and keep only 
  the important ones
- Now I have introduced a processing layer before topic is sent to the broker by default kafka has 
    At-Least-once delivery semantics so duplicates may occur (may need to think about Exactly-Once ot Almost once
  for my use case )   enabled idempotency to use Exactly-Once semantics 
# Create the Schema Registry 
- the producer encodes the key/values using th uuid generated form the Schema Registry
- the consumer/processor can decode the key/value by referring the uuid with corresponding schema
- missing modules need to be added [SchemaRegistry,FastAvro] 

# Create the kafka stream consumer 
- kstreams connects to the bootstrap server and uses the schema registry to get the schema 
- uses the avro deserializer to the read the stream of events 
- filters the stream to only posts mentioning specific tracked brands, and
performs sentiment analysis then transforms the survived posts enriched with 
sentiment to JSON and write them to another topic `posts.enriched`. 

will change `ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"` to latest
> In previous `post.raw` I was working with JSON after which without deleting the \
> topic I started to work with avro format which caused `Unknown magic byte!` error \
> in consumer side which could only fix form the producer side (deleting the topic and 
> re-building the messages) 

- With filter we cannot map GenericRecord to toLowerString as we loose info have to keep it
    generic Record for the sentiment analysis to work as we loose other metadata info

# Create the python Sentiment analysis
- Python consumer subscribes to posts.enriched the filtered posts gives each post a score and saves that to 
post.scored which can be pulled by clickhouse
> truncation=True tells the pipeline: if the text is too long, cut it off at 512 tokens and score what fits,\
> rather than erroring (should be fine as sentiment of a post is generally set early)
