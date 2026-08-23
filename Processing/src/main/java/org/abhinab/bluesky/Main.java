package org.abhinab.bluesky;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

public class Main {
    public static void main(String[] args) {

       // configure property
        Properties streamProps = new Properties();
        streamProps.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "bluesky-processing-v1");
        streamProps.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProps.setProperty(StreamsConfig.consumerPrefix(AUTO_OFFSET_RESET_CONFIG), "earliest");
        // streamProps.setProperty("schema.registry.url","http://localhost:8081");

        // TODO: All this config can be repalced with
        // StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG
        // build the avro serializer
        GenericAvroSerde valueSerde = new GenericAvroSerde();
        // configure
        valueSerde.configure(
                Map.of("schema.registry.url", "http://localhost:8081"),
                false);

        // build the stream
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, GenericRecord> stream = builder.stream("posts.raw",
                Consumed.with(Serdes.String(), valueSerde));
        // used for state store   
        GlobalKTable<String,String> globalKTable = builder.globalTable("brands.config",
            Consumed.with(Serdes.String(),Serdes.String()),
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("brands-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
        );
        stream.process(BrandFilterProcessor::new).to("posts.enriched",
            Produced.with(Serdes.String(),Serdes.String())
        );
        // filter returns a boolean so throws away the brand names
        /*
         * stream.filter(
         * (key, value) -> {
         * Object raw = value.get("text");
         * if(raw==null) return false;
         * String text = raw.toString().toLowerCase();
         * return brands.stream().anyMatch(brand->text.contains(brand));
         * })
         * .mapValues(value -> JsonMapper.recordToJson(value))
         * .to("posts.enriched", Produced.with(Serdes.String(),Serdes.String()));
         */
        /* stream.mapValues(record -> JsonMapper.recordToJson(record,pattern))
                .filter((key, value) -> value != null)
                .to("posts.enriched", Produced.with(Serdes.String(), Serdes.String())); */
        
        

        // create the kafak stream instance
        KafkaStreams kafakaStreams = new KafkaStreams(builder.build(), streamProps);

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafakaStreams.close();
            latch.countDown();
        }, "streams-shutdown-hook"));

        try {
            kafakaStreams.start();
            System.out.println("Starting kafka stream ...");
            latch.await();
        } catch (InterruptedException e) {
            System.out.println("Interrupted stream ...");
            Thread.currentThread().interrupt();
        }
    }

   
}


