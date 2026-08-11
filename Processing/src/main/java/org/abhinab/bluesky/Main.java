package org.abhinab.bluesky;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

public class Main {
    public static void main(String[] args) {
        //configure property
        Properties streamProps = new Properties();
        streamProps.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"bluesky-processing-v1");
        streamProps.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        streamProps.setProperty(StreamsConfig.consumerPrefix(AUTO_OFFSET_RESET_CONFIG),"earliest");
//        streamProps.setProperty("schema.registry.url","http://localhost:8081");

        // TODO: All this config can be repalced with StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG
        // build the avro serializer
        GenericAvroSerde valueSerde = new GenericAvroSerde();
        //configure
        valueSerde.configure(
                Map.of("schema.registry.url","http://localhost:8081"),
                false
        );

        //build the stream
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, GenericRecord> stream = builder.stream("posts.raw",
                Consumed.with(Serdes.String(), valueSerde));
        stream.foreach((key,value)-> System.out.println(value));

        //create the kafak stream instance
        KafkaStreams kafakaStreams = new KafkaStreams(builder.build(),streamProps);
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            kafakaStreams.close();
            latch.countDown();
        }));
        kafakaStreams.start();
        try {
            System.out.println("Starting kafka stream ...");
            latch.await();
        } catch (InterruptedException e){
            System.out.println("Interrupted stream ...");
            Thread.currentThread().interrupt();
        }
    }
}
