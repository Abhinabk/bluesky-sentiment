package org.abhinab.bluesky;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

public class Main {
    public static void main(String[] args) {

        List<String> brands= List.of("love", "sad", "good", "happy","angry");
        //configure property
        Properties streamProps = new Properties();
        streamProps.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "bluesky-processing-v1");
        streamProps.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProps.setProperty(StreamsConfig.consumerPrefix(AUTO_OFFSET_RESET_CONFIG), "earliest");
//        streamProps.setProperty("schema.registry.url","http://localhost:8081");

        // TODO: All this config can be repalced with StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG
        // build the avro serializer
        GenericAvroSerde valueSerde = new GenericAvroSerde();
        //configure
        valueSerde.configure(
                Map.of("schema.registry.url", "http://localhost:8081"),
                false
        );

        //build the stream
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, GenericRecord> stream = builder.stream("posts.raw",
                Consumed.with(Serdes.String(), valueSerde));
        stream.filter(
                (key, value) -> {
                    Object raw = value.get("text");
                    if(raw==null) return false;
                    String text = raw.toString().toLowerCase();
                    return  brands.stream().anyMatch(brand->text.contains(brand));
                })
                .mapValues(value -> JsonMapper.recordToJson(value))
                .to("posts.enriched", Produced.with(Serdes.String(),Serdes.String()));


        //create the kafak stream instance
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

class JsonMapper{
    private static final ObjectMapper mapper = new ObjectMapper();
    public  static String recordToJson(GenericRecord record){
        ObjectNode node = mapper.createObjectNode();
        node.put("text",record.get("text").toString());
        node.put("did",record.get("did").toString());
        node.put("rkey",record.get("rkey").toString());
        node.put("createdAt",record.get("createdAt").toString());
        node.put("time_us",(Long) record.get("time_us"));
        return  node.toString();
    }
}
