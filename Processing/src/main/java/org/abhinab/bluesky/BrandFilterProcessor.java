package org.abhinab.bluesky;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class BrandFilterProcessor implements Processor<String, GenericRecord, String, String> {
    private KeyValueStore<String, String> store;
    private ProcessorContext<String, String> context;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String recordToJson(Record<String, GenericRecord> record,String brand){

        ObjectNode node = MAPPER.createObjectNode();
        node.put("brand", brand);
        node.put("text", record.value().get("text").toString());
        node.put("did", record.value().get("did").toString());
        node.put("rkey", record.value().get("rkey").toString());
        node.put("createdAt", record.value().get("createdAt").toString());
        node.put("time_us", (Long) record.value().get("time_us"));
        return node.toString();
    }

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
        this.store = context.getStateStore("brands-store");
    }

    @Override
    public void process(Record<String, GenericRecord> record) {

        var text = record.value().get("text").toString();
        try (var it = store.all()) {
            while (it.hasNext()) {
                var brand = it.next().key;
                String regex = "\\b" + Pattern.quote(brand) + "\\b";
                Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(text);
                if (matcher.find()) {
                    var json= recordToJson(record,brand);
                    context.forward(new Record<>(record.key(),json,record.timestamp()));
                    System.out.println("found brand: "+ brand);
                    break; //first match wins
                }
            }
        }
    }

}
