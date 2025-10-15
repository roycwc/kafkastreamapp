package com.testagg;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AppTest {
    private Serde<String> stringSerde = new Serdes.StringSerde();

    @Test
    @DisplayName("IO topic test")
    public void shouldInputOutputWorks() {

        var topology = new Topology();
        topology.addSource("source01", "input-topic");
        topology.addSink("sink01", "result-topic", "source01");

        var props = new Properties();
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        var testDriver = new TopologyTestDriver(topology, props);

        var inputTopic = testDriver.createInputTopic(
                "input-topic",
                stringSerde.serializer(),
                stringSerde.serializer());
        var outputTopic = testDriver.createOutputTopic(
                "result-topic",
                stringSerde.deserializer(),
                stringSerde.deserializer());

        inputTopic.pipeInput("123", "hello");
        var out = outputTopic.readKeyValue();

        assertEquals("123", out.key);
        assertEquals("hello", out.value);

        testDriver.close();
    }

    @Test
    @DisplayName("Read JSON and check transaction_amount")
    public void shouldReadJsonAndCheckTransactionAmount() throws IOException {
        // Read the JSON file as string
        var path = "src/test/resources/expenses_enriched/samples/tx-20241223-123455.json";
        var jsonStr = Files.readString(Paths.get(path));

        // Parse member_id from JSON
        var mapper = new ObjectMapper();
        var node = mapper.readTree(jsonStr);
        var memberId = node.get("member_id").asText();
        var transactionAmount = node.get("transaction_amount").asInt();

        var topology = new Topology();
        topology.addSource("source01", "input-topic");
        topology.addSink("sink01", "result-topic", "source01");

        var props = new Properties();
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        var testDriver = new TopologyTestDriver(topology, props);

        var inputTopic = testDriver.createInputTopic(
                "input-topic",
                stringSerde.serializer(),
                stringSerde.serializer());
        var outputTopic = testDriver.createOutputTopic(
                "result-topic",
                stringSerde.deserializer(),
                stringSerde.deserializer());

        // Put the JSON string into input topic with member_id as key
        inputTopic.pipeInput(memberId, jsonStr);

        // Read output and check transaction_amount is present in the message
        var out = outputTopic.readKeyValue();
        assertEquals(memberId, out.key);
        var outNode = mapper.readTree(out.value);
        assertEquals(transactionAmount, outNode.get("transaction_amount").asInt());

        testDriver.close();
    }

    @Test
    @DisplayName("Transform adds 1000 to transaction_amount")
    public void shouldAddThousandThroughTransform() throws IOException {
        var path = "src/test/resources/expenses_enriched/samples/tx-20241223-123455.json";
        var originalJson = Files.readString(Paths.get(path));

        var mapper = new ObjectMapper();
        var node = mapper.readTree(originalJson);
        var memberId = node.get("member_id").asText();
        var expectedAmount = node.get("transaction_amount").asInt() + 1000;

        var builder = new StreamsBuilder();
        builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(value -> {
                    try {
                        var eventNode = (ObjectNode) mapper.readTree(value);
                        var current = eventNode.get("transaction_amount").asInt();
                        eventNode.put("transaction_amount", current + 1000);
                        return mapper.writeValueAsString(eventNode);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .to("result-topic", Produced.with(Serdes.String(), Serdes.String()));

        var topology = builder.build();

        var props = new Properties();
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        try (var testDriver = new TopologyTestDriver(topology, props)) {
            var inputTopic = testDriver.createInputTopic(
                    "input-topic",
                    stringSerde.serializer(),
                    stringSerde.serializer());
            var outputTopic = testDriver.createOutputTopic(
                    "result-topic",
                    stringSerde.deserializer(),
                    stringSerde.deserializer());

            inputTopic.pipeInput(memberId, originalJson);

            var out = outputTopic.readKeyValue();
            assertEquals(memberId, out.key);
            var transformedNode = mapper.readTree(out.value);
            assertEquals(expectedAmount, transformedNode.get("transaction_amount").asInt());
        }
    }

}
