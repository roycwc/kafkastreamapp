package com.testagg;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit test for simple App.
 */
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

}
