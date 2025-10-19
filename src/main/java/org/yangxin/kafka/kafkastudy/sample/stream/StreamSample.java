package org.yangxin.kafka.kafkastudy.sample.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

@SuppressWarnings({"unused", "resource"})
public class StreamSample {
    private static final String INPUT_TOPIC = "stream-in";
    private static final String OUTPUT_TOPIC = "stream-out";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-app");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 如何构建流结构拓扑
        StreamsBuilder builder = new StreamsBuilder();
        // 构建 word count processor
        wordCountStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    static void wordCountStream(StreamsBuilder builder) {
        KStream<String, String> source = builder.stream(INPUT_TOPIC);
        // Hello World mooc
        KTable<String, Long> count =
                source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
                        .groupBy((key, value) -> value)
                        .count();
        count.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }
}
