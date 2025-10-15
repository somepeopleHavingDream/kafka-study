package org.yangxin.kafka.kafkastudy.sample.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

@SuppressWarnings("unused")
public class SamplePartition implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        /*
            key-1
            key-2
            key-3
         */
        String keyStr = key + "";
        String keyInt = keyStr.substring(4);
        System.out.println(keyStr + ":" + keyInt);

        int i = Integer.parseInt(keyInt);
        return i % 2;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
