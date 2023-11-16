package com.maguasoft.example.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyPartitioner implements Partitioner {

    /**
     *
     * @param topic The topic name
     * @param key 要分区的键（如果没有键，则为 null）
     * @param keyBytes 要分区的序列化键（如果没有键，则为 null）
     * @param value 要分区的值或 null
     * @param valueBytes 要分区的序列化值 on 或 null
     * @param cluster 当前集群元数据
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {


        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
