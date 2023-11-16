package com.maguasoft.example.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

public class Consumer {
    public static void main(String[] args) {
        Map<String, Object> configs = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.27.153.46:10086,172.27.153.46:10087,172.27.153.46:10088",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(configs);
    }
}
