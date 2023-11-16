package com.maguasoft.example.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Objects;

@Slf4j
public class Producer {

    public static void main(String[] args) throws InterruptedException {
        Map<String, Object> configs = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.27.153.46:10086,172.27.153.46:10087,172.27.153.46:10088",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(configs)) {
            producer.send(new ProducerRecord<>("test", "Hello Kafka"),
                    (metadata, exception) -> {
                        if (Objects.isNull(exception)) {
                            log.info("Send message successful");
                        } else {
                            log.info("Send message error, Cause by: ", exception);
                        }
                    });
        } catch (Exception e) {
            log.error("Producer error. Cause by: ", e);
        }

        Thread.sleep(3000);
    }
}
