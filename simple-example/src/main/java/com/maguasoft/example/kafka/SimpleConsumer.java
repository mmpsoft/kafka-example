package com.maguasoft.example.kafka;

import com.maguasoft.example.kafka.partitioner.MyPartitioner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
public class SimpleConsumer {
    public static void main(String[] args) {
        Map<String, Object> configs = Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.27.153.46:10086,172.27.153.46:10087,172.27.153.46:10088",
                // 消费者组名
                // 一个消费者组内有多个消费者组成，每个消费者之间是竞争关系。
                // 组与组之间是隔离关系
                ConsumerConfig.GROUP_ID_CONFIG, "TEST_GROUP",
                // Key 的反序列化
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                // Value 的反序列化
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                // 开启自动提交offset
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true,
                // 提交offset的时间间隔，单位 ms
                ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000
        );

        try (KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(configs)) {
            // 订阅 Topic，即指定需要消费那个Topic
            consumer.subscribe(List.of("test"));

//            while (true) {
                // 拉取，拉取到的是批量的数据
                ConsumerRecords<Object, Object> consumerRecords = consumer.poll(Duration.ofSeconds(5));
                for (ConsumerRecord<Object, Object> consumerRecord : consumerRecords) {
                    log.info("SimpleConsumer: Topic: {}, Partition: {}, Key: {}, Value: {}",
                            consumerRecord.topic(),
                            consumerRecord.partition(),
                            consumerRecord.key(),
                            consumerRecord.value());
                }
//            }

            Thread.sleep(3000);
        } catch (Exception e) {
            log.error("SimpleConsumer error, Cause by: ", e);
        }
    }
}
