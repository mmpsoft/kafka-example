package com.maguasoft.example.kafka.partitioner;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class PartitionProducer {

    public static void main(String[] args) throws InterruptedException {
        Map<String, Object> configs = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.27.153.46:10086,172.27.153.46:10087,172.27.153.46:10088",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                // 配置自定义分区器
                ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(configs)) {
            // topic 应将记录发送到的主题
            // partition 应将记录发送到的分区
            // timestamp 记录的时间戳，以纪元以来的毫秒为单位。如果为 null，则生产者将使用 System.currentTimeMillis（） 分配时间戳。
            // K key 要分区的key（如果没有键，则为 null）
            // V value 记录内容
            // headers 将包含在记录中的头

            // 将数据到达 test 主题的分区 1 中
            producer.send(new ProducerRecord<>("test", 1, System.currentTimeMillis(), "", "Hello Partition", List.of()));
        } catch (Exception e) {
            log.error("Producer error. Cause by: ", e);
        }

        Thread.sleep(3000);
    }
}
