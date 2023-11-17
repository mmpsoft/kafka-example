package com.maguasoft.example.kafka;

import com.maguasoft.example.kafka.partitioner.MyPartitioner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Objects;

@Slf4j
public class SimpleProducer {

    public static void main(String[] args) throws InterruptedException {
        Map<String, Object> configs = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.27.153.46:10086,172.27.153.46:10087,172.27.153.46:10088",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                // 配置自定义分区器，不指定时，使用默认分区器org.apache.kafka.clients.producer.internals.DefaultPartitioner
                ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class.getName(),
                // 缓冲区大小，默认32m
                ProducerConfig.BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024,
                // 每次批次数据大小，默认16kb
                ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024,
                // 数据大小不足批次大小时，等待多久发送数据，默认0ms，即立刻发送
                ProducerConfig.LINGER_MS_CONFIG, 10,
                // 对数据压缩
                ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy",
                // ack = -1(all)时，生产者发送过来的数据，Leader 和 ISR 队列的里的所有节点收到数据后应答。
                // -1和all等价。
                ProducerConfig.ACKS_CONFIG, "all",
                // 重试的最大值，默认 Integer.MAX_VALUE
                ProducerConfig.RETRIES_CONFIG, 3
        );

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(configs)) {
            // 异步发送
            producer.send(new ProducerRecord<>("test", "Hello Kafka1"));

            // 异步回调发送
            producer.send(new ProducerRecord<>("test", "Hello Kafka2"),
                    (metadata, exception) -> {
                        if (Objects.isNull(exception)) {
                            log.info("Send message successful");
                        } else {
                            log.info("Send message error, Cause by: ", exception);
                        }
                    });

            // 同步发送
            RecordMetadata recordMetadata = producer.send(new ProducerRecord<>("test", "Hello Kafka3")).get();
            log.info("SimpleProducer Synchronized send, recordMetadata: {}", recordMetadata);
        } catch (Exception e) {
            log.error("Producer error. Cause by: ", e);
        }

        Thread.sleep(3000);
    }
}
