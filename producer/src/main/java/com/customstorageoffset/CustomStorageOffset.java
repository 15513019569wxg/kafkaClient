package com.customstorageoffset;/*
    @author wxg
    @date 2021/7/30-12:26
    */

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @author capensis
 */
public class CustomStorageOffset {
    public static Map<TopicPartition, Long> currentOffset = new HashMap<>();

    /**
     * 无论是同步提交还是异步提交offset，都有可能会造成数据的漏消费或者重复消费。先提交offset后消费，有可能造成数据的漏消费；
     * 而先消费后提交 offset，有可能会造成数据的重复消费。
     * @param args 数组
     */
    public static void main(String[] args) {
        /*
        Kafka 0.9 版本之前， offset 存储在 zookeeper， 0.9 版本及之后，默认将 offset 存储在 Kafka的一个内置的 topic 中。
        除此之外， Kafka 还可以选择自定义存储 offset。offset 的维护是相当繁琐的， 因为需要考虑到消费者的 Rebalace。

        当有新的消费者加入消费者组、 已有的消费者推出消费者组或者所订阅的主题的分区发生变化，就会触发到分区的重新分配，重新分配的过程叫做Rebalance。
        消费者发生 Rebalance 之后，每个消费者消费的分区就会发生变化。 因此消费者要首先获取到自己被重新分配到的分区，并且定位到每个分区最近提交的 offset 位置继续消费。
        要实现自定义存储 offset，需要借助 ConsumerRebalanceListener， 以下为示例代码，其中提交和获取 offset 的方法，需要根据所选的 offset 存储系统自行实现。
         */
        //创建配置信息
        Properties props = new Properties();
        //Kafka 集群
        props.put("bootstrap.servers", "hadoop102:9092");
        //消费者组，只要 group.id 相同，就属于同一个消费者组
        props.put("group.id", "test");
        //关闭自动提交 offset
        props.put("enable.auto.commit", "false");
        //Key 和 Value 的反序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //创建一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //消费者订阅主题
        consumer.subscribe(Collections.singletonList("first"), new ConsumerRebalanceListener() {
                    //该方法会在 Rebalance 之前调用
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        commitOffset(currentOffset);
                    }
                    //该方法会在 Rebalance 之后调用
                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        currentOffset.clear();
                        for (TopicPartition partition : partitions) {
                            //定位到最近提交的 offset 位置继续消费
                            consumer.seek(partition, getOffset(partition));
                        }
                    }
        });
        while (true) {
            //消费者拉取数据
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                currentOffset.put(new TopicPartition(record.topic(), record.partition()), record.offset());
            }
            //异步提交
            commitOffset(currentOffset);
        }
    }
    /**
     *  获取某分区的最新offset
     */
    private static long getOffset(TopicPartition partition) {
        return 0;
    }

    /**
     *  提交该消费者所有分区的 offset
     */
    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {
    }
}
