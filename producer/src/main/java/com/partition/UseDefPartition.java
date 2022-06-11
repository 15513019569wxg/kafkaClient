package com.partition;/*
    @author wxg
    @date 2021/6/27-19:01
    */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

/**
 * @author capensis
 */
public class UseDefPartition {
    public static void main(String[] args) {
        //  kafka配置文件对象
        Properties props = new Properties();
        //  kafka集群, broker-list
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        //  ack应答
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //  重试次数
        props.put("retries", 3);
        //  批量大小 16K
        props.put("batch.size", 16384);
        //  等待时间
        props.put("linger.ms", 1);
        //  RecordAccumulator 缓冲区大小  32M
        props.put("buffer.memory", 33554432);
        //  K-V 序列化
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //  添加自定义的分区器
        props.put("partitioner.class", "com.partition.DefPartition");

        //  创建kafka生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            //回调函数， 该方法会在 Producer 收到 ack 时调用，为异步调用
            //  new ProducerRecord()有很多的重载方法
            producer.send(new ProducerRecord<>("news", Integer.toString(i), Integer.toString(i)), (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("success->" + metadata.partition() + "--" + metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        }
        //  关闭资源
        producer.close();
    }
}
