package com.producer;/*
    @author wxg
    @date 2021/6/27-13:38
    */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

/**
 * 需要用到的类：
 * KafkaProducer：需要创建一个生产者对象，用来发送数据
 * ProducerConfig：获取所需的一系列配置参数
 * ProducerRecord：每条数据都要封装成一个 ProducerRecord 对象
 * @author capensis
 */

// 需要用到的类：
// KafkaProducer：需要创建一个生产者对象，用来发送数据
// ProducerConfig：获取所需的一系列配置参数
// ProducerRecord：每条数据都要封装成一个 ProducerRecord 对象

public class DefProducer {
    public static void main(String[] args) {
        //  kafka配置文件对象
        Properties props = new Properties();
        //  kafka集群, broker-list
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        //  ack应答
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //  重试次数
        props.put("retries", 3);
        //  批量大小   16K
        props.put("batch.size", 16384);
        //  等待时间
        props.put("linger.ms", 1);
        //  RecordAccumulator 缓冲区大小   32M
        props.put("buffer.memory", 33554432);
        //  K-V 序列化
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        //  创建kafka生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10000; i++) {
            //  new ProducerRecord()有很多的重载方法, 异步发送，get()一但调用就会变成同步
            producer.send(new ProducerRecord<>("movies","hello", "this is " + i + ""));
        }
        //  关闭资源
        producer.close();
    }
}
