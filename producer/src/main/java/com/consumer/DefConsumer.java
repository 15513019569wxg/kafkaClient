package com.consumer;/*
    @author wxg
    @date 2021/6/27-19:28
    */


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author capensis
 */
public class DefConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");
        //  设置自动提交offset
        props.put("enable.auto.commit", "true");
        //  设置自动提交延时时间
        props.put("auto.commit.interval.ms", "1000");
        //  反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //  组ID
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        //  创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //  订阅主题（可以订阅多个主题）
        consumer.subscribe(Arrays.asList("movies", "news"));
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + "--" + consumerRecord.value());
                //System.out.println(consumerRecord);
            }
        }
    }
        //  关闭连接
//        consumer.close();
    
}
