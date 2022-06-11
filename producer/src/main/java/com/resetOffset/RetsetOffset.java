package com.resetOffset;/*
    @author wxg
    @date 2021/6/27-20:14
    */

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 需要用到的类：
 * KafkaConsumer： 需要创建一个消费者对象，用来消费数据
 * ConsumerConfig： 获取所需的一系列配置参数
 * ConsumerRecord： 每条数据都要封装成一个 ConsumerRecord 对象
 * 为了使我们能够专注于自己的业务逻辑， Kafka 提供了自动提交 offset 的功能。
 * 自动提交 offset 的相关参数：
 *      enable.auto.commit： 是否开启自动提交 offset 功能
 *      auto.commit.interval.ms： 自动提交 offset 的时间间隔
 * ps： offset存在于两个位置，分别在内存和__consumer-offsets文件中，文件中的offsets在消费者初次开始消费时读取，之后在消费者消费过程中不再读取，只会
 *      读取内存中实时更新的offset值，如果此时自动提交功能设置为false，那么内存中更改的offset就不会每隔1秒去更新（提交）文件中的offset。
 * @author capensis
 */
public class RetsetOffset {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");
        //  设置自动提交offset
        props.put("enable.auto.commit", "false");
        //  设置自动提交延时时间
        props.put("auto.commit.interval.ms", "1000");
        //  反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //  组ID
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test5");
        //  重置消费者的offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //  创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //  订阅主题
        consumer.subscribe(Arrays.asList("artic","news"));
        while (true) {
            //  获取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            //  解析并打印consumerRecords
            for (ConsumerRecord<String, String> consumerRecord :consumerRecords) {
                System.out.println(consumerRecord.key() + "--" + consumerRecord.value());
            }
        }
        //  关闭连接
//        consumer.close();
    }
}
