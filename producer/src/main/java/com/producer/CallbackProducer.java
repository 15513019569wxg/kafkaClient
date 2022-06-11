package com.producer;/*
    @author wxg
    @date 2021/6/27-15:52
    */


import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CallbackProducer {
    /**
     * 回调函数会在 producer 收到 ack 时调用，为异步调用， 该方法有两个参数，分别是
     * RecordMetadata 和 Exception，如果 Exception 为 null，说明消息发送成功，如果
     * Exception 不为 null，说明消息发送失败。
     * 注意：消息发送失败会自动重试，不需要我们在回调函数中手动重试。
     */
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");//kafka 集群， broker-list
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            //回调函数， 该方法会在 Producer 收到 ack 时调用，为异步调用
            //  new ProducerRecord()有很多的重载方法
            producer.send(new ProducerRecord<>("movie", Integer.toString(i), Integer.toString(i)), (metadata, exception) -> {
                if (exception == null) System.out.println("success->" + metadata.partition() + "--" + metadata.offset());
                else exception.printStackTrace();
            });
        }
        //  关闭资源
        producer.close();
    }
}
