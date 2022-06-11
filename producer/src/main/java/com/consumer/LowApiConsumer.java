package com.consumer;/*
    @author wxg
    @date 2021/11/26-1:12
    */

import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

/**
 * @author capensis
 */
public class LowApiConsumer {
    public static void main(String[] args) {
        BrokerEndPoint leader = null;
        String topic = "movie";
        int partion = 0;
        // 1、获取分区的leader
        SimpleConsumer simpleConsumer = new SimpleConsumer("hadoop102", 9092, 500, 10 * 1024, "metadata");
        // 2、获取元数据信息
        TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
        TopicMetadataResponse response = simpleConsumer.send(topicMetadataRequest);
        //  3、获得分区的leader所在的主机
        leaderLabel: for (TopicMetadata topicMetadatum : response.topicsMetadata()) {
            if(topic.equals(topicMetadatum.topic())){
                //  4、movies主题的元数据信息
                for (PartitionMetadata partitionsMetadatum : topicMetadatum.partitionsMetadata()) {
                    int partId = partitionsMetadatum.partitionId();
                    System.out.println(partId);
                    if(partId == partion){
                        leader = partitionsMetadatum.leader();
                        System.out.println(leader);
                        break leaderLabel;
                    }
                }
            }
        }

        if(leader == null ) {
            System.out.println("分区信息不正确");
            return;
        }

        // 5、host/port应该是指定分区的leader
        SimpleConsumer consumer = new SimpleConsumer(leader.host(), leader.port(), 5000, 10 * 1024, "accessLeader");
        // 6、抓取数据
        final kafka.api.FetchRequest req = new FetchRequestBuilder().addFetch(topic, partion, 0, 10 * 1024).build();
        final FetchResponse resp = consumer.fetch(req);
        // 7、获得消息
        ByteBufferMessageSet messageSet = resp.messageSet(topic, partion);
        for (MessageAndOffset messageAndOffset : messageSet) {
            ByteBuffer buffer = messageAndOffset.message().payload();
            byte[] bytes = new byte[buffer.limit()];
            buffer.get(bytes);
            String value;
            value = new String(bytes, StandardCharsets.UTF_8);
            System.out.println(value);
        }
    }
}
