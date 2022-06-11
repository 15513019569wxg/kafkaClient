package com.partition;/*
    @author wxg
    @date 2021/6/27-16:19
    */


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author capensis
 */
public class DefPartition implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
// Integer integer = clust er.partitionCountForTopic(topic);
// return key.toString().hashCode() % integer;
        return 1;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
