package com.sakura.spark.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * kafka 消费者
 */
public class KafkaConsumer extends Thread {

    private String topic;

    public KafkaConsumer(String topic) {
        this.topic = topic;
    }

    private ConsumerConnector createConnector() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", KafkaPreperties.ZK);
        properties.put("group.id", KafkaPreperties.GROUP_ID);
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    @Override
    public void run() {
        ConsumerConnector connector = createConnector();

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = connector.createMessageStreams(topicCountMap);

        KafkaStream<byte[], byte[]> messageAndMetadata = messageStreams.get(topic).get(0);

        ConsumerIterator<byte[], byte[]> iterator = messageAndMetadata.iterator();
        while (iterator.hasNext()) {
            System.out.println("rec: " + new String(iterator.next().message()));
        }
    }
}
