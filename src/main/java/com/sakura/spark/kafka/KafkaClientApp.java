package com.sakura.spark.kafka;

public class KafkaClientApp {

    public static void main(String[] args) {
        new KafkaProducer(KafkaPreperties.TOPIC).start();

        new KafkaConsumer(KafkaPreperties.TOPIC).start();
    }
}
