package com.zayson.lafka.chapter3.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerAsync {
    public static void main(String[] args) {
        Properties props = new Properties();

        // Kafka Broker 리스트 설정
        props.put("bootstrap.servers", "host.docker.internal:9092, host.docker.internal:9093, host.docker.internal:9094");

        // 메세지 Key,Value Serializer 설정
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 프로듀서 객체 생성
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            final String topic = "peter-basic01";
            for (int loop = 0; loop < 3; loop++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "Apache Kafka is a distributes streaming platform - " + loop);

                // Record Send 시 Callback을 함께 전달
                producer.send(record, new ProducerCallback(record));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
