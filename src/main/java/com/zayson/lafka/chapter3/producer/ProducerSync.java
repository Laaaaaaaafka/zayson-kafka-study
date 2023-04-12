package com.zayson.lafka.chapter3.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class ProducerSync {
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
                RecordMetadata recordMetadata = producer.send(record).get();  // get 메서드를 통해 카프카의 응답을 기다림 -> RecordMetadata 반환
                System.out.println("========");
                System.out.println("recordMetadata.topic() = " + recordMetadata.topic());
                System.out.println("recordMetadata.partition() = " + recordMetadata.partition());
                System.out.println("recordMetadata.offset() = " + recordMetadata.offset());
                System.out.println("record.key() = " + record.key());
                System.out.println("record.value() = " + record.value());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
