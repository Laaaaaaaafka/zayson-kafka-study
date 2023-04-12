package com.zayson.lafka.chapter3.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerSync {
    public static void main(String[] args) {
        Properties props = new Properties();

        // Kafka Broker 리스트 설정
        props.put("bootstrap.servers", "host.docker.internal:9092, host.docker.internal:9093, host.docker.internal:9094");

        props.put("group.id", "peter-consumer01");  // 컨슈머 그룹 아이디 지정
        props.put("enable.auto.commit", "false");  // Manual Commit 사용
        props.put("auto.offset.reset", "latest");  // 컨슈머 오프셋을 찾지 못하는 경우 latest로 초기화하며 가장 최근부터 메세지를 가져옴

        // 메세지 Deserialization
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            final String topic = "peter-basic01";
            consumer.subscribe(Arrays.asList(topic));  // 구독할 토픽을 지정

            // 무한 루프를 돌면서 지속적으로 메세지 컨슘
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000); // 1초마다 메세지 폴링
                for (ConsumerRecord<String, String> record : records) { // poll()은 레코드 전체를 리턴 -> 여러 개의 메세지를 가져옴
                    System.out.println("======");
                    System.out.println("record.topic() = " + record.topic());
                    System.out.println("record.partition() = " + record.partition());
                    System.out.println("record.offset() = " + record.offset());
                    System.out.println("record.key() = " + record.key());
                    System.out.println("record.value() = " + record.value());
                }

                consumer.commitSync();  // 현재 배치를 통해 메세지를 모두 컨슘 -> 추가 메세지 폴링 전 오프셋을 동기 커밋
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
