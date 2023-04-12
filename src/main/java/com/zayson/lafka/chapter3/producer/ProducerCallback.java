package com.zayson.lafka.chapter3.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

// Kafka Client의 Callback 인터페이스를 구현해 콜백 사용 가능
public class ProducerCallback implements Callback {
    private ProducerRecord<String, String> record;

    public ProducerCallback(ProducerRecord<String, String> record) {
        this.record = record;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            exception.printStackTrace();
        } else {
            System.out.println("========");
            System.out.println("recordMetadata.topic() = " + metadata.topic());
            System.out.println("recordMetadata.partition() = " + metadata.partition());
            System.out.println("recordMetadata.offset() = " + metadata.offset());
            System.out.println("record.key() = " + record.key());
            System.out.println("record.value() = " + record.value());
        }
    }
}
