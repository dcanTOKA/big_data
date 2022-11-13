package com.btk.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

public class Producer {
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "search";

        // kafka-topics.sh --create --zookeeper lcoalhost:2181 --replication-factor 1 partitions 1 --topic search
        // kafka-topics --list --bootstrap-server localhost:9092
        // kafka-console-consumer --bootstrap-server localhost:9092 --topic search

        Scanner read = new Scanner(System.in);

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // inf loop to send messages continuously via Scanner
        while (true){
            System.out.printf("Send data to Kafka : " );
            // create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, read.nextLine());
            producer.send(producerRecord);
        }


    }
}
