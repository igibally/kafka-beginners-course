package io.conductor.demos.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getName());
    public static void main(String[] args) {
        log.info("iam the producer !");

        //connect to localhost
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");


        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("test_topic","hello world-testTwo");
        producer.send(producerRecord);
        producer.flush();
        producer.close();


        // create producer properties
        // create producer
        // flush and close producer
    }
}
