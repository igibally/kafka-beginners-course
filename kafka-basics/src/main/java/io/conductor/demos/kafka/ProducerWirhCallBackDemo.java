package io.conductor.demos.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWirhCallBackDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerWirhCallBackDemo.class.getName());
    public static void main(String[] args) {
        log.info("iam the producer with call back !");

        //connect to localhost
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");


        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        properties.setProperty("batch.size","30");
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        for(int i=1; i<60;i++){

            produceToTopic(producer,i);
        }
        producer.flush();
        producer.close();
        // create producer properties
        // create producer
        // flush and close producer
    }

    private static void produceToTopic(KafkaProducer<String, String> producer,int increment) {
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("test_topic","hello world-test"+increment);
        producer.send(producerRecord,(metadata, exception) -> {
                if(exception == null){
                    log.info("Received new metadata\n" +
                            "Topic: " +metadata.topic()+ "\n"+
                            "Partition:"+metadata.partition()+ "\n"+
                            "offset: " +metadata.offset()+ "\n"+
                            "TimeStamp: " +metadata.timestamp()+ "\n"+
                            "Received new metadata");
                }
                else{
                    log.info("exception message",exception.getMessage());
                }
        });
    }
}
