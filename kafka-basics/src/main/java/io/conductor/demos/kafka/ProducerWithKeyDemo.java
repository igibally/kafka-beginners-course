package io.conductor.demos.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeyDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithKeyDemo.class.getName());
    public static void main(String[] args) {
        log.info("iam the producer with call back !");

        //connect to localhost
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");


        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        for (int j=0; j<2;j ++){
            for(int i=1; i<10;i++){

                produceToTopic(producer,i);

            }
            try {
                Thread.sleep(100000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }

        producer.flush();
        producer.close();
        // create producer properties
        // create producer
        // flush and close producer
    }

    private static void produceToTopic(KafkaProducer<String, String> producer,int increment) {
        String topic= "javaApp_topic";
        String key ="key_"+increment;
        String value= "hello world-test-new :: "+increment;
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic,key,value);
        producer.send(producerRecord,(metadata, exception) -> {
                if(exception == null){
                    log.info("Received new metadata\n" +
                            "Key: " +key+ "\n"+
                            "Partition:"+metadata.partition()+ "\n"+
                            "Received new metadata");
                }
                else{
                    log.info("exception message",exception.getMessage());
                }
        });
    }
}
