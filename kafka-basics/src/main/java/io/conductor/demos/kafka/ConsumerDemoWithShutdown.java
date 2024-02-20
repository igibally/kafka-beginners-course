package io.conductor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getName());

    public static void main(String[] args) {


        log.info("iam the producer with call back !");
        String group_id = "my-java-application";

        String topic_name = "javaApp_topic";

        //connect to localhost
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", group_id);


        // set producer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("auto.offset.reset", "earliest"); // to make the consumer read from the beginning of the topic
        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic_name));

        final Thread mainThread = Thread.currentThread();


        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info(" detected shutdown , let's wait by calling consumer.wakeup()...");
                consumer.wakeup();
                try {
                    mainThread.join(); // this thread will  be waiting unitl the main thread wich is catching the exception is finishes
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }

        } catch (WakeupException exception) {
             log.info("Consumer starting to shutDown ");
        }
        finally {
            consumer.close();
            log.info("Consumer is gracefully shutdown");
        }


    }
}
