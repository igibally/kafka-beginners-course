package wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    private KafkaProducer<String, String> kafkaProducer;
    private String kafkaTopic;

    private static final Logger logger = LoggerFactory.getLogger(WikimediaChangeHandler.class.getName());

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String kafkaTopic) {
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
    }

    @Override
    public void onOpen() throws Exception {
        // noting here
    }

    @Override
    public void onClosed() throws Exception {
        kafkaProducer.close();
    }
    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        logger.info(messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(kafkaTopic,messageEvent.getData()));
    }
    @Override
    public void onComment(String comment) throws Exception {
        /// noting here
    }
    @Override
    public void onError(Throwable t) {
        logger.error("Error in stream reading",t);

    }
}
