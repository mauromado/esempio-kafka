package producer;

import model.Quote;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import serializer.QuoteSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

//Wrapper di KafkaProducer -> costruttore + metodo send(...)
public class Producer {
    private final Logger logger = LoggerFactory.getLogger(Producer.class);
    private KafkaProducer<String, String> producer;
    private String topic;

    public Producer(String topic, int instanceNumber) {
        this.topic = topic;
        Properties properties = new Properties();
        /**
         *
         * Valgono le stesse considerazioni fatte per il Consumer. Tuttavia, vengono omesse due properties,
         * le quali sono specifiche per il consumatore:
         *
         * auto.commit.enable
         * group.id
         *
         * Inoltre, la derializzazione di un record viene fatta usando una classe ad HOC
         *
         */
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS_CONFIG);
        properties.put("client.id", "QuotesDownloader" + instanceNumber);
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", QuoteSerializer.class);
        this.producer = new KafkaProducer<>(properties);
    }

    public boolean send(String key, Quote value) {// chiave e valore del record e restituzione di true/false in base all'esito
        try {
            this.logger.debug("Invio del recdord...");
            this.producer.send(new ProducerRecord(this.topic, key, value)).get();
            this.logger.debug("Record inviato");
            return true;
        } catch (Exception e) {
            this.logger.error("Eccezione sull'invio:", e);
            return false;
        }
    }
}
