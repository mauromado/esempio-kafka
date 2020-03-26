package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

//Wrapper di KafkaConsumer -> costruttore + metodo receive(...)
public class Consumer {
    private final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private KafkaConsumer<String, String> consumer;
    private String topic;

    public Consumer(String topic, int instanceNumber) {
        this.topic = topic;
        Properties properties = new Properties();
        /**
         * Proprietà specifiche di Kafka:
         *
         * bootstrap.servers, valore "localhost:9092" -> stringa contenente la lista degli indirizzi di tutti i broker Kafka
         * a cui vogliamo connetterci;
         *
         * group.id, valore "QuotesDownloader" -> identificativo del gruppo di consumatori a cui appartiene il servizio (fisso
         * in questo caso, ovvero tutti devono appartenere allo stesso gruppo);
         *
         * client.id, valore "QuotesDownloaderN" -> identificativo del consumatore univoco. Ipotizziamo di concatenare N, ossia
         * il numero dell'istanza, a una stringa fissa;
         *
         * auto.commit.enable, valore "true" -> il consumatore comunica periodicamente e autonomamente a Kafka l'offset raggiunto
         *
         * key.deserializer, valore "StringDeserializer.class" -> classe utilizzata per deserializzare la chiave del record
         * ricevuto (in questo caso viene utilizzata una classe standard messa a disposizione dalla libreria per gestire le stringhe)
         *
         * value.deserializer, valore "StringDeserializer.class" -> classe utilizzata per deserializzare il valore del record
         * ricevuto (in questo caso viene utilizzata una classe standard messa a disposizione dalla libreria per gestire le stringhe)
         *
         */
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS_CONFIG);
        properties.put("group.id", "QuotesDownloader");
        properties.put("client.id", "QuotesDownloader" + instanceNumber);
        properties.put("auto.commit.enable", "true");
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", StringDeserializer.class);
        this.consumer = new KafkaConsumer<>(properties);
        this.consumer.subscribe(Arrays.asList(this.topic));
    }

    public ConsumerRecords<String, String> receive() { //fa solo una cosa: chiama il metodo poll(Duration d); della libreria di Kafka
        ConsumerRecords<String, String> records = null;
        try {
            this.logger.debug("Ricezione del record...");
            records = this.consumer.poll(Duration.ofSeconds(1));
            this.logger.debug("Ricevuto il record {}", records.count());
        } catch (Exception e) {
            this.logger.error("Eccezione sulla ricezione:", e);
        }
        return records;
    }

    public void close() {
        this.consumer.close();
    }
}
