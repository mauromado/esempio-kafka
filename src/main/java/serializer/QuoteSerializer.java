package serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.Quote;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class QuoteSerializer implements Serializer<Quote> {//implementazione della classe Serializer di Kafka
    //nota: L'oggetto Quote conterrà i dati di nostro interesse, ovvero:
    //prezzo, volume, simbolo del titolo, timestamp
    private ObjectMapper objectMapper;

    public void configure(Map<String, ?> configs, boolean isKey) {
        //metodo che viene eseguito all'avvio e riceve le configurazioni. In più un flag che mi dice se l'oggetto da
        //serializzare è la chiave o il valore del record
        this.objectMapper = new ObjectMapper(); //ObjectMapper della libreria Jackson, che permette di trasformare oggetti Java in JSON.
    }

    public byte[] serialize(String topic, Quote quote) {
        try {
            return objectMapper.writeValueAsBytes(quote);
        } catch (Exception e) {
            throw new SerializationException("Error serializing object", e);
        }
    }

    public void close() {
    }
}

