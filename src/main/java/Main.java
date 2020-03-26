import consumer.Consumer;
import model.Downloader;
import model.Quote;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import producer.Producer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    private final Logger logger = LoggerFactory.getLogger(Main.class);

    public void main(String[] args) {
        final int instanceNumber = Integer.valueOf(args[0]);
        final List<Quote> quotes = new ArrayList<>();

        Quote q1 = new Quote(1000, 3, "Q1");
        Quote q2 = new Quote(2000, 2, "Q2");
        Quote q3 = new Quote(3000, 1, "Q3");

        quotes.add(q1);
        quotes.add(q2);
        quotes.add(q3);

        Consumer consumer = new Consumer("Stocks", instanceNumber);
        List<String> symbols = new ArrayList<>();
        while (symbols.isEmpty()) {
            ConsumerRecords<String, String> records = consumer.receive();
            for (ConsumerRecord<String, String> record : records) {
                String symbol = record.value();
                this.logger.debug("Adding symbol {}", symbol);
                symbols.add(symbol);
            }
        }
        consumer.close();
        for (int i=0; i <= symbols.size(); i++) {
            Downloader downloader = new Downloader(symbols.get(i), quotes);
            Producer producer = new Producer("Quotes", instanceNumber);
            Map<String, Quote> oldQuotesMap = new HashMap<>();
            while (true) {
                for (String symbol : symbols) {
                    Quote newQuote = downloader.download(symbol);
                    Quote oldQuote = oldQuotesMap.get(symbol);
                    if (newQuote != null && !newQuote.equals(oldQuote)) {
                        boolean success = producer.send(symbol, newQuote);
                        if (success) {
                            oldQuotesMap.put(symbol, newQuote);
                        }
                    }
                }
            }
        }
    }
}
