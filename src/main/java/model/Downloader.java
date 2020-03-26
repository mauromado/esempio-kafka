package model;

import java.util.List;

public class Downloader {

    private String symbol;
    private List<Quote> quotes;

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public List<Quote> getQuotes() {
        return quotes;
    }

    public void setQuotes(List<Quote> quotes) {
        this.quotes = quotes;
    }

    public Downloader(String symbol, List<Quote> quotes) {
        this.symbol = symbol;
        this.quotes = quotes;
    }

    public Quote download(String symbol) {
        for(int i=0; i<=quotes.size(); i++) {
            if(quotes.get(i).getSimbolo().equals(symbol)) {
                return quotes.get(i);
            }
        }
        return null;
    }
}
