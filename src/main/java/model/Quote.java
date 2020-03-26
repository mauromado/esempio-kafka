package model;

import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Date;

public class Quote {

    private double prezzo;
    private int volume;
    private String simbolo;

    public Quote(double prezzo, int volume, String simbolo) {
        this.prezzo = prezzo;
        this.volume = volume;
        this.simbolo = simbolo;
    }

    public double getPrezzo() {
        return prezzo;
    }

    public void setPrezzo(double prezzo) {
        this.prezzo = prezzo;
    }

    public int getVolume() {
        return volume;
    }

    public void setVolume(int volume) {
        this.volume = volume;
    }

    public String getSimbolo() {
        return simbolo;
    }

    public void setSimbolo(String simbolo) {
        this.simbolo = simbolo;
    }
}
