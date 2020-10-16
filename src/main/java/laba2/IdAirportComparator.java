package laba2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IdAirportComparator extends WritableComparator {
    protected IdAirportComparator() {
        super(KeyPair.class, true);
    }
    public int compare(WritableComparator w1, KeyPair w2) {
//        KeyPair o1 = (KeyPair) w1;
//        KeyPair o2 = (KeyPair) w2;
        return w1.getIdAirport().toString().compareTo(w2.getIdAirport().toString());
    }
}