package laba2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IdAirportComparator extends WritableComparator {
    protected IdAirportComparator() {
        super(KeyPair.class, true);
    }
    public int compare(WritableComparable w1, WritableComparable w2) {
        KeyPair o1 = (KeyPair) w1;
        KeyPair o2 = (KeyPair) w2;
        return o1.getIdAirport().toString().compareTo(o2.getIdAirport().toString());
    }
}