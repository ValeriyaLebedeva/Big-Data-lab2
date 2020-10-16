package laba2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyComparator extends WritableComparator {
    protected MyComparator() {
        super(KeyPair.class, true);
    }


    public int compare(WritableComparable w1, WritableComparable w2) {
        KeyPair o1 = (KeyPair) w1;
        KeyPair o2 = (KeyPair) w2;
        return o1.idAirport.toString().compareTo(o2.idAirport.toString());
    }
}