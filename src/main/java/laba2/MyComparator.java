package laba2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyComparator extends WritableComparator {

    protected MyComparator() {
        super(KeyPair.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        KeyPair o1 = (KeyPair) a;
        KeyPair o2 = (KeyPair) b;
        return o1.idAirport.toString().compareTo(o2.idAirport.toString());
    }
    
}