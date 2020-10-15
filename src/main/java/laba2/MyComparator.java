package laba2;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

public class MyComparator implements WritableComparator {

    public int compare(TextPair a, TextPair b) {
        return a.compareTo(b);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return 0;
    }
}