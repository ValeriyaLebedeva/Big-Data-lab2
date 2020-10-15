package laba2;

import org.apache.hadoop.io.RawComparator;

public class MyComparator implements RawComparator<TextPair> {

    public int compare(TextPair a, TextPair b) {
        return a.compareTo(b);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return 0;
    }
}