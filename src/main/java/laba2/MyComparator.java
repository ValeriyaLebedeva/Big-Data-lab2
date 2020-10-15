package laba2;

import org.apache.hadoop.io.RawComparator;

public class MyComparator implements RawComparator<KeyPair> {

    public int compare(KeyPair a, KeyPair b) {
        return a.compareTo(b);
    }
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return 0;
    }
}