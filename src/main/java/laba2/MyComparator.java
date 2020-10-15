package laba2;

import org.apache.hadoop.io.RawComparator;

public class MyComparator implements RawComparator {
    public int compare(TextPair a, TextPair b) {
        return a.compareTo(b);
    }}