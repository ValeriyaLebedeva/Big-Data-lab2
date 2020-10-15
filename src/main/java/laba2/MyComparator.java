package laba2;

import org.apache.hadoop.io.RawComparator;

public class MyComparator implements RawComparator {
...
    public int compare(WritableComparable a, WritableComparable b) {
        return a.compareTo(b);
    }}