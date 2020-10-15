package laba2;
package org.apache.hadoop.mapreduce;

public abstract class Partitioner<KEY, VALUE> {
    public int getPartition(KEY key, VALUE value, int numPartitions) {
        return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}