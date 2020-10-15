package laba2;

import org.apache.hadoop.mapreduce.Partitioner;

public abstract class MyPartitioner extends Partitioner<KEY, VALUE> {
    public int getPartition(KEY key, VALUE value, int numPartitions) {
        return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}