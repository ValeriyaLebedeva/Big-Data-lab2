package laba2;

import org.apache.hadoop.mapreduce.Partitioner;

public abstract class MyPartitioner<KEY, VALUE>  extends Partitioner {
    public int getPartition(KEY key, VALUE value, int numPartitions) {
        return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}