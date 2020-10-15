package laba2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public abstract class MyPartitioner extends Partitioner<KeyPair, Text> {
    @Override
    public int getPartition(KeyPair key, Text value, int numPartitions) {
        return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}