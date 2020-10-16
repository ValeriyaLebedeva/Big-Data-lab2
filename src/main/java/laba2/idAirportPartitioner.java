package laba2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.io.WritableComparable;

public class MyPartitioner extends Partitioner<WritableComparable, Text> {
    @Override
    public int getPartition(WritableComparable key, Text value, int numPartitions) {
        KeyPair o = (KeyPair) key;
        return (o.idAirport.toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}