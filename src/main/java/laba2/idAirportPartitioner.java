package laba2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.io.WritableComparable;

public abstract class idAirportPartitioner extends Partitioner<WritableComparable, Text> {
    @Override
    public int getPartition(WritableComparable key, Text value, int numPartitions) {
        KeyPair o = (KeyPair) key;
        return (o.getIdAirport().toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}