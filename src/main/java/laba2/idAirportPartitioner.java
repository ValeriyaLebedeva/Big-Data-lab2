package laba2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.io.WritableComparable;

public class idAirportPartitioner extends Partitioner<KeyPair, Text> {
    @Override
    public int getPartition(KeyPair key, Text value, int numPartitions) {
//        KeyPair o = (KeyPair) key;
        return (key.getIdAirport().toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}