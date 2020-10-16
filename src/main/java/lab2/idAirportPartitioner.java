package lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class idAirportPartitioner extends Partitioner<KeyPair, Text> {
    @Override
    public int getPartition(KeyPair key, Text value, int numPartitions) {
        return (key.getIdAirport().toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}