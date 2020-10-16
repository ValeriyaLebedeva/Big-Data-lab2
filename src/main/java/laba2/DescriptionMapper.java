package laba2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DescriptionMapper extends Mapper<LongWritable, Text, KeyPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String str = value.toString();
        int numSpitter = str.indexOf(",");
        String idAirport = str.substring(0, numSpitter);
        String description = str.substring(numSpitter+1);
        context.write(new KeyPair(new Text(idAirport), new Text("0")), new Text(description));
    }
}