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
        String idAirport = String[1:numSpitter];
        if (records.length > 2) {
            context.write(new KeyPair(new Text(records[0]), new Text("0")), new Text(records[1] + "," + records[2]));
        } else if (records.length == 2){
            context.write(new KeyPair(new Text(records[0]), new Text("0")), new Text(records[1]));
        }
    }
}