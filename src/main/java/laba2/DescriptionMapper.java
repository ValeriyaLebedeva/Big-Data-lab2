package laba2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DescriptionMapper extends Mapper<LongWritable, Text, KeyPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] records = value.toString().split(",");
        if (records.length > 0) {
            Text idAirport = new Text(records[0].replace("\"", ""));
            KeyPair keyPair = new KeyPair(idAirport,new Text("0"));
            context.write(keyPair,
                    new Text(records[1]));
        }
    }
}