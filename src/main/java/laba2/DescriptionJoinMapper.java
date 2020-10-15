package laba2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DescriptionJoinMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] records = value.toString().split(",");
        TextPair key_pair = new TextPair(records[0].replace("\"", ""),"0");
        context.write(key_pair,
                new Text(records[1]));
    }
}