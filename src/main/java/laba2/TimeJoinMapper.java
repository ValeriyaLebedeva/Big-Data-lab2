package laba2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TimeJoinMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] records = value.toString().split(",");
        String delay_time = records[18];
        String airport_id = records[14]
        String[] key_pair = {airport_id,"1"};
        TextPair key_pair = new TextPair(records[0];
        context.write(key_pair, delay_time);
    }
}