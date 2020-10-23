package lab2;


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class TimeMapper extends Mapper<LongWritable, Text, KeyPair, Text> {

    public static final int NUM_DELAY_TIME = 18;
    public static final int NUM_AIRPORT_ID = 14;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] records = value.toString().split(TimeDelayCounterJob.SPLITTER);
        if (records[NUM_DELAY_TIME].length() > 0) {
            Text delay_time = new Text(records[NUM_DELAY_TIME]);
            Text airport_id = new Text(records[NUM_AIRPORT_ID]);
            KeyPair key_pair = new KeyPair(airport_id, new Text(TimeDelayCounterJob.MARK_TIME));
            context.write(key_pair, delay_time);
        }
    }
}