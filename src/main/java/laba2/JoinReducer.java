package laba2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {
    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text AirportDescription = iter.next();
        Text idAirport = new Text(key.idAirport);
        float maxTime = Float.MIN_VALUE;
        float minTime = Float.MAX_VALUE;
        float sumTime = 0;
        int iterCount = 0;
        while (iter.hasNext()) {
            String timeString = iter.next().toString();
            float time = Float.parseFloat(timeString);
            iterCount++;
            sumTime += time;
            if (time > maxTime) {
                maxTime = time;
            }
            if (time < minTime) {
                minTime = time;
            }
        }
        String meanTime = String.format("%.3f", sumTime / iterCount);
        Text outValue = new Text(call + "\t" + AirportDescription.toString());
        context.write(idAirport, outValue);
    }
}