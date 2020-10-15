package laba2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<KeyPair, Text, Text, Text> {
    @Override
    protected void reduce(KeyPair key, Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text AirportDescription = iter.next();
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
        String meanTimeStr = String.format("mean: %.2f", sumTime / iterCount);
        String maxTimeStr = String.format("max: %.2f", maxTime);
        String minTimeStr = String.format("min: %.2f", minTime);
        Text outValue = new Text(meanTimeStr + "\t" + maxTimeStr + "\t" + minTimeStr );
        context.write(AirportDescription, outValue);
    }
}