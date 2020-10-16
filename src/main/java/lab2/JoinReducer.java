package lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<KeyPair, Text, Text, Text> {
    @Override
    protected void reduce(KeyPair key, Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text AirportDescription = new Text(iter.next().toString());
        float maxTime = Float.MIN_VALUE * -1;
        float minTime = Float.MAX_VALUE;
        float sumTime = 0;
        int iterCount = 0;
        while (iter.hasNext()) {
            String timeString = iter.next().toString();
            float time = Float.parseFloat(timeString);
            if (time > 0) {
                iterCount++;
                sumTime += time;
                if (time > maxTime) {
                    maxTime = time;
                }
                if (time < minTime) {
                    minTime = time;
                }
            }
        }
        if (iterCount > 0) {
            String meanTimeStr = String.format("\"mean\": %.2f", sumTime / iterCount);
            String maxTimeStr = String.format("\"max\": %.2f", maxTime);
            String minTimeStr = String.format("\"min\": %.2f", minTime);
            Text outValue = new Text("{\"Time delay\": [{" + meanTimeStr + "}, {" + maxTimeStr + "}, {" + minTimeStr + "}]}" );
            context.write(AirportDescription, outValue);
        }
    }
}