package main.java;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class WordReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//        <считаем количество записей в итераторе и генерируем запись в контекст
//        В контекст пишется пара — Text и LongWritable>
        long count=0;
        for (IntWritable value : values) {
            count++;
        }
        context.write(key, new LongWritable(count));
    }
}