package laba2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TimeDelayCounter {
    public static final int numReduceTasks = 2;
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: TimeDelayCounter <l_airport_id path> <t_ontime path> <output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        job.setJarByClass(TimeDelayCounter.class);
        job.setJobName("TimeDelayCounter sort");
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, DescriptionJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TimeJoinMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setPartitionerClass(MyPartitioner.class);
        job.setGroupingComparatorClass(MyComparator.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(TextPair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(TimeDelayCounter.numReduceTasks);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}