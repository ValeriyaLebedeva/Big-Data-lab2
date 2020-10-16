package laba2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TimeDelayCounterJob {
    public static final int numReduceTasks = 2;
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: TimeDelayCounter <t_ontime path> <l_airport_id path> <output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        job.setJarByClass(TimeDelayCounterJob.class);
        job.setJobName("TimeDelayCounter");
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TimeMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DescriptionMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setPartitionerClass(MyPartitioner.class);
        job.setGroupingComparatorClass(MyComparator.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(KeyPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(TimeDelayCounterJob.numReduceTasks);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}