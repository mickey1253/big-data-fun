package d07mapreduce.cn.mickey.bigdata.hortonwork.practice.task1;

// ====== Group Driver ==========

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class GroupDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        FileUtils.deleteDirectory(new File("/Local/data/output"));
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "GroupMR");
        job.setJarByClass(GroupDriver.class);
        job.setMapperClass(GroupMapper.class);
        job.setReducerClass(GroupReducer.class);
        job.setOutputKeyClass(Country.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setMaxInputSplitSize(job, 10);
        FileInputFormat.setMinInputSplitSize(job, 100);
        FileInputFormat.addInputPath(job, new Path("/Local/data/Country.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/Local/data/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}