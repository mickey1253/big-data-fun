package d07mapreduce.cn.mickey.bigdata.hortonwork.practice.task2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Task2Driver {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException,
            InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Task2Job");
        job.setJarByClass(Task2Driver.class);

        Path flightFilesPath = new Path("/user/horton/flightdelays");
        Path weatherCachedFilesPath = new Path("/user/horton/weather/sfo_weather.csv");
        Path outputDirectory = new Path("/user/horton/output");

        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputDirectory)){
            fs.delete(outputDirectory, true);
        }

        FileInputFormat.setInputPaths(job,flightFilesPath);
        FileOutputFormat.setOutputPath(job,outputDirectory);
        job.addCacheFile(weatherCachedFilesPath.toUri());

        job.setMapOutputKeyClass(Task2CustomKey.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Task2CustomKey.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(Task2Mapper.class);
        job.setPartitionerClass(Task2Partitioner.class);
        job.setCombinerKeyGroupingComparatorClass(Task2GroupComparator.class);  //TODO watch out this. In tihs example we are comparing keys, so *CombinerKey* should be used.
        job.setNumReduceTasks(2);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}

