package d07mapreduce.cn.mickey.bigdata.wcdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordcountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

      //  System.setProperty("hadoop.home.dir", "C:/Users/505007855/BIG_DATA/Hadoop/TOOLS/InstallationPackage/hadoop-2.6.5/");

        // When build new jar, need to backup current MANIFEST.MF file

        // Before Run jar, make sure time sync by using this command against all nodes: sudo date -s "2018-06-04 12:08:30"
        // Otherwise it will get this exception:  org.apache.hadoop.yarn.exceptions.YarnException: Unauthorized request to start container

        // After sync timestamp, the active namenode might be changed to standby one, so the path need to make corresponding change

        // hdfs://ns1/
        // Run jar: hadoop jar wcdemo.jar "hdfs://www.mickey01.com:9000/start-hadoop.sh" "hdfs://www.mickey01.com:9000/wordcount"

        // Check result:  hadoop fs -ls /wordcount
        // hadoop fs -cat /wordcount/part-r-00000


        if (args == null || args.length == 0 ){
            args = new String[2];
            args[0] = "hdfs://www.mickey01.com:9000/start-hadoop.sh";
            args[1] = "hdfs://www.mickey01.com:9000/wordcount";
        }


        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(WordcountDriver.class);

        job.setMapperClass(WordcountMapper.class);

        job.setReducerClass(WordcountReducer.class);

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);

    }


}
