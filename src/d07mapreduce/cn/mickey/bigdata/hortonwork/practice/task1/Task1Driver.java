package d07mapreduce.cn.mickey.bigdata.hortonwork.practice.task1;

/**
 * Created by 505007855 on 3/28/2017.
 */
import java.io.IOException;
import d07mapreduce.cn.mickey.bigdata.hortonwork.practice.task2.Task2GroupComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Environment Details
 •	A one-node HDP cluster is running on a server named namenode
 that is installed with various HDP components,
 including HDFS, MapReduce, YARN, Tez and Slider.
 •	You are currently logged in to an Ubuntu instance as a user named horton.
 As the horton user, you can SSH onto the cluster as the root user:
 •	$ ssh root@namenode

 The root password on the namenode is hadoop.

 •	Eclipse is installed and a shortcut is provided on the Desktop.
 •	A project named Task1 is created for you, and a class named task1.
 Task1 is stubbed out already.
 The build file for this project is preconfigured to use task1.Task1 as the main class,
 and the project has the proper build path for developing Hadoop MapReduce applications.
 •	To build the project, right-click on the Task1 project folder in Eclipse and
 select Run As -> Gradle Build.
 •	Ambari is available at http://namenode:8080.
 The username and password for Ambari are both admin.
 ________________________________________
 TASK 1
 There are two folders in HDFS in the /user/horton folder:
 flightdelays and weather.
 These are comma-separated files that contain flight delay information
 for airports in the U.S. for the year 2008,
 along with the weather data from the San Francisco airport.
 Write and execute a Java MapReduce application that satisfies the following criteria:

 1.	Join the flight delay data in flightdelays with the weather data in weather.
 Join the data by the day, month and year and also
 where the "Dest" column in flightdelays is equal to "SFO".

 2.	The output of each delayed flight into SFO consists of the following fields:

 3.	Year,Month,DayofMonth,DepTime,ArrTime,UniqueCarrier,FlightNum,

 4.	ActualElapsedTime,ArrDelay,DepDelay,Origin,Dest,PRCP,TMAX,TMIN

 For example, for the date 2008-01-03, there is a delayed flight
 number 488 from Las Vegas (LAS) to San Francisco (SFO).
 The corresponding output would be:
 2008,1,3,1426,1605,WN,488,99,35,31,LAS,SFO,43,150,94

 5.	The output is sorted by date ascending,
 and on each day the output is sorted by ArrDelay descending
 (so that the longest arrival delays appear first).

 6.	The output is in text files in a new folder in HDFS named task1
 with values separated by commas

 7.	The output is in two text files
 */

public class Task1Driver {

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"MyTask1Job");
        job.setJarByClass(Task1Driver.class);

        Path inputFlightDelaysPath = new Path("/user/horton/flightdelays");
        Path output = new Path("/user/horton/task1output");
        Path cachedSFOWeather = new Path("/user/horton/weather/sfo_weather.csv");
        //TODO watch out this line. you need to specify complete path with filename.

        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output)){
            fs.delete(output, true);
        }
        FileInputFormat.setInputPaths(job, inputFlightDelaysPath);
        FileOutputFormat.setOutputPath(job, output);
        job.addCacheFile(cachedSFOWeather.toUri());
        //TODO don't go with setCacheFile if you have only one.

        job.setMapperClass(Task1Mapper.class);
        job.setPartitionerClass(Task1Partitioner.class);

        job.setCombinerKeyGroupingComparatorClass(Task2GroupComparator.class);
        //TODO watch out this. In tihs example we are comparing keys,
        //TODO  so *CombinerKey* should be used.

        job.setNumReduceTasks(2);
        //TODO this is required to split output into two files.

        job.setMapOutputKeyClass(CustomKey.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(CustomKey.class);
        job.setOutputValueClass(NullWritable.class);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}















