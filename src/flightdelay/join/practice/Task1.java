package flightdelay.join.practice;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**

 Environment Details
 A one-node HDP cluster is running on a server named namenode that is installed with various HDP components, including HDFS, MapReduce, YARN, Tez and Slider.
 You are currently logged in to an Ubuntu instance as a user named horton. As the horton user, you can SSH onto the cluster as the root user:
 $ ssh root@namenode
 The root password on the namenode is hadoop.
 Eclipse is installed and a shortcut is provided on the Desktop.
 A project named Task1 is created for you, and a class named task1.Task1 is stubbed out already. The build file for this project is preconfigured to use task1.Task1 as the main class, and the project has the proper build path for developing Hadoop MapReduce applications.
 To build the project, right-click on the Task1 project folder in Eclipse and select Run As -> Gradle Build.
 Ambari is available at http://namenode:8080. The username and password for Ambari are both admin.
 TASK 1
 There are two folders in HDFS in the /user/horton folder: flightdelays and weather. These are comma-separated files that contain flight delay information for airports in the U.S. for the year 2008, along with the weather data from the San Francisco airport. Write and execute a Java MapReduce application that satisfies the following criteria:

 Join the flight delay data in flightdelays with the weather data in weather. Join the data by the day, month and year and also where the "Dest" column in flightdelays is equal to "SFO".
 The output of each delayed flight into SFO consists of the following fields:
 Year,Month,DayofMonth,DepTime,ArrTime,UniqueCarrier,FlightNum,
 ActualElapsedTime,ArrDelay,DepDelay,Origin,Dest,PRCP,TMAX,TMIN
 For example, for the date 2008-01-03, there is a delayed flight number 488 from Las Vegas (LAS) to San Francisco (SFO). The corresponding output would be:
 2008,1,3,1426,1605,WN,488,99,35,31,LAS,SFO,43,150,94
 The output is sorted by date ascending, and on each day the output is sorted by ArrDelay descending (so that the longest arrival delays appear first).
 The output is in text files in a new folder in HDFS named task1 with values separated by commas
 The output is in two text files

 Data:
 -------------------------sfo_weather.csv ---------------------------------------

 STATION_NAME,YEAR,MONTH,DAY,PRCP,TMAX,TMIN
 SAN FRANCISCO INTERNATIONAL AIRPORT CA US,2008,01,01,0,122,39
 SAN FRANCISCO INTERNATIONAL AIRPORT CA US,2008,01,02,0,117,39
 SAN FRANCISCO INTERNATIONAL AIRPORT CA US,2008,01,03,43,150,94
 SAN FRANCISCO INTERNATIONAL AIRPORT CA US,2008,01,04,533,150,100
 SAN FRANCISCO INTERNATIONAL AIRPORT CA US,2008,01,05,196,122,78
 SAN FRANCISCO INTERNATIONAL AIRPORT CA US,2008,01,06,15,106,50
 SAN FRANCISCO INTERNATIONAL AIRPORT CA US,2008,01,07,0,111,67
 SAN FRANCISCO INTERNATIONAL AIRPORT CA US,2008,01,08,20,128,61

 -------------------------flight_delays1?2?3.csv----------------------------------

 Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,
 ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,
 CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay
 2008,1,3,4,2003,1955,2211,2225,WN,335,N712SW,128,150,116,-14,8,IAD,TPA,810,4,8,0,,0,NA,NA,NA,NA,NA
 2008,1,3,4,754,735,1002,1000,WN,3231,N772SW,128,145,113,2,19,IAD,TPA,810,5,10,0,,0,NA,NA,NA,NA,NA
 2008,1,3,4,628,620,804,750,WN,448,N428WN,96,90,76,14,8,IND,BWI,515,3,17,0,,0,NA,NA,NA,NA,NA
 2008,1,3,4,926,930,1054,1100,WN,1746,N612SW,88,90,78,-6,-4,IND,BWI,515,3,7,0,,0,NA,NA,NA,NA,NA
 2008,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,34,34,IND,BWI,515,3,10,0,,0,2,0,0,0,32
 2008,1,3,4,1940,1915,2121,2110,WN,378,N726SW,101,115,87,11,25,IND,JAX,688,4,10,0,,0,NA,NA,NA,NA,NA
 2008,1,3,4,1937,1830,2037,1940,WN,509,N763SW,240,250,230,57,67,IND,LAS,1591,3,7,0,,0,10,0,0,0,47
 2008,1,3,4,1039,1040,1132,1150,WN,535,N428WN,233,250,219,-18,-1,IND,LAS,1591,7,7,0,,0,NA,NA,NA,NA,NA

 Understand:
 1.Inner join, we can see weather data is small enough to get into memory, so let's start with map side join.
 a.Add weather data as cache file.Use day, month and year as the join key.
 b.Use "Dest" column in flightdelays as filter, which will filter "Dest" column in flightdelays is equal to "SFO".
 c.Get the "SFO" from arguments in main().
 2.Use Year,Month,DayofMonth as key,
 use DepTime,ArrTime,UniqueCarrier,FlightNum,ActualElapsedTime,ArrDelay,DepDelay,Origin,Dest,PRCP,TMAX,TMIN  as value.
 a.DepTime,ArrTime,UniqueCarrier,FlightNum,ActualElapsedTime,ArrDelay,DepDelay,Origin,Dest from flight_delays1?2?3.csv.
 b.PRCP,TMAX,TMIN from sfo_weather.csv.
 3.Modify Year,Month,DayofMonth, ArrDelay as key. Custom output format.
 4.Output dir is "task1", output file is text file, fields separated by commas.
 5.Reducer task is two.

 */


public class Task1 extends Configured implements Tool {

    private static final String DESTINATION = "Dest";

    public static class MapSideJoinMapper extends Mapper<LongWritable, Text, DateDelay, DelayWeather>{

            private Map<Datee, Weather> map = new HashMap<Datee, Weather>();

            private String destination;

            @Override
            protected void setup(Mapper<LongWritable, Text, DateDelay, DelayWeather>.Context context) throws IOException, InterruptedException{
                    destination = context.getConfiguration().get(DESTINATION);
                    BufferedReader reader = new BufferedReader(new FileReader("sfo_weather.csv"));
                    String line;
                    String[] wStr;
                    Datee datee;
                    Weather weather;
                    while((line = reader.readLine()) != null){

                        wStr = StringUtils.split(line, '\\', ',');

                        if(wStr[1].equals("YEAR")){
                            continue;
                        }

                        datee = new Datee(Integer.parseInt(wStr[1]), Integer.parseInt(wStr[2]), Integer.parseInt(wStr[3]));
                        weather = new Weather(Integer.parseInt(wStr[4]), Integer.parseInt(wStr[5]), Integer.parseInt(wStr[6]));
                        map.put(datee, weather);
                    }

                    reader.close();
                }


            @Override
            protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DateDelay, DelayWeather>.Context context) throws IOException, InterruptedException {

                String[] delays = StringUtils.split(value.toString(), '\\', ',');
                DateDelay dateDelay;
                Datee datee;
                if (delays[0].equalsIgnoreCase("Year")) {
                    return;
                }

                if (delays[17].trim().equalsIgnoreCase(destination)) {
                    boolean xx = Utils.replaceNAWithZero(delays);
                    if (xx) {
                        return;
                    }

                    datee = new Datee(Integer.parseInt(delays[0]), Integer.parseInt(delays[1]), Integer.parseInt(delays[2]));

                    if (map.containsKey(datee)) {
                        dateDelay = new DateDelay(datee, Integer.parseInt(delays[14]));
                        FlightDelay flightDelay = new FlightDelay(Integer.parseInt(delays[4]),
                                Integer.parseInt(delays[6]),
                                delays[8],
                                Integer.parseInt(delays[9]),
                                Integer.parseInt(delays[11]),
                                Integer.parseInt(delays[14]),
                                Integer.parseInt(delays[15]),
                                delays[16],
                                delays[17]
                        );
                        DelayWeather delayWeather = new DelayWeather();
                        delayWeather.flightDelay = flightDelay;
                        delayWeather.weather = map.get(datee);
                        context.write(dateDelay, delayWeather);
                    }
                }
             }
       }

       public static final class MapSideJoinReducer extends Reducer<DateDelay, DelayWeather, DateDelay, DelayWeather>{

           @Override
           protected void reduce(DateDelay key, Iterable<DelayWeather> values, Reducer<DateDelay, DelayWeather, DateDelay, DelayWeather>.Context context) throws IOException, InterruptedException {
               Iterator<DelayWeather> iterator = values.iterator();

               while(iterator.hasNext()){
                   context.write(key, iterator.next());
               }
           }
       }



    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Task1");
        Configuration conf = job.getConfiguration();


        conf.set(DESTINATION, args[0]);
        conf.set(TextOutputFormat.SEPERATOR, ",");

        job.addCacheFile(new URI("/ToolsForBigData/horton/weather/sfo_weather.csv"));
        Path out = new Path("task1");
        out.getFileSystem(conf).delete(out, true);
        FileInputFormat.setInputPaths(job, new Path("/ToolsForBigData/horton/flightdelays"));

        FileOutputFormat.setOutputPath(job, out);

        job.setJarByClass(getClass());
        job.setMapperClass(MapSideJoinMapper.class);
        job.setReducerClass(MapSideJoinReducer.class);
        job.setOutputFormatClass(DelayFileOutputFormat.class);
        job.setMapOutputKeyClass(DateDelay.class);
        job.setMapOutputValueClass(DelayWeather.class);
        job.setOutputKeyClass(DateDelay.class);
        job.setOutputValueClass(DelayWeather.class);
        job.setNumReduceTasks(2);

        return job.waitForCompletion(true)? 0:1;
    }


    public static void main(String[] args) {
        int result = 0;
        try{
            result = ToolRunner.run(new Configuration(), new Task1(), args);
        }catch (Exception e){
            e.printStackTrace();
        }

        System.exit(result);
    }
}
