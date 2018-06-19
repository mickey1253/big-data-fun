package d07mapreduce.cn.mickey.bigdata.hortonwork.practice.task1;

/**
 * Created by 505007855 on 3/28/2017.
 */
import java.io.IOException;

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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task1Mapper extends
        Mapper<LongWritable,Text,CustomKey,NullWritable>{

    private Map<String,String> sfoWeatherMap =
            new HashMap<String,String>();

    private BufferedReader br;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

      URI[] uris = context.getCacheFiles();

      if(uris != null && uris.length > 0){

          for(URI uri : uris){

              if(uri.getPath().contains("sfo_weather.csv")){

                  br = new BufferedReader(new FileReader("sfo_weather.csv"));
                  //TODO Watch out the file without complete path.
                  // TODO This is how it worked on AWS cloud.

                  String line;
                  while((line=br.readLine())!= null){
                      sfoWeatherMap.put("3","3");
                      String[] values = line.split(",");
                      if(values[0].equalsIgnoreCase
                              ("SAN FRANCISCO INTERNATIONAL AIRPORT CA US")){

                          String key = Integer.parseInt(values[1])+
                                  ","+Integer.parseInt(values[2])+
                                  ","+Integer.parseInt(values[3]);

                         String value = values[4]+","+values[5]+","+values[6];
                         sfoWeatherMap.put(key, value);
                     }
                 }
             }
         }
       }


       super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] values = value.toString().split(",");

        //Year[0]    Month[1]    DayofMonth[2]    DayOfWeek[3]
        // DepTime[4]    CRSDepTime[5]    ArrTime[6]    CRSArrTime[7]
        //UniqueCarrier[8]    FlightNum[9]    TailNum[10]
        // ActualElapsedTime[11]    CRSElapsedTime[12]
        // AirTime[13]    ArrDelay[14]  DepDelay[15]  Origin[16]  Dest[17]
        // Distance  TaxiIn  TaxiOut   Cancelled   CancellationCode  Diverted
        // CarrierDelay  WeatherDelay NASDelay SecurityDelay LateAircraftDelay

        //2008    1    3    4    2003    1955    2211    2225
        //  WN    335    N712SW    128    150    116    -14
        // 8    IAD    TPA    810    4    8    0
        // 0    NA    NA    NA    NA    NA

        //What we want
        //Year,Month,DayofMonth,DepTime,ArrTime,
        // UniqueCarrier,FlightNum,ActualElapsedTime,
        // ArrDelay,DepDelay,Origin,Dest,PRCP,TMAX,TMIN

        Text Year = new Text(values[0]);
        Text Month = new Text(values[1]);
        Text DayofMonth = new Text(values[2]);
        Text DepTime = new Text(values[4]);
        Text ArrTime = new Text(values[6]);
        Text UniqueCarrier = new Text(values[8]);
        Text FlightNum = new Text(values[9]);
        Text ActualElapsedTime = new Text(values[11]);
        Text ArrDelay = new Text(values[14]);
        Text DepDelay = new Text(values[15]);
        Text Origin = new Text(values[16]);
        Text Dest = new Text(values[17]);

        CustomKey customKey = new CustomKey();

        if(Dest.toString().equalsIgnoreCase("sfo")){

            String mapKey = Integer.parseInt(Year.toString())+
                    ","+Integer.parseInt(Month.toString())+
                    ","+Integer.parseInt(DayofMonth.toString());

            Text PRCP = new Text();
            Text TMAX = new Text();
            Text TMIN = new Text();

            if(sfoWeatherMap.get(mapKey) !=null){
                String[] mapValue = sfoWeatherMap.get(mapKey).split(",");

                PRCP = new Text(mapValue[0]);
                TMAX = new Text(mapValue[1]);
                TMIN = new Text(mapValue[2]);
            }

            customKey = new CustomKey(Year,Month,DayofMonth,
                    DepTime,ArrTime,UniqueCarrier,FlightNum,
                    ActualElapsedTime,ArrDelay,DepDelay,Origin,Dest,
                    PRCP,TMAX,TMIN);

            context.write(customKey,NullWritable.get());
        }
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text,
      Text, Text>.Context context)throws IOException, InterruptedException {

        if(br != null){
            br.close();
        }
    }
}


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements WritableComparable<CustomKey>{

    private Text Year,Month,DayofMonth,DepTime,ArrTime,
            UniqueCarrier,FlightNum,ActualElapsedTime,
            ArrDelay,DepDelay,Origin,Dest,
            PRCP,TMAX,TMIN;

    public CustomKey() {

        this.Year = new Text();
        this.Month = new Text();
        this.DayofMonth = new Text();
        this.DepTime = new Text();
        this.ArrTime = new Text();
        this.UniqueCarrier = new Text();
        this.FlightNum = new Text();
        this.ActualElapsedTime = new Text();
        this.ArrDelay = new Text();
        this.DepDelay = new Text();
        this.Origin = new Text();
        this.Dest = new Text();
        this.PRCP = new Text();
        this.TMAX = new Text();
        this.TMIN = new Text();
    }

    public CustomKey(Text Year, Text Month, Text DayofMonth,
                     Text DepTime, Text ArrTime, Text UniqueCarrier,
                     Text FlightNum, Text ActualElapsedTime,
                     Text ArrDelay, Text DepDelay, Text Origin,
                     Text Dest, Text PRCP, Text TMAX, Text TMIN) {

        this.Year = Year;
        this.Month = Month;
        this.DayofMonth = DayofMonth;
        this.DepTime = DepTime;
        this.ArrTime = ArrTime;
        this.UniqueCarrier = UniqueCarrier;
        this.FlightNum = FlightNum;
        this.ActualElapsedTime = ActualElapsedTime;
        this.ArrDelay = ArrDelay;
        this.DepDelay = DepDelay;
        this.Origin = Origin;
        this.Dest = Dest;
        this.PRCP = PRCP;
        this.TMAX = TMAX;
        this.TMIN = TMIN;
    }

    public Text getDayofMonth(){
        return this.DayofMonth;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Year.write(out);
        Month.write(out);
        DayofMonth.write(out);
        DepTime.write(out);
        ArrTime.write(out);
        UniqueCarrier.write(out);
        FlightNum.write(out);
        ActualElapsedTime.write(out);
        ArrDelay.write(out);
        DepDelay.write(out);
        Origin.write(out);
        Dest.write(out);
        PRCP.write(out);
        TMAX.write(out);
        TMIN.write(out);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        Year.readFields(in);
        Month.readFields(in);
        DayofMonth.readFields(in);
        DepTime.readFields(in);
        ArrTime.readFields(in);
        UniqueCarrier.readFields(in);
        FlightNum.readFields(in);
        ActualElapsedTime.readFields(in);
        ArrDelay.readFields(in);
        DepDelay.readFields(in);
        Origin.readFields(in);
        Dest.readFields(in);
        PRCP.readFields(in);
        TMAX.readFields(in);
        TMIN.readFields(in);
    }

    @Override
    public int compareTo(CustomKey o) {
        Calendar cal1 = Calendar.getInstance();
        cal1.set(Integer.parseInt(Year.toString()),
                Integer.parseInt(Month.toString()),
                Integer.parseInt(DayofMonth.toString()));

        Calendar cal2 = Calendar.getInstance();
        cal2.set(Integer.parseInt(o.Year.toString()),
                Integer.parseInt(o.Month.toString()),
                Integer.parseInt(o.DayofMonth.toString()));

        IntWritable arrivalDelay1 = new IntWritable(0);
        IntWritable arrivalDelay2 = new IntWritable(0);

        if(!(ArrDelay.toString().equalsIgnoreCase("NA"))
                && !(o.ArrDelay.toString().equalsIgnoreCase("NA"))){
            arrivalDelay1 = new IntWritable
                    (Integer.parseInt(ArrDelay.toString()));
            arrivalDelay2 = new IntWritable
                    (Integer.parseInt(o.ArrDelay.toString()));
        }

        if(cal1.compareTo(cal2) == 0){
            return -1*(arrivalDelay1.compareTo(arrivalDelay2));
        } else {
            return cal1.compareTo(cal2);
        }
    }

    @Override
    public String toString() {
        return Year+","+Month+","+DayofMonth+","+DepTime+
                ","+ArrTime+","+UniqueCarrier+","+FlightNum+","+
                ActualElapsedTime+","+ArrDelay+","+DepDelay+","+
                Origin+","+Dest+","+PRCP+","+TMAX+","+TMIN;
    }
}

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class Task1Partitioner extends Partitioner<CustomKey,NullWritable>{

    @Override
    public int getPartition(CustomKey key, NullWritable value, int numPartitions) {
        if( numPartitions == 0){
            return 0;
        }

        if(Integer.parseInt(key.getDayofMonth().toString()) % 2 == 0){
            return 1 % numPartitions;
        } else {
            return 2 % numPartitions;
        }
    }
}

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



// ------------- Task2 --------------

import org.apache.hadoop.io.WritableComparator;

public class Task2GroupComparator extends WritableComparator{


    public Task2GroupComparator() {
        super(Task2CustomKey.class,true);
    }

    @Override
    public int compare(Object a, Object b) {

        Task2CustomKey a1 = (Task2CustomKey)a;
        Task2CustomKey a2 = (Task2CustomKey)a;

        return -1*(a1.getDayofMonth().compareTo(a2.getDayofMonth()));
    }
}

//---------------------  Task2CustomKey ---------------
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Task2CustomKey implements WritableComparable<Task2CustomKey>{

    private Text Year;
    private Text Month;
    private Text DayofMonth;
    private Text DepTime;
    private Text ArrTime;
    private Text UniqueCarrier;
    private Text FlightNum;
    private Text ActualElapsedTime;
    private Text ArrDelay;
    private Text DepDelay;
    private Text Origin;
    private Text Dest;
    private Text PRCP;
    private Text TMAX;
    private Text TMIN;

    public Task2CustomKey() {
        Year = new Text();
        Month = new Text();
        DayofMonth = new Text();
        DepTime = new Text();
        ArrTime = new Text();
        UniqueCarrier = new Text();
        FlightNum = new Text();
        ActualElapsedTime = new Text();
        ArrDelay = new Text();
        DepDelay = new Text();
        Origin = new Text();
        Dest = new Text();
        PRCP = new Text();
        TMAX = new Text();
        TMIN = new Text();
    }

  public Task2CustomKey(Text Year, Text Month,
                        Text DayofMonth, Text DepTime,
                        Text ArrTime, Text UniqueCarrier,
                        Text FlightNum, Text ActualElapsedTime,
                        Text ArrDelay, Text DepDelay, Text Origin,
                        Text Dest, Text PRCP, Text TMAX, Text TMIN){

        this.Year = Year;
        this.Month = Month;
        this.DayofMonth = DayofMonth;
        this.DepTime = DepTime;
        this.ArrTime = ArrTime;
        this.UniqueCarrier = UniqueCarrier;
        this.FlightNum = FlightNum;
        this.ActualElapsedTime = ActualElapsedTime;
        this.ArrDelay = ArrDelay;
        this.DepDelay = DepDelay;
        this.Origin = Origin;
        this.Dest = Dest;
        this.PRCP = PRCP;
        this.TMAX = TMAX;
        this.TMIN = TMIN;
    }

    public Text getDayofMonth(){
        return this.DayofMonth;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        Year.write(out);
        Month.write(out);
        DayofMonth.write(out);
        DepTime.write(out);
        ArrTime.write(out);
        UniqueCarrier.write(out);
        FlightNum.write(out);
        ActualElapsedTime.write(out);
        ArrDelay.write(out);
        DepDelay.write(out);
        Origin.write(out);
        Dest.write(out);
        PRCP.write(out);
        TMAX.write(out);
        TMIN.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        Year.readFields(in);
        Month.readFields(in);
        DayofMonth.readFields(in);
        DepTime.readFields(in);
        ArrTime.readFields(in);
        UniqueCarrier.readFields(in);
        FlightNum.readFields(in);
        ActualElapsedTime.readFields(in);
        ArrDelay.readFields(in);
        DepDelay.readFields(in);
        Origin.readFields(in);
        Dest.readFields(in);
        PRCP.readFields(in);
        TMAX.readFields(in);
        TMIN.readFields(in);
    }

    @Override
    public int compareTo(Task2CustomKey o) {
        Calendar cal1 = Calendar.getInstance();
        cal1.set(Integer.parseInt(Year.toString()),
                Integer.parseInt(Month.toString()),
                Integer.parseInt(DayofMonth.toString()));

        Calendar cal2 = Calendar.getInstance();
        cal2.set(Integer.parseInt(o.Year.toString()),
                Integer.parseInt(o.Month.toString()),
                Integer.parseInt(o.DayofMonth.toString()));

        IntWritable arrivalDelay1 = new IntWritable(0);
        IntWritable arrivalDelay2 = new IntWritable(0);

        if(!(ArrDelay.toString().equalsIgnoreCase("NA"))
                && !(o.ArrDelay.toString().equalsIgnoreCase("NA"))){
            arrivalDelay1 = new IntWritable(
                    Integer.parseInt(ArrDelay.toString()));
            arrivalDelay2 = new IntWritable(
                    Integer.parseInt(o.ArrDelay.toString()));
        }

        if(cal1.compareTo(cal2) == 0){
            return -1*(arrivalDelay1.compareTo(arrivalDelay2));
        } else {
            return cal1.compareTo(cal2);
        }
    }


    @Override
    public String toString() {
        return Year.toString() +
                ","+ Month.toString() +
                ","+ DayofMonth.toString() +
                ","+ DepTime.toString() +
                ","+ ArrTime.toString() +
                ","+ UniqueCarrier.toString()+
                ","+ FlightNum.toString()
                +","+ ActualElapsedTime.toString() +
                ","+ ArrDelay.toString() +
                ","+ DepDelay.toString() +
                ","+ Origin.toString() +
                ","+ Dest.toString() +
                ","+ PRCP.toString() +
                ","+ TMAX.toString()
                +","+ TMIN.toString();
    }

}

// ---------------- Task2Partitioner --------------------------

import org.apache.hadoop.io.NullWritable;
        import org.apache.hadoop.mapreduce.Partitioner;

public class Task2Partitioner extends
        Partitioner<Task2CustomKey,NullWritable>{

    @Override
    public int getPartition(Task2CustomKey key, NullWritable value,
                            int numPartitions){

      if( numPartitions == 0){
          return 0;
      }

      if(Integer.parseInt(key.getDayofMonth().toString()) % 2 == 0){
          return 1 % numPartitions;
      } else {
          return 2 % numPartitions;
      }
    }
}

// ------------------- Task2Mapper -------------------------------

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task2Mapper extends Mapper<LongWritable,Text,
        Task2CustomKey,NullWritable>{

    private BufferedReader br = null;
    private Map<String,String> sfoWeatherMap =
            new HashMap<String,String>();

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

        URI[] uris = context.getCacheFiles();

        if(uris != null && uris.length>0){

            for(URI uri : uris){

                if(uri.getPath().contains("sfo_weather.csv")){

                  br = new BufferedReader(new FileReader
                          ("sfo_weather.csv"));
                  String line = null;

                  while((line = br.readLine()) != null){

                    //STATION_NAME(0)	YEAR(1)	MONTH(2)
                    // DAY(3)	PRCP(4)	TMAX(5)	TMIN(6)
                    //SAN FRANCISCO INTERNATIONAL AIRPORT CA US
                    // 2008	1	1	0	122	39
                    String[] weatherTokens = line.split(",");

                    if(weatherTokens[0].equalsIgnoreCase
                      ("SAN FRANCISCO INTERNATIONAL " +
                              "AIRPORT CA US")){
                      String key =
                              Integer.parseInt(weatherTokens[1])+","+
                              Integer.parseInt(weatherTokens[2])+","
                              +Integer.parseInt(weatherTokens[3]);

                      String value = weatherTokens[4]+","+
                              weatherTokens[5]+","+
                              weatherTokens[6];
                      sfoWeatherMap.put(key, value);
                    }
                  }
                }
            }
        }

        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //Data sample
        //Year(0)	Month(1)
        // DayofMonth(2)
        // DayOfWeek(3)	DepTime(4)
        // CRSDepTime(5)	ArrTime(6)
        // CRSArrTime(7)	UniqueCarrier(8)
        // FlightNum(9)	TailNum(10)	ActualElapsedTime(11)
        //CRSElapsedTime(12)	AirTime(13)
        // ArrDelay(14)	DepDelay(15)
        // Origin(16)	Dest(17)	Distance(18)
        // TaxiIn(19)	TaxiOut	Cancelled
        // CancellationCode	Diverted
        // CarrierDelay	WeatherDelay
        // NASDelay	SecurityDelay	LateAircraftDelay
        //2008	1	3	4	2003	1955	2211	2225
        // WN	335	N712SW	128	150	116	-14	8
        // IAD	TPA	810	4	8	0		0	NA	NA	NA	NA	NA

        //Expected output sample.
        //Year,Month,DayofMonth,DepTime,ArrTime,UniqueCarrier,
        // FlightNum,ActualElapsedTime,ArrDelay,DepDelay,Origin,Dest,
        // PRCP,TMAX,TMIN

        String[] inputValueSplits = value.toString().split(",");

        String Year = inputValueSplits[0];
        String Month = inputValueSplits[1];
        String DayofMonth = inputValueSplits[2];
        String DepTime = inputValueSplits[4];
        String ArrTime = inputValueSplits[6];
        String UniqueCarrier = inputValueSplits[8];
        String FlightNum = inputValueSplits[9];
        String ActualElapsedTime = inputValueSplits[11];
        String ArrDelay = inputValueSplits[14];
        String DepDelay = inputValueSplits[15];
        String Origin = inputValueSplits[16];
        String Dest = inputValueSplits[17];

        if(Dest.equalsIgnoreCase("sfo")){

            String  mapLookUpKey = Integer.parseInt(Year)+","+
                    Integer.parseInt(Month)+","+
                    Integer.parseInt(DayofMonth);

            String PRCP = null;
            String TMAX = null;
            String TMIN = null;

            if(sfoWeatherMap.get(mapLookUpKey) != null){
                String[] mapLookUpValues =
                        sfoWeatherMap.get(mapLookUpKey).split(",");
                PRCP = mapLookUpValues[0];
                TMAX = mapLookUpValues[1];
                TMIN = mapLookUpValues[2];
            }

            Task2CustomKey task2CustomKey =
                    new Task2CustomKey(new Text(Year)
                    ,new Text(Month),new Text(DayofMonth)
                    ,new Text(DepTime),new Text(ArrTime)
                    ,new Text(UniqueCarrier) ,new Text(FlightNum)
                    ,new Text(ActualElapsedTime),new Text(ArrDelay)
                    ,new Text(DepDelay),new Text(Origin)
                    ,new Text(Dest),new Text(PRCP)
                    ,new Text(TMAX),new Text(TMIN));

            context.write(task2CustomKey, NullWritable.get());
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        if(br != null){
            br.close();
        }
    }

}


// ----------- Task2Driver -------------------
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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



// ========  Compute average delay  =======

public class AverageAndTotalSalaryCompute {
    /*
     * data schema(tab separated) :-100 Steven King M SKING 515.123.4567
     * 17-JUN-03 AD_PRES 25798.9 90 Sex at position 4th and salary at 9th
     * position
     */
    public static class MapperClass extends
            Mapper<LongWritable, Text, Text, FloatWritable> {
        public void map(LongWritable key, Text empRecord, Context con)
                throws IOException, InterruptedException {
            String[] word = empRecord.toString().split("\\t");
            String sex = word[3];
            try {
                Float salary = Float.parseFloat(word[8]);
                con.write(new Text(sex), new FloatWritable(salary));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class ReducerClass extends
            Reducer<Text, FloatWritable, Text, Text> {
        public void reduce(Text key, Iterable<FloatWritable> valueList,
                           Context con) throws IOException, InterruptedException {
            try {
                Float total = (float) 0;
                int count = 0;
                for (FloatWritable var : valueList) {
                    total += var.get();
                    System.out.println("reducer " + var.get());
                    count++;
                }
                Float avg = (Float) total / count;
                String out = "Total: " + total + " :: " + "Average: " + avg;
                con.write(key, new Text(out));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf, "FindAverageAndTotalSalary");
            job.setJarByClass(AverageAndTotalSalaryCompute.class);
            job.setMapperClass(MapperClass.class);
            job.setReducerClass(ReducerClass.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);
            // Path p1 = new Path(args[0]);
            // Path p2 = new Path(args[1]);
            // FileInputFormat.addInputPath(job, p1);
            // FileOutputFormat.setOutputPath(job, p2);
            Path pathInput = new Path(
                    "hdfs://192.168.213.133:54310/user/hduser1/employee_records.txt");
            Path pathOutputDir = new Path(
                    "hdfs://192.168.213.133:54310/user/hduser1/testfs/output_mapred00");
            FileInputFormat.addInputPath(job, pathInput);
            FileOutputFormat.setOutputPath(job, pathOutputDir);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}


// Group By example

// customer key

public class Country implements WritableComparable<Country>{

        Text country;
        Text country;
        Text state;

public Country(Text country, Text state) {
        this.country = country;
        this.state = state;
        }
public Country() {
        this.country = new Text();
        this.state = new Text();
        }
public void write(DataOutput out) throws IOException {
        this.country.write(out);
        this.state.write(out);
        }
public void readFields(DataInput in) throws IOException {
        this.country.readFields(in);
        this.state.readFields(in);
        }
public int compareTo(Country pop) {
        if (pop == null)
        return 0;
        int intcnt = country.compareTo(pop.country);
        if (intcnt != 0) {
        return intcnt;
        } else {
        return state.compareTo(pop.state);

        }
        }
@Override
public String toString() {
        return country.toString() + "," + state.toString();
        }

}






