package flight2.maximum.delay;

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

import java.io.IOException;
import java.util.Iterator;

/**

 第二题：求最大值：

 HDFS系统中/user/horton/flights下有两个文件， 2007.cvs 和 2008.cvs （结构和上面的附件第一题里的一样）
 要求写一个mapreduce程序，实现以下功能：

 1. 求出每个Arrival airport code中arrival delay时间最长的一个
 2. 结果存放在 HDFS中的 /user/horton/task1下
 3. 每条结果包含以下数据： Arrival airport code, Maximum Arrival delay, Departure airport code, Year, Month, DayOfMonth,每个feild之间用逗号隔开
 4. 输出结果按照2007和2008分成两个文件
 5. 最终结果以Arrival airport code的字母正序排列

 Data:

 ------------ 2007.csv ------------
 Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay
 2007,1,6,7,1050,1050,1211,1210,WN,680,N283WN,81,80,65,1,0,LAX,SFO,337,6,10,0,,0,NA,NA,NA,NA,NA
 2007,1,6,7,1244,1245,1405,1405,WN,776,N720WN,81,80,68,0,-1,LAX,SFO,337,3,10,0,,0,NA,NA,NA,NA,NA
 2007,1,6,7,1547,1455,1655,1600,WN,173,N350SW,68,65,58,55,52,LAX,SJC,308,4,6,0,,0,21,0,3,0,31
 2007,1,6,7,1909,1910,1918,1915,WN,160,N489WN,69,65,56,3,-1,LBB,ABQ,289,5,8,0,,0,NA,NA,NA,NA,NA
 2007,1,6,7,1759,1745,1859,1850,WN,555,N512SW,60,65,49,9,14,LBB,AUS,341,3,8,0,,0,NA,NA,NA,NA,NA
 2007,1,6,7,847,850,954,955,WN,836,N775SW,67,65,52,-1,-3,LBB,AUS,341,5,10,0,,0,NA,NA,NA,NA,NA

 ---------- 2008.csv ------------

 Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay
 2008,1,9,3,1552,1550,1856,1900,WN,438,N307SW,124,130,113,-4,2,LIT,BWI,912,3,8,0,,0,NA,NA,NA,NA,NA
 2008,1,9,3,706,705,809,810,WN,7,N902WN,63,65,49,-1,1,LIT,DAL,296,3,11,0,,0,NA,NA,NA,NA,NA
 2008,1,9,3,1454,1500,1558,1605,WN,41,N465WN,64,65,53,-7,-6,LIT,DAL,296,4,7,0,,0,NA,NA,NA,NA,NA
 2008,1,9,3,732,735,826,835,WN,1194,N660SW,54,60,44,-9,-3,MAF,AUS,294,4,6,0,,0,NA,NA,NA,NA,NA
 2008,1,9,3,1835,1830,1928,1925,WN,2374,N347SW,53,55,41,3,5,MAF,AUS,294,4,8,0,,0,NA,NA,NA,NA,NA
 2008,1,9,3,1537,1535,1636,1635,WN,43,N305SW,59,60,46,1,2,MAF,DAL,319,3,10,0,,0,NA,NA,NA,NA,NA

 Understand:

 1. Map side: Airport codes (column 17) as the key, [arrival delay (column 15) + Departure airport code, Year, Month, DayOfMonth] => flightDelay as the value;
 2. Reduce side find the Maximum arrival delay for each key;
 3. Reduce side write the result: (key, value);
 4. Set Partition:
 public static HashMap<Integer, Integer> years = new HashMap<>();
 static{
 airCode.put(2007, 1);
 airCode.put(2008, 2);
 }
 getPartition(){
    years.get(flightDelay.year)
 }

 */

public class Task1Maximum extends Configured implements Tool {

    public static class MaximumMapper extends Mapper<LongWritable, Text, Text, FlightDelay>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] delays = StringUtils.split(line, '\\', ',');

            if(delays[0].equalsIgnoreCase("year")){
                return;
            }

            if(Utils.replaceNAwithZero(delays)){
                return;
            }

            String airCode = delays[16];

           //  [arrival delay (column 15) + Departure airport code, Year, Month, DayOfMonth]
           // Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay
            // 2008,1,9,3,1552,1550,1856,1900,WN,438,N307SW,124,130,113,-4,2,LIT,BWI,912,3,8,0,,0,NA,NA,NA,NA,NA

            FlightDelay flightDelay = new FlightDelay(
                Integer.parseInt(delays[14]),
                delays[17],
                Integer.parseInt(delays[0]),
                Integer.parseInt(delays[1]),
                Integer.parseInt(delays[2]));

            context.write(new Text(airCode), flightDelay);

        }
    }


        public static class MaximumReducer extends Reducer<Text, FlightDelay, Text, FlightDelay>{



            @Override
            protected void reduce(Text airCode, Iterable<FlightDelay> values, Context context) throws IOException, InterruptedException {

                 /*
                Iterator<FlightDelay> iterator = values.iterator();

                while(iterator.hasNext()){
                    context.write(airCode, iterator.next());
                    return;
                }*/

                int maximum = 0;

                String dptAirCode = "";
                int year = 0;
                int month = 0;
                int dayOfMonth = 0;

               for (FlightDelay flightDelay : values) {

                   if(flightDelay.getArrDelay() > maximum){
                       maximum = flightDelay.getArrDelay();
                       dptAirCode = flightDelay.getDptAirCode();
                       year = flightDelay.getYear();
                       month = flightDelay.getMonth();
                       dayOfMonth = flightDelay.getDayOfMonth();
                   }
               }

               // [arrival delay (column 15) + Departure airport code, Year, Month, DayOfMonth]
               context.write(airCode, new FlightDelay(maximum, dptAirCode, year, month, dayOfMonth ));



            }
        }

  public static void main(String[] args) {
    //

      int result = 0;

      try {
          result = ToolRunner.run(new Configuration(), new Task1Maximum(), args);
      } catch (Exception e) {
          e.printStackTrace();
      }

      System.exit(result);

  }

  @Override
  public int run(String[] strings) throws Exception {

    Job job = Job.getInstance(getConf(), "Task1Maximum");
    Configuration conf = job.getConfiguration();
    conf.set(TextOutputFormat.SEPERATOR, ",");

    job.setJarByClass(Task1Maximum.class);
    job.setMapperClass(MaximumMapper.class);
    job.setReducerClass(MaximumReducer.class);
    job.setPartitionerClass(YearPartitioner.class);
    job.setNumReduceTasks(2);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FlightDelay.class);
    job.setOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FlightDelay.class);
    FileInputFormat.setInputPaths(job, new Path("/user/horton/flights/real/"));
    FileOutputFormat.setOutputPath(job, new Path("/user/horton/Task1Maximum"));

    return job.waitForCompletion(true) ? 0 : 1;

   }

}
