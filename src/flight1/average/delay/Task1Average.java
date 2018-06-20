package flight1.average.delay;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
Write and execute a Java MapReduce application that satisfies all of the following criteria:

 1. The input of the application is the two text files in /user/horton/flights/.
 2. Your application computes the average departure delay (column 16) for each distinct airport code (column 17).
 3. Store the output in a new folder in HDFS named /user/horton/task1.
 4. The output is partitioned into exactly two files. Airport codes that start with 'A' through 'M' should be in one file,
    and airport codes that start with 'N' through 'Z' should be in another file.
 5. Each row in the output should consist of two values separated by a comma: the airport code and the value you computed for the average departure delay.
 6. Do NOT compute two averages (one for each year). Compute the average departure delay over the two year span of 2007 and 2008.

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

 1. Map side: Airport codes (column 17) as the key, departure delay (column 16) as the value;
 2. Reduce side: get the sum and the count of the departure delay;
 3. Reduce side write the result: (key, sum / count)
 4. Set Partition:
   public static HashMap<String, Integer> airCode = new HashMap<>();
     static{
         airCode.put("A", 0);
         airCode.put("B", 0);
         ...
         airCode.put("N", 0);;
         ...
     }
 * */

public class Task1Average extends Configured implements Tool {

    public static class AverageMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] airCodes = StringUtils.split(value.toString(), '\\', ',');
            if(airCodes[16].equalsIgnoreCase("Origin")){
                return;
            }

            String airCode = airCodes[16];
            int delay = Integer.parseInt(airCodes[15]);

            context.write(new Text(airCode), new IntWritable(delay));

        }



    }

    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }
}
