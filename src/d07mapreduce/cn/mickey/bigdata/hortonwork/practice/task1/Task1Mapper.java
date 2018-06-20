package d07mapreduce.cn.mickey.bigdata.hortonwork.practice.task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import static org.apache.hadoop.mapreduce.Mapper.*;

public class Task1Mapper extends
        Mapper<LongWritable,Text,CustomKey,NullWritable> {

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
    protected void cleanup(Context context)throws IOException, InterruptedException {

        if(br != null){
            br.close();
        }
    }
}

