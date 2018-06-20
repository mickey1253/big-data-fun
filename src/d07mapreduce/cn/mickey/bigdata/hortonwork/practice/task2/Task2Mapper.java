package d07mapreduce.cn.mickey.bigdata.hortonwork.practice.task2;

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

public class Task2Mapper extends Mapper<LongWritable,Text,
        Task2CustomKey,NullWritable> {

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

