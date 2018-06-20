package d07mapreduce.cn.mickey.bigdata.hortonwork.practice.task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;

public class CustomKey implements WritableComparable<CustomKey> {

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
