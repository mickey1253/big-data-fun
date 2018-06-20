package d07mapreduce.cn.mickey.bigdata.hortonwork.practice.task2;

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