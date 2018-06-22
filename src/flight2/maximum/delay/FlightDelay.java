package flight2.maximum.delay;


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

// [arrival delay (column 15) + Departure airport code, Year, Month, DayOfMonth]
/*
*
*  Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay
 2007,1,6,7,1050,1050,1211,1210,WN,680,N283WN,81,80,65,1,0,LAX,SFO,337,6,10,0,,0,NA,NA,NA,NA,NA
*
* */
public class FlightDelay implements Writable {

    private int arrDelay;
    private String dptAirCode;
    private int year;
    private int month;
    private int dayOfMonth;

    public FlightDelay() {
    }

    public FlightDelay(int arrDelay, String dptAirCode, int year, int month, int dayOfMonth) {
        this.arrDelay = arrDelay;
        this.dptAirCode = dptAirCode;
        this.year = year;
        this.month = month;
        this.dayOfMonth = dayOfMonth;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(arrDelay);
        dataOutput.writeUTF(dptAirCode);
        dataOutput.writeInt(year);
        dataOutput.writeInt(month);
        dataOutput.writeInt(dayOfMonth);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.arrDelay = dataInput.readInt();
        this.dptAirCode = dataInput.readUTF();
        this.year = dataInput.readInt();
        this.month = dataInput.readInt();
        this.dayOfMonth = dataInput.readInt();
    }

    public int getArrDelay() {
        return arrDelay;
    }

    public void setArrDelay(int arrDelay) {
        this.arrDelay = arrDelay;
    }

    public String getDptAirCode() {
        return dptAirCode;
    }

    public void setDptAirCode(String dptAirCode) {
        this.dptAirCode = dptAirCode;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDayOfMonth() {
        return dayOfMonth;
    }

    public void setDayOfMonth(int dayOfMonth) {
        this.dayOfMonth = dayOfMonth;
    }

    @Override
    public String toString() {
        return arrDelay + "," + dptAirCode + "," + year + "," + month + "," + dayOfMonth;
    }

}
