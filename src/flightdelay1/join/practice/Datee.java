package flightdelay1.join.practice;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Datee implements WritableComparable<Datee> {

    public int year;
    public int month;
    public int day;

    public Datee() {
    }

    public Datee(int year, int month, int day) {
        this.year = year;
        this.month = month;
        this.day = day;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

     dataOutput.writeInt(year);
     dataOutput.writeInt(month);
     dataOutput.writeInt(day);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        year = dataInput.readInt();
        month = dataInput.readInt();
        day = dataInput.readInt();

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

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    @Override
    public int compareTo(Datee o) {

        int response = this.year - o.year;
        if(response == 0){
            response = this.month - o.month;
        }
        if(response == 0){
            response = this.day - o.day;
        }

        return response;
    }

    @Override
    public boolean equals(Object o) {

        if(o instanceof Datee){
            Datee datee = (Datee) o;
            if(year == datee.year && month == datee.month && day == datee.day){
                return true;
            }
        }

        return false;

    }


    @Override
    public int hashCode() {

        return year + month + day;
    }

    @Override
    public String toString() {
        return this.year + "," +
               this.month + "," +
               this.day;
    }
}
