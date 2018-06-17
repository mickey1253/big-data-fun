package flightdelay.join.practice;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readInt();
        month = in.readInt();
        day = in.readInt();

    }

    @Override
    public int hashCode(){
        return year + month + year;
    }

    @Override
    public boolean equals(Object obj){
        if(obj instanceof Datee){
            Datee date = (Datee) obj;
            if(year == date.year && month == date.month && day == date.day){
                return true;
            }
        }

        return false;
    }

    @Override
    public int compareTo(Datee date) {
        int response = this.year - date.year;
        if(response == 0){
            response = this.month - date.month;
        }

        if(response == 0){
            response = this.day - date.day;
        }


        return response;
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
    public String toString() {
        return this.year + "," + this.month + "," + this.day;
    }
}
