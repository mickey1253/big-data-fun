package flightdelay.join.practice;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateDelay0 implements WritableComparable<DateDelay0> {

    public Datee0 date;
    public int arriveDelay;

    public DateDelay0(){}


    public DateDelay0(Datee0 date, int arriveDelay) {
        this.date = date;
        this.arriveDelay = arriveDelay;
    }


    @Override
    public void write(DataOutput out) throws IOException {

        date.write(out);
        out.writeInt(arriveDelay);

    }


    @Override
    public void readFields(DataInput in) throws IOException {

        date = new Datee0();
        date.readFields(in);
        arriveDelay = in.readInt();
    }



    @Override
    public int compareTo(DateDelay0 dateDelay) {

        int response = this.date.compareTo(dateDelay.date);
        if(response == 0){
            response = dateDelay.arriveDelay - this.arriveDelay;
        }

        return response;
    }

    @Override
    public String toString() {
        return this.date + "," + this.arriveDelay;
    }

}
