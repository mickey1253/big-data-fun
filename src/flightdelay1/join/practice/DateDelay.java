package flightdelay1.join.practice;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateDelay implements WritableComparable<DateDelay> {

    public Datee datee;
    public int arrDelay;

    public DateDelay() {
    }

    public DateDelay(Datee datee, int arrDelay) {
        this.datee = datee;
        this.arrDelay = arrDelay;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        datee.write(dataOutput);
        dataOutput.writeInt(arrDelay);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        datee = new Datee();
        datee.readFields(dataInput);
        arrDelay = dataInput.readInt();

    }

    @Override
    public int compareTo(DateDelay o) {

        int response = this.datee.compareTo(o.datee);

        if(response == 0){
            response = o.arrDelay - this.arrDelay;
        }

        return response;
    }

    @Override
    public String toString() {
        return this.datee + "," + this.arrDelay;
    }
}
