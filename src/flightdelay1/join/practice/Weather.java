package flightdelay1.join.practice;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Weather implements Writable {

    private int prcp;
    private int tMax;
    private int tMin;

    public Weather() {
    }

    public Weather(int prcp, int tMax, int tMin) {
        this.prcp = prcp;
        this.tMax = tMax;
        this.tMin = tMin;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(prcp);
        dataOutput.writeInt(tMax);
        dataOutput.writeInt(tMin);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        prcp = dataInput.readInt();
        tMax = dataInput.readInt();
        tMax = dataInput.readInt();
    }

    public String toString(){
        return this.prcp + "," + this.tMax + "," + this.tMin;
    }

}
