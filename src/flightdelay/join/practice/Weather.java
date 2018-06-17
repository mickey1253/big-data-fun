package flightdelay.join.practice;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Weather implements Writable {

    private int prcp;
    private int tMax;
    private int tMin;

    public Weather() {}

    public Weather(int prcp, int tMax, int tMin) {
        this.prcp = prcp;
        this.tMax = tMax;
        this.tMin = tMin;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(prcp);
        out.writeInt(tMax);
        out.writeInt(tMin);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.prcp = in.readInt();
        this.tMax = in.readInt();
        this.tMin = in.readInt();
    }

    @Override
    public String toString() {
        return this.prcp + "," + this.tMax + "," + this.tMin;
    }
}
