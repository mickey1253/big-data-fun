package flightdelay.join.practice;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightDelay implements Writable {

    public int depTime;
    public int arrTime;
    public String uniqueCarrier;
    public int flightNum;
    public int actualElapsedTime;
    public int arrDelay;
    public int depDelay;
    public String origin;
    public String destination;

    public FlightDelay() {
    }

    public FlightDelay(int depTime, int arrTime, String uniqueCarrier, int flightNum, int actualElapsedTime, int arrDelay, int depDelay, String origin, String destination) {
        this.depTime = depTime;
        this.arrTime = arrTime;
        this.uniqueCarrier = uniqueCarrier;
        this.flightNum = flightNum;
        this.actualElapsedTime = actualElapsedTime;
        this.arrDelay = arrDelay;
        this.depDelay = depDelay;
        this.origin = origin;
        this.destination = destination;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(depTime);
        out.writeInt(arrTime);
        out.writeUTF(uniqueCarrier);
        out.writeInt(flightNum);
        out.writeInt(actualElapsedTime);
        out.writeInt(arrDelay);
        out.writeInt(depDelay);
        out.writeUTF(origin);
        out.writeUTF(destination);

    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.depTime = in.readInt();
        this.arrTime = in.readInt();
        this.uniqueCarrier = in.readUTF();
        this.flightNum = in.readInt();
        this.actualElapsedTime = in.readInt();
        this.arrDelay = in.readInt();
        this.depDelay = in.readInt();
        this.origin = in.readUTF();
        this.destination = in.readUTF();

    }

    @Override
    public String toString() {
        return this.depTime + "," +
               this.arrTime + "," +
               this.uniqueCarrier + "," +
               this.flightNum + "," +
               this.actualElapsedTime + "," +
               this.arrDelay + "," +
               this.depDelay + "," +
               this.origin + "," +
               this.destination;
    }
}
