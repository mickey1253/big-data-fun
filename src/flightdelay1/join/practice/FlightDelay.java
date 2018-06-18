package flightdelay1.join.practice;

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
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(depTime);
        dataOutput.writeInt(arrTime);
        dataOutput.writeUTF(uniqueCarrier);
        dataOutput.writeInt(flightNum);
        dataOutput.writeInt(actualElapsedTime);
        dataOutput.writeInt(arrDelay);
        dataOutput.writeInt(depDelay);
        dataOutput.writeUTF(origin);
        dataOutput.writeUTF(destination);



    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.depTime = dataInput.readInt();
        this.arrTime = dataInput.readInt();
        this.uniqueCarrier = dataInput.readUTF();
        this.flightNum = dataInput.readInt();
        this.actualElapsedTime = dataInput.readInt();
        this.arrDelay = dataInput.readInt();
        this.depDelay = dataInput.readInt();
        this.origin = dataInput.readUTF();
        this.destination = dataInput.readUTF();

    }

    @Override
    public String toString() {
        return
        this.depTime + "," +
        this.arrTime  + "," +
        this.uniqueCarrier  + "," +
        this.flightNum  + "," +
        this.actualElapsedTime  + "," +
        this.arrDelay  + "," +
        this.depDelay  + "," +
        this.origin  + "," +
        this.destination  + ",";
    }
}
