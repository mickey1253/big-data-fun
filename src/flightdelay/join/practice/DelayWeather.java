package flightdelay.join.practice;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DelayWeather implements Writable {

    public FlightDelay flightDelay;
    public Weather weather;

    @Override
    public void write(DataOutput out) throws IOException {
        flightDelay.write(out);
        weather.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        flightDelay = new FlightDelay();
        weather = new Weather();

        flightDelay.readFields(in);
        weather.readFields(in);

    }

    @Override
    public String toString() {
        return this.flightDelay + ", " + weather;
    }
}
