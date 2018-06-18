package flightdelay1.join.practice;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DelayWeather implements Writable {

  public FlightDelay flightDelay;
  public Weather weather;

  @Override
  public void write(DataOutput dataOutput) throws IOException {

    flightDelay.write(dataOutput);
    weather.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {

    flightDelay = new FlightDelay();
    flightDelay.readFields(dataInput);
    weather = new Weather();
    weather.readFields(dataInput);
  }

    @Override
    public String toString() {
        return this.flightDelay + "," + this.weather;
    }
}
