package flightdelay1.join.practice;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

public class DelayRecordWriter extends RecordWriter<DateDelay, DelayWeather> {

    private DataOutputStream out;

    private final static String SEPERATOR = ",";

    public DelayRecordWriter() {
    }

    public DelayRecordWriter(DataOutputStream out) {
        this.out = out;
    }

    @Override
    public void write(DateDelay dateDelay, DelayWeather delayWeather) throws IOException, InterruptedException {

        StringBuilder builder = new StringBuilder();
        builder.append(dateDelay.datee);
        builder.append(SEPERATOR);
        builder.append(delayWeather);
        builder.append("\n");
        out.write(builder.toString().getBytes());
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        out.close();

    }
}
