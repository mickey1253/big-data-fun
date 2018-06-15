package flightdelay.join.practice;



import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

public class DelayRecordWrite extends RecordWriter<DateDelay, DelayWeather> {

    private DataOutputStream out;
    private final static String SEPERATOR = ",";

    public DelayRecordWrite(DataOutputStream out) {
        this.out = out;
    }

    @Override
    public void write(DateDelay key, DelayWeather value) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        builder.append(key.date);
        builder.append(SEPERATOR);
        builder.append(value);
        builder.append("\n");
        out.write(builder.toString().getBytes());

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        out.close();
    }
}
