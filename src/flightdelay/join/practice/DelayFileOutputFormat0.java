package flightdelay.join.practice;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DelayFileOutputFormat0 extends FileOutputFormat {
    @Override
    public RecordWriter<DateDelay0, DelayWeather0> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        int partition = job.getTaskAttemptID().getTaskID().getId();
        Path outDir = FileOutputFormat.getOutputPath(job);
        Path fileName = new Path(outDir.getName() + Path.SEPARATOR + job.getJobName() + "_" + partition);
        FileSystem fileSystem = fileName.getFileSystem(job.getConfiguration());

        FSDataOutputStream out = fileSystem.create(fileName);
        return new DelayRecordWrite0(out);
    }
}
