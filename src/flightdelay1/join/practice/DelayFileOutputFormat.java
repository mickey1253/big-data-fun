package flightdelay1.join.practice;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DelayFileOutputFormat extends FileOutputFormat {


    @Override
    public RecordWriter<DateDelay, DelayWeather> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        int partition  = taskAttemptContext.getTaskAttemptID().getTaskID().getId();
        Path outDir = FileOutputFormat.getOutputPath(taskAttemptContext);
        Path fileName = new Path(outDir.getName() + Path.SEPARATOR + taskAttemptContext.getJobName() + "_" + partition);

        FileSystem fileSystem = fileName.getFileSystem(taskAttemptContext.getConfiguration());

        FSDataOutputStream out = fileSystem.create(fileName);

        return new DelayRecordWriter(out);
    }
}
