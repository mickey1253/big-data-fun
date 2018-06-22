package d07mapreduce.cn.mickey.bigdata.hortonwork.practice.MaximumValue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//求最大值
public class MaxNum extends Configured implements Tool {
    public static class MapClass extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private int maxNum = 0;
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] str = value.toString().split(",", 2);
            try {// 对于非数字字符我们忽略掉
                int temp = Integer.parseInt(str[0]);
                if (temp > maxNum) {
                    maxNum = temp;
                }
            } catch (NumberFormatException e) {
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            context.write(new IntWritable(maxNum), new IntWritable(maxNum));
        }
    }
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private int maxNum = 0;
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable val : values) {
                if ( val.get() > maxNum) {
                    maxNum = val.get();
                }
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            context.write(new IntWritable(maxNum), new IntWritable(maxNum));
        }
    }
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "MaxNum");
        job.setJarByClass(MaxNum.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MapClass.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
    public static void main(String[] args) throws Exception {
        long start = System.nanoTime();
        int res = ToolRunner.run(new Configuration(), new MaxNum(), args);
        System.out.println(System.nanoTime()-start);
        System.exit(res);
    }
}
/*
 * 最终输出：6009554 6009554
 *
 */
