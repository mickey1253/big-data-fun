package d07mapreduce.cn.mickey.bigdata.wordmedian;

import com.google.common.base.Charsets;
import d07mapreduce.cn.mickey.bigdata.wordmean.WordMean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class WordMedian extends Configured implements Tool {

    private double median = 0;
    private final static IntWritable ONE = new IntWritable(1);


    public static class WordMedianMapper extends Mapper<Object, Text, IntWritable, IntWritable>{

        private IntWritable length = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());

            while(itr.hasMoreTokens()){
                String string = itr.nextToken();
                length.set(string.length());
                context.write(length, ONE);
            }

        }
    }


    public static class WordMedianReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private IntWritable val = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            val.set(sum);
            context.write(key, val);
        }
    }



    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: WordMedian <in> <out>");
            return 0;
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(WordMedian.class);
        job.setMapperClass(WordMedianMapper.class);
        job.setReducerClass(WordMedianReducer.class);

        job.setCombinerClass(WordMedianReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        long totalWords = job.getCounters().getGroup(TaskCounter.class.getCanonicalName()).findCounter("MAP_OUTPUT_RECORDS", "Map output records").getValue();
        int medianIndex1 = (int) Math.ceil(totalWords / 2.0 );
        int medianIndex2 = (int) Math.floor(totalWords / 2.0);


        median = readAndFindMedain(args[1], medianIndex1, medianIndex2, conf);


        return 0;
    }

    private double readAndFindMedain(String path, int medianIndex1, int medianIndex2, Configuration conf) throws IOException {

        FileSystem fs =  FileSystem.get(conf);

        Path file = new Path(path, "part-r-00000" );

        if(!fs.exists(file)){
            throw new IOException("Output not found!");
        }

        BufferedReader br = null;

        try{

            br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));
            int num = 0;
            String line = null;

            while((line = br.readLine()) != null){
                StringTokenizer st = new StringTokenizer(line);

                String currlen = st.nextToken();

                String lenthFreq = st.nextToken();

                int prevNum = num;

                num += Integer.parseInt(lenthFreq);

                if(medianIndex2 >= prevNum && medianIndex1 <= num){
                    System.out.println("The median is: " + currlen);
                    br.close();
                    return Double.parseDouble(currlen);
                } else if(medianIndex2 >= prevNum && medianIndex1 < num ){
                    String nextCurrlen = st.nextToken();
                    double theMedian =( (Integer.parseInt(currlen) + Integer.parseInt(nextCurrlen))) / 2.0;
                    System.out.println("The median is: " + theMedian);
                    br.close();
                    return theMedian;
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if ( br != null){
                br.close();
            }
        }

        return -1;

    }

    public static void main(String[] args) throws Exception {



        // When build new jar, need to backup current MANIFEST.MF file

        // Before Run jar, make sure time sync by using this command against all nodes: sudo date -s "2018-06-09 20:58:00"

        //  hdfs://ns1/
        //  Run jar: hadoop jar wordmedian.jar "hdfs://ns1/start-hadoop.sh" "hdfs://ns1/wordmedian"

        //  Check result:  hadoop fs -ls /wordmedian
        //  hadoop fs -cat /wordmedian/part-r-00000

        System.setProperty("hadoop.home.dir", "C:/Users/505007855/BIG_DATA/Hadoop/TOOLS/InstallationPackage/hadoop-2.6.5/");

        if (args == null || args.length == 0 ){
            args = new String[2];
            args[0] = "hdfs://ns1/start-hadoop.sh";
            args[1] = "hdfs://ns1/wordmedian";
        }

        ToolRunner.run(new Configuration(), new WordMedian(), args);
    }
}
