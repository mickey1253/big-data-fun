package d07mapreduce.cn.mickey.bigdata.wordmean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import static com.google.common.base.Charsets.UTF_8;


public class WordMean extends Configuration implements Tool {

    private double mean = 0;

    private final static Text COUNT = new Text("count");
    private final static Text LENGTH = new Text("length");
    private final static LongWritable ONE = new LongWritable(1);


    public static class WordMeanMapper extends Mapper<Object, Text, Text, LongWritable>{

        private LongWritable wordLen = new LongWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()){
                String string = itr.nextToken();
                this.wordLen.set(string.length());
                context.write(LENGTH, this.wordLen);
                context.write(COUNT, ONE);
            }
        }

    }

    public static class WordMeanReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

        private LongWritable sum = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int theSum = 0;

            for (LongWritable val : values ) {

                theSum += val.get();

            }

            sum.set(theSum);
            context.write(key, sum);
        }

    }

    private double readAndCalcMean(Path path, Configuration conf) throws IOException{
        FileSystem fs = FileSystem.get(conf);
        Path file = new Path(path, "part-r-00000");

        if(!fs.exists(file)){
            throw new IOException("Output not found!");
        }

        BufferedReader br = null;

        try{
            br = new BufferedReader(new InputStreamReader(fs.open(file), UTF_8));
            long count = 0;
            long length = 0;

            String line;
            while ((line = br.readLine()) != null) {

                StringTokenizer st = new StringTokenizer(line);

                String type = st.nextToken();

                if(type.equals(COUNT.toString())){
                    String countLit = st.nextToken();
                    count = Long.parseLong(countLit);
                } else if(type.equals(LENGTH.toString())){
                    String lengthLit = st.nextToken();
                    length = Long.parseLong(lengthLit);
                }
            }

            double theMean = ((double)length) / ((double)count) ;

            System.out.println("The mean is: " + theMean);
            return theMean;
        }finally {
            if (br != null) {
                br.close();
            }
        }
    }



    @Override
    public int run(String[] args) throws Exception {

        if(args.length != 2){
            System.err.println("Usage: WordMean <in> <out>");
            return 0;
        }


        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordMean.class);
        job.setMapperClass(WordMeanMapper.class);
        job.setReducerClass(WordMeanReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        mean = readAndCalcMean(new Path(args[1]), conf);

        return (res?0:1);


    }

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }

    public double getMean(){ return  mean;}

    public static void main(String[] args) throws Exception {

        // When build new jar, need to backup current MANIFEST.MF file

        // Before Run jar, make sure time sync by using this command against all nodes: sudo date -s "2018-06-04 12:08:30"

        //  hdfs://ns1/
        //  Run jar: hadoop jar wordmean.jar "hdfs://ns1/start-hadoop.sh" "hdfs://ns1/wordmean"

        //  Check result:  hadoop fs -ls /wordcount
        //  hadoop fs -cat /wordmean/part-r-0000


        ToolRunner.run(new Configuration(), new WordMean(), args);
    }

}
