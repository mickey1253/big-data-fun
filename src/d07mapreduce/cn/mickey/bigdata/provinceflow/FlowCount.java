package d07mapreduce.cn.mickey.bigdata.provinceflow;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Mickey on 10/25/2016.
 */
public class FlowCount {

    static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //将一行内容转成String
            String line = value.toString();
            //切分字段
            String[] fileds = line.split("\t");
            //获取手机号
            String phoneNbr = fileds[1];
            //取出上行流量和下载流量
            long upFlow = Long.parseLong(fileds[fileds.length-3]);
            long dFlow = Long.parseLong(fileds[fileds.length-2]);

            context.write(new Text(phoneNbr), new FlowBean(upFlow,dFlow));

        }


    }


    static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{

        // <1302115468,bean1><1302115468,bean2><1302115468,bean3><1302115468,bean4>...
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

            long sum_upFlow = 0;
            long sum_dFlow = 0;

            //遍历所有的bean，将其中的上行流量，下行流量分别累加

            for(FlowBean bean: values){

                sum_upFlow += bean.getUpFlow();
                sum_dFlow += bean.getdFlow();

            }

            FlowBean resultBean = new FlowBean(sum_upFlow, sum_dFlow);
            context.write(key, resultBean);

        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowCount.class);

        // 指定本业务job要使用到mapper/reducer业务类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //指定自定义的数据分区器
        job.setPartitionerClass(ProvincePartitioner.class);

        //同时指定相应数据分区数量的reduceTask
        job.setNumReduceTasks(5);

        // 如果设置NumReduceTasks 为1，则运行结果会保存到一个文件中
        // 如果设置NumReduceTasks 为2，3，4，则会报错，因为ProvincePartitioner划了5个分区

        //指定mapper输出数据到KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指定最终输出到数据的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定job输入的原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[1]));
       // FileInputFormat.setInputPaths(job, new Path(args[0]));

        //指定job的输出结果所在目录
       // FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        // job.submit();

        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);

    }

}
