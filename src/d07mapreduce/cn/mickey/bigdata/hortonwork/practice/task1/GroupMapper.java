package d07mapreduce.cn.mickey.bigdata.hortonwork.practice.task1;

// ========== Mapper =======

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public  class GroupMapper extends Mapper<LongWritable, Text, Country, IntWritable> {
    Country cntry = new Country();
    Text cntText = new Text();
    Text stateText = new Text();
    IntWritable populat = new IntWritable();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] keyvalue = line.split(",");
        cntText.set(new Text(keyvalue[0]));
        stateText.set(keyvalue[1]);
        populat.set(Integer.parseInt(keyvalue[3]));
        Country cntry = new Country(cntText, stateText);
        context.write(cntry, populat);

    }
}

