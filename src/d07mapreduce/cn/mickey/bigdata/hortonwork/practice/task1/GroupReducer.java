package d07mapreduce.cn.mickey.bigdata.hortonwork.practice.task1;


// ========= Reduce ===========

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class GroupReducer extends Reducer<Country, IntWritable, Country, IntWritable> {

    /**
     * The map task out will pass to reduce which will spawned reduce() method for each key.
     *
     *
     */
    public void reduce(Country key, Iterator<IntWritable> values, Context context) throws IOException,
            InterruptedException {

        int cnt = 0;
        while (values.hasNext()) {
            cnt = cnt + values.next().get();
        }
        context.write(key, new IntWritable(cnt));

    }

}

