package d07mapreduce.cn.mickey.bigdata.hortonwork.practice.task2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class Task2Partitioner extends Partitioner<Task2CustomKey,NullWritable> {

    @Override
    public int getPartition(Task2CustomKey key, NullWritable value,
                            int numPartitions){

        if( numPartitions == 0){
            return 0;
        }

        if(Integer.parseInt(key.getDayofMonth().toString()) % 2 == 0){
            return 1 % numPartitions;
        } else {
            return 2 % numPartitions;
        }
    }
}