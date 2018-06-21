package flight1.average.delay;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

public class AirCodePartitioner extends Partitioner<Text, IntWritable> {

    public static Map<String, Integer> airCodeMap = new HashMap<>();

    static{
        airCodeMap.put("A", 0);
        airCodeMap.put("B", 0);
        airCodeMap.put("C", 0);
        airCodeMap.put("D", 0);
        airCodeMap.put("E", 0);
        airCodeMap.put("F", 0);
        airCodeMap.put("G", 0);
        airCodeMap.put("H", 0);
        airCodeMap.put("I", 0);
        airCodeMap.put("J", 0);
        airCodeMap.put("K", 0);
        airCodeMap.put("L", 0);
        airCodeMap.put("M", 0);
        airCodeMap.put("N", 1);
        airCodeMap.put("O", 1);
        airCodeMap.put("P", 1);
        airCodeMap.put("Q", 1);
        airCodeMap.put("R", 1);
        airCodeMap.put("S", 1);
        airCodeMap.put("T", 1);
        airCodeMap.put("U", 1);
        airCodeMap.put("V", 1);
        airCodeMap.put("W", 1);
        airCodeMap.put("X", 1);
        airCodeMap.put("Y", 1);
        airCodeMap.put("Z", 1);
    }



    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        String prefix = text.toString().substring(0,1);
        Integer patitionID = airCodeMap.get(prefix);
        return patitionID;
    }
}
