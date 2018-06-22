package flight2.maximum.delay;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

public class YearPartitioner extends Partitioner<Text, FlightDelay> {

    public static Map<Integer, Integer> yearMap = new HashMap<>();

    static {
        yearMap.put(2007, 0);
        yearMap.put(2008, 1);
    }

    @Override
    public int getPartition(Text key, FlightDelay flightDelay, int i) {
        Integer partitionID = yearMap.get(flightDelay.getYear());
        return partitionID;
    }

}
