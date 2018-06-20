package d07mapreduce.cn.mickey.bigdata.provinceflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;


/**
 * Created by Mickey on 2016/11/7.
 * Key Value对应的是map的输出kv类型
 *
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    public static HashMap<String, Integer> provinceDict = new HashMap<>();

    static{
        provinceDict.put("136", 0);
        provinceDict.put("137", 1);
        provinceDict.put("138", 2);
        provinceDict.put("139", 3);

        //实际生产环境中通常连接数据库，将查询结果保存到内存里
    }

    @Override
    public int getPartition(Text key, FlowBean flowBean, int numPartitions) {

       String prefix = key.toString().substring(0,3);
       Integer provinceID = provinceDict.get(prefix);

        return provinceID == null? 4: provinceID;
    }
}
