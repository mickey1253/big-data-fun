package d07mapreduce.cn.mickey.bigdata.hortonwork.practice.task2;

// ------------- Task2 --------------

import org.apache.hadoop.io.WritableComparator;

public class Task2GroupComparator extends WritableComparator{


    public Task2GroupComparator() {
        super(Task2CustomKey.class,true);
    }

    @Override
    public int compare(Object a, Object b) {

        Task2CustomKey a1 = (Task2CustomKey)a;
        Task2CustomKey a2 = (Task2CustomKey)a;

        return -1*(a1.getDayofMonth().compareTo(a2.getDayofMonth()));
    }
}
