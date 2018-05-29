package d04multithread.cn.mickey.mythread.pool;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class ExcutorDemo {

    public static void main(String[] args) {
        ExecutorService newSingleThreadExcutor = Executors.newSingleThreadExecutor();
        ExecutorService newCachedThreadPool = Executors.newCachedThreadPool();

        int cpuNums = Runtime.getRuntime().availableProcessors();
        System.out.println(cpuNums);

        ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(cpuNums);

        ScheduledExecutorService newScheduledThreadPool = Executors.newScheduledThreadPool(8);

        ScheduledExecutorService newSingelThreadScheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    }
}
