package d04multithread.cn.mickey.mythread.pool;

import java.util.Random;
import java.util.concurrent.Callable;

public class TaskCallable implements Callable<String> {
    private  int s;
    Random r = new Random();

    public TaskCallable(int s) {
        this.s = s;
    }

    @Override
    public String call() throws Exception {
        String name = Thread.currentThread().getName();
        long currentTimeMillis = System.currentTimeMillis();

        System.out.println(name + " Start time is: " + currentTimeMillis / 1000);

        int rint = r.nextInt(3);

        try {
            Thread.sleep(rint * 1000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }

        System.out.println(name + " is working... " + s + " seconds");

        return s + "";
    }
}
