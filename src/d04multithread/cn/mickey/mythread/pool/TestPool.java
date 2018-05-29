package d04multithread.cn.mickey.mythread.pool;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.*;

public class TestPool {

    public static void main(String[] args) throws  Exception{

        Future<?> submit = null;

        Random random = new Random();

        ExecutorService exec = Executors.newFixedThreadPool(4);

        ScheduledExecutorService sexec = Executors.newScheduledThreadPool(4);

        ArrayList<Future<?>> results = new ArrayList<>();

        for(int i = 0; i < 10; i++){
          //  submit = exec.submit(new TaskRunnable(i));
          //  submit = exec.submit(new TaskCallable(i));
          //  submit = sexec.submit(new TaskRunnable(i));
            submit = sexec.schedule(new TaskCallable(i), random.nextInt(10), TimeUnit.SECONDS);

            results.add(submit);
        }

        for (Future f : results) {

            boolean done = f.isDone();
            System.out.println(done ? "Already finished" : "Not finish yet" );
            System.out.println("Thread return future result: " + f.get());
        }

        exec.shutdown();
        sexec.shutdown();

    }

}
