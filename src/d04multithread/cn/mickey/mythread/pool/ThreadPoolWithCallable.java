package d04multithread.cn.mickey.mythread.pool;

import java.util.concurrent.*;

public class ThreadPoolWithCallable {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(4);

        for (int i = 0; i < 10; i++) {
            Future<String> submit = pool.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    Thread.sleep(5000);

                    return "b--" + Thread.currentThread().getName();
                }
            });

            System.out.println(submit.get());
        }

        pool.shutdown();
    }
}
