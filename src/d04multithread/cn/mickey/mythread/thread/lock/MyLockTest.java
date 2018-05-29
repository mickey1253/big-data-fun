package d04multithread.cn.mickey.mythread.thread.lock;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MyLockTest {

    private static ArrayList<Integer> arrayList = new ArrayList<>();
    static Lock lock = new ReentrantLock();

    public static void main(String[] args) {
        new Thread() {
            public void run() {
                Thread thread = Thread.currentThread();
                lock.lock();

                try{
                    System.out.println(thread.getName() + " Got the Lock");

                    for (int i = 0; i < 5; i++) {
                        System.out.println("i = " + i);
                        arrayList.add(i);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    System.out.println(thread.getName() + " Release the Lock");
                    lock.unlock();
                }
            }
        }.start();

        for (int i : arrayList ) {
            System.out.println("i = " + i);
        }

        new Thread(){
            public void run(){
                Thread thread = Thread.currentThread();
                lock.lock();
                try{
                    System.out.println(thread.getName() + " Got the lock");

                    for (int i = 0; i < 5; i++) {
                        System.out.println("i = " + i);
                        arrayList.add(i);
                    }

                }catch(Exception e){
                    e.printStackTrace();
                }finally{
                    System.out.println(thread.getName() + " Release the lock");
                    lock.unlock();
                }
            }
        }.start();


    }
}
