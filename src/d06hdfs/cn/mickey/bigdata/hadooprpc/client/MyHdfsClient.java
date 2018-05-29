package d06hdfs.cn.mickey.bigdata.hadooprpc.client;

import d06hdfs.cn.mickey.bigdata.hadooprpc.protocol.ClientNamenodeProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;


public class MyHdfsClient {

    public static void main(String[] args) throws Exception {

        // If run app from windows machine. then we need to set the hadoop.home.dir
        System.setProperty("hadoop.home.dir", "C:/Users/505007855/BIG_DATA/Hadoop/TOOLS/InstallationPackage/hadoop-2.6.5/");

        // Remote communicate with Linux server, then need to set this hostname, otherwise use localhost
        ClientNamenodeProtocol namenode = RPC.getProxy( ClientNamenodeProtocol.class, 1L, new InetSocketAddress(/*"localhost" */ "www.mickey01.com", 8888), new Configuration());

        String metaData = namenode.getMetaData("/StreamTest.txt");

        System.out.println(metaData);

    }
}
