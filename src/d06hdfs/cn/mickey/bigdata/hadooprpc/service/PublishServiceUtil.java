package d06hdfs.cn.mickey.bigdata.hadooprpc.service;

import d06hdfs.cn.mickey.bigdata.hadooprpc.protocol.ClientNamenodeProtocol;
import d06hdfs.cn.mickey.bigdata.hadooprpc.protocol.IUserLoginService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.RPC.Builder;

public class PublishServiceUtil {

    public static void main(String[] args) throws Exception{

        // If run app from windows machine. then we need to set the hadoop.home.dir
        System.setProperty("hadoop.home.dir", "C:/Users/505007855/BIG_DATA/Hadoop/TOOLS/InstallationPackage/hadoop-2.6.5/");

        Builder builder = new RPC.Builder(new Configuration());
        // Remote communicate with Linux server, then need to set this hostname, otherwise use localhost
        builder.setBindAddress(/*localhost*/"www.mickey01.com")
                .setPort(8888)
                .setProtocol(ClientNamenodeProtocol.class)
                .setInstance(new MyNameNode());

       Server server = builder.build();
       server.start();


       Builder builder2 = new RPC.Builder(new Configuration());
        // Remote communicate with Linux server, then need to set this hostname, otherwise use localhost
       builder2.setBindAddress(/*localhost*/"www.mickey01.com")
               .setPort(8887)
               .setProtocol(IUserLoginService.class)
               .setInstance(new UserLoginServiceIpml());

       Server server2 = builder2.build();
       server2.start();


    }

}
