package d06hdfs.cn.mickey.bigdata.hadooprpc.client;

import d06hdfs.cn.mickey.bigdata.hadooprpc.protocol.IUserLoginService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class UserLoginAction {
    public static void main(String[] args) throws IOException {

        // If run app from windows machine. then we need to set the hadoop.home.dir
        System.setProperty("hadoop.home.dir", "C:/Users/505007855/BIG_DATA/Hadoop/TOOLS/InstallationPackage/hadoop-2.6.5/");

        // Remote communicate with Linux server, then need to set this hostname, otherwise use localhost
        IUserLoginService userLoginService = RPC.getProxy(IUserLoginService.class, 100L, new InetSocketAddress(/*localhost*/"www.mickey01.com", 8887), new Configuration());
        String login = userLoginService.login("Mickey", "123456");
        System.out.println(login);
    }
}
