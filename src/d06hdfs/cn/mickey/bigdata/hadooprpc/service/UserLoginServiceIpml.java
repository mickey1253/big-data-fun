package d06hdfs.cn.mickey.bigdata.hadooprpc.service;

import d06hdfs.cn.mickey.bigdata.hadooprpc.protocol.IUserLoginService;

public class UserLoginServiceIpml implements IUserLoginService {
    @Override
    public String login(String name, String passwd) {
        return name + " Logged in successfully!";
    }
}
