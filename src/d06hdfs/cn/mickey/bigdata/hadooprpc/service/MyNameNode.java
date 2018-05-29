package d06hdfs.cn.mickey.bigdata.hadooprpc.service;

import d06hdfs.cn.mickey.bigdata.hadooprpc.protocol.ClientNamenodeProtocol;

public class MyNameNode implements ClientNamenodeProtocol {
    @Override
    public String getMetaData(String path) {
        return path + ": 3 - {BLK_1, BLK_2} ...";
    }
}
