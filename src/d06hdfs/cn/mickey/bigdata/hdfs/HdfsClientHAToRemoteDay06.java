package d06hdfs.cn.mickey.bigdata.hdfs;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
// import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;


public class HdfsClientHAToRemoteDay06 {

    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void init() throws Exception{

        System.setProperty("hadoop.home.dir", "C:/Users/505007855/BIG_DATA/Hadoop/TOOLS/InstallationPackage/hadoop-2.6.5/");

        conf = new Configuration();

        /* set replication instead of fs.defaultFS means HA system */
        conf.set("dfs.replication", "5");

        fs = FileSystem.get(conf);

        // fs = FileSystem.get(new URI("hdfs://www.mickey02.com:9000"),conf, "hadoop");

        /** This configuration can be found in core-site.xml / core-default.xml
            Since the code running from windows client, the current user is 505007855, so it has to set username "hadoop"
         */
        fs = FileSystem.get(new URI("hdfs://ns1/"), conf, "hadoop");



    }

    @Test
    public void testUpload() throws Exception{
        Thread.sleep(2000);
       fs.copyFromLocalFile(new Path("C:/Users/505007855/BIG_DATA/Hadoop/TOOLS/InstallationPackage/download.txt"), new Path("/download.txt.copy"));

        fs.close();
    }

    @Test
    public void testUploadStream() throws Exception{
        FSDataOutputStream outputStream = fs.create(new Path("/StreamTest.txt"), true);
        FileInputStream inputStream = new FileInputStream("C:/Users/505007855/BIG_DATA/Hadoop/srcdata/公司机##########虚拟机位置以及各主机对应的集群安装配置过程#####.txt");

        IOUtils.copy(inputStream, outputStream);

    }


    @Test
    public void testDownloadStream() throws Exception{

        FSDataInputStream inputStream = fs.open(new Path("/StreamTest.txt"));

        FileOutputStream outputStream = new FileOutputStream("C:/Users/505007855/BIG_DATA/Hadoop/TestResult/StreamTest.txt");

        // RandomAccess to an offset position
        inputStream.seek(12);

      //  IOUtils.copy(inputStream, outputStream);

         IOUtils.copy(inputStream, System.out);

        // IOUtils.copyBytes(inputStream, System.out, 1024);

    }


    @Test
    public void testDelet() throws IOException {
        boolean delete = fs.delete(new Path("/ToolsForBigData"), true);

        System.out.println("Delete successfully? " + delete);
    }

    @Test
    public void testList() throws Exception{

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("Block Size: " + fileStatus.getBlockSize());
            System.out.println("Owner: " + fileStatus.getOwner());
            System.out.println("Replication: " + fileStatus.getReplication());
            System.out.println("Permission: " + fileStatus.getPermission());
            System.out.println("Name:" + fileStatus.getPath().getName());
            System.out.println("==============");

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation b: blockLocations) {
                System.out.println("Block begin index: " + b.getOffset());
                System.out.println("Block size: " + b.getLength());

                String[] datanodes = b.getHosts();
                for (String dn: datanodes) {
                    System.out.println("Datanode: " + dn);
                }
            }
        }

    }


}
