package d06hdfs.cn.mickey.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Iterator;
import java.util.Map.*;

public class HdfsClientLocalToRemoteDay06 {

    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void init() throws Exception{

        System.setProperty("hadoop.home.dir", "C:/Users/505007855/BIG_DATA/Hadoop/TOOLS/InstallationPackage/hadoop-2.6.5/");

        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://ns1/");
       // conf.set("fs.defaultFS", "hdfs://www.mickey01.com:9000");

        /** This configuration can be found in core-site.xml / core-default.xml */
       // fs = FileSystem.get(new URI("hdfs://www.mickey01.com:9000"), conf, "hadoop");
        fs = FileSystem.get(new URI("hdfs://ns1/"), conf, "hadoop");

        System.out.println("File config is set");
    }

    /** After test, check result in linux server by using following command:

     [hadoop@www ~]$ cd /ToolsForBigData/hadoop/app/hadoop-2.6.5/sbin
     [hadoop@www sbin]$ hadoop fs -put /etc/profile /profile
     [hadoop@www sbin]$ hadoop fs -ls /

     * */

    @Test
    public void testUpload() throws Exception{
        Thread.sleep(2000);
        fs.copyFromLocalFile(new Path("C:/Users/505007855/BIG_DATA/Hadoop/TOOLS/InstallationPackage/download.txt"), new Path("/download.txt.copy"));
        fs.close();
    }

    @Test
    public void testDelete() throws Exception{
        boolean delete = fs.delete(new Path("/wordmedian"), true);
        System.out.println("Delete Result is: " + delete);
    }

    @Test
    public void testDownload() throws Exception{
        fs.copyToLocalFile(new Path("/start-hadoop.sh"), new Path("C:/Users/505007855/BIG_DATA/Hadoop/TestResult/"));
        fs.close();
    }

    @Test
    public void testConf() throws Exception{
        Iterator<Entry<String, String>> iterator = conf.iterator();
        while(iterator.hasNext()){
            Entry<String, String> entry = iterator.next();
            System.out.println(entry.getKey() + "---" + entry.getValue());
        }
    }

    @Test
    public void testMkDir() throws Exception{
        boolean mkdirs = fs.mkdirs(new Path("/aaa/bbb"));

        System.out.println("Make Directory success? " + mkdirs);
    }

    @Test
    public void testListDir() throws Exception{
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus: listStatus) {
            System.out.println(fileStatus.getPath() + " -----> " + fileStatus.toString());
        }

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus next = listFiles.next();
            String name = next.getPath().getName();
            Path path = next.getPath();
            System.out.println(name + "======>>" + path.toString());
        }

    }



}
