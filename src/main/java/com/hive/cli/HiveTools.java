package com.hive.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HiveTools {
    public static void main(String[] args) {
        try {
//            HiveClient hiveClient = new HiveClient();
//            hiveClient.dropAllTablesBykey("intermediate");
//            System.setProperty("HADOOP_USER_NAME", "hdfs");
            Configuration configuration = new Configuration();
//            configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//
            FileSystem fileSystem = FileSystem.get(configuration);
//
//            FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/kylin"));
//            for (FileStatus fileStatus : fileStatuses) {
//                if (fileStatus.getPath().toString().contains("whf")) {
//                    fileSystem.delete(fileStatus.getPath(), true);
//                    System.out.println("Path: " + fileStatus.getPath());
//                }
//            }
        } catch (Exception e) {
            System.out.println(e);
            //do nothing
        }
    }
}
