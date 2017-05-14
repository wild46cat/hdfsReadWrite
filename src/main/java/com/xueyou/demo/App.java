package com.xueyou.demo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.FileInputStream;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        String uri = args[0];
        System.out.println(uri);
//        HDFSUtils.ReadFile(uri, System.out);
//        HDFSUtils.mkdirs(uri, "/user/xytest/wes", new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
//        HDFSUtils.mkdirs(uri, "/user/xytest/cc");
//        FileStatus fileStatus = HDFSUtils.getFileStatus(uri);
        Configuration configuration = new Configuration();

        FSDataOutputStream fsDataOutputStream = null;
        FileSystem fileSystem = null;
        FileInputStream fileInputStream = null;

    }
}
