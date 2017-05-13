package com.xueyou.demo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        String uri = args[0];
        System.out.println(uri);
//        HDFSUtils.ReadFile(uri, System.out);
        Configuration configuration = new Configuration();

        FSDataOutputStream fsDataOutputStream = null;
        FileSystem fileSystem = null;
        FileInputStream fileInputStream = null;

//        HDFSUtils.mkdirs(uri, "/user/xytest/wes", new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
        HDFSUtils.mkdirs(uri, "/user/xytest/cc");

//        try {
//            fileSystem = FileSystem.get(URI.create(uri), configuration);
//            fileSystem.mkdirs(new Path("/user/xytest/cc"));
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//        }
    }
}
