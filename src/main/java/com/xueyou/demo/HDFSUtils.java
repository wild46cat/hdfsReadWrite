package com.xueyou.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

/**
 * Created by wuxueyou on 2017/5/11.
 */
public class HDFSUtils {
    public static Configuration configuration = new Configuration();

    /**
     * 读取hdfs文件中的内容到out中
     *
     * @param uri
     * @param out
     * @throws IOException
     */
    public static void ReadFile(String uri, OutputStream out) {
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(URI.create(uri), configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        InputStream in = null;
        try {
            in = fileSystem.open(new Path(uri));
            IOUtils.copyBytes(in, out, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }

    /**
     * 本地文件拷贝到hdfs
     *
     * @param localPath
     * @param dstUri
     */
    public static void CopyFromLocal(String localPath, String dstUri) {

        FSDataOutputStream fsDataOutputStream = null;
        FileSystem fileSystem = null;
        FileInputStream fileInputStream = null;

        try {
            fileInputStream = new FileInputStream(localPath);
            fileSystem = FileSystem.get(URI.create(dstUri), configuration);
            fsDataOutputStream = fileSystem.create(new Path(dstUri));
            IOUtils.copyBytes(fileInputStream, fsDataOutputStream, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fileSystem.close();
                fileInputStream.close();
                fsDataOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
            }
        }

    }

    /**
     * 创建文件夹
     * @param uri hdfs://192.168.0.84:8020/
     * @param path
     * @param fsPermission
     * @return
     */
    public static boolean mkdirs(String uri, String path, FsPermission fsPermission) {
        FileSystem fileSystem = null;
        boolean result = false;
        try {
            fileSystem = FileSystem.get(URI.create(uri), configuration);
            result = fileSystem.mkdirs(new Path(path), fsPermission);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return result;
        }
    }

    /**
     *创建文件夹
     * @param uri  hdfs://192.168.0.84:8020/
     * @param path
     * @return
     */
    public static boolean mkdirs(String uri, String path) {
        FileSystem fileSystem = null;
        boolean result = false;
        try {
            fileSystem = FileSystem.get(URI.create(uri), configuration);
            result = fileSystem.mkdirs(new Path(path));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return result;
        }
    }
}
