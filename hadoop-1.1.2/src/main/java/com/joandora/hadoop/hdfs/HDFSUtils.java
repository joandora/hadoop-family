package com.joandora.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * Created by joandora on 2016/7/21.
 * 操作hdfs的工具类
 */
public class HDFSUtils {
    private static final Logger logger = LoggerFactory.getLogger(HDFSUtils.class);

    /**hdf url**/
    private String hdfsPath;
    /**配置信息**/
    private Configuration conf;

    public HDFSUtils(String hdfsPath,Configuration conf) throws IOException {
        this.hdfsPath = hdfsPath;
        this.conf = conf;
    }

    /**创建文件夹 类似于：hadoop fs -mkdir /test **/
    public void mkdirs(String folder) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsPath), conf);
        Path path = new Path(folder);
        if (!fileSystem.exists(path)) {
            fileSystem.mkdirs(path);
            logger.debug("hdfs create dirs(not exist):{}",folder);
        }
        logger.debug("hdfs create dirs(exist):{}",folder);
        fileSystem.close();
    }
    /**删除文件夹 类似于：hadoop fs -rmr /test **/
    public void rmr(String folder) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsPath), conf);
        Path path = new Path(folder);
        fileSystem.deleteOnExit(path);
        logger.debug("hdfs delete dirs:{}",folder);
        fileSystem.close();
    }
    /**重命名文件夹**/
    public void rename(String src, String dst) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsPath), conf);
        Path srcPath = new Path(src);
        Path dstPath = new Path(dst);
        fileSystem.rename(srcPath, dstPath);
        logger.debug("hdfs rename: from {} to {}",src,dst);
        fileSystem.close();
    }
    /**同linux ls 类似于:hadoop fs -ls / **/
    public void ls(String folder) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsPath), conf);
        Path path = new Path(folder);
        FileStatus[] list = fileSystem.listStatus(path);
        logger.debug("hdfs ls: " + folder);
        logger.debug("hdfs ==========================================================");
        for (FileStatus f : list) {
            logger.debug("hdfs name: {}, folder: {}, size: {}\n", f.getPath(), f.isDir(), f.getLen());
        }
        logger.debug("hdfs ==========================================================");
        fileSystem.close();
    }
    /**创建文件**/
    public void createFile(String file, String content) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsPath), conf);
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        try {
            os = fileSystem.create(new Path(file));
            os.write(buff, 0, buff.length);
            logger.debug("hdfs create: " + file);
        } finally {
            if (os != null)
                os.close();
        }
        fileSystem.close();
    }
    /**复制文件**/
    public void copyFile(String local, String remote) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsPath), conf);
        fileSystem.copyFromLocalFile(new Path(local), new Path(remote));
        logger.debug("hdfs copy from: {} to {}",local,remote);
        fileSystem.close();
    }
    /**下载文件**/
    public void download(String remote, String local) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsPath), conf);
        Path path = new Path(remote);
        fileSystem.copyToLocalFile(path, new Path(local));
        logger.debug("hdfs download: from {} to {}",remote,local);
        fileSystem.close();
    }
    /**查看文件内容**/
    public String cat(String remoteFile) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsPath), conf);
        Path path = new Path(remoteFile);
        FSDataInputStream fsdis = null;
        logger.debug("hdfs cat: " + remoteFile);
        OutputStream baos = new ByteArrayOutputStream();
        String str = null;
        try {
            fsdis = fileSystem.open(path);
            IOUtils.copyBytes(fsdis, baos, 4096, false);
            str = baos.toString();
        } finally {
            IOUtils.closeStream(fsdis);
        }
        logger.debug(str);
        fileSystem.close();
        return str;
    }


    public static void main(String[] args) throws IOException {
        HDFSUtils hdfs = new HDFSUtils(JobConfUtils.HDFS_URL,JobConfUtils.getJobConf(HDFSUtils.class,"HDFSUtils"));
        hdfs.mkdirs("/tmp/new");
        hdfs.ls("/tmp");
    }
}
