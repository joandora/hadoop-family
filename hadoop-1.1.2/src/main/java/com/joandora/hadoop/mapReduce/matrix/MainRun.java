package com.joandora.hadoop.mapReduce.matrix;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.joandora.hadoop.hdfs.JobConfUtils;
import org.apache.hadoop.mapred.JobConf;

/**
 * <pre>
 * 延时矩阵乘法
 * MartrixMultiply：矩阵乘法
 * SparseMartrixMultiply: 稀疏矩阵乘法
 * http://blog.fens.me/tag/%E7%9F%A9%E9%98%B5/
 * </pre>
 */
public class MainRun {

    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static void main(String[] args) {
//        martrixMultiply();
        sparseMartrixMultiply();
    }
    
    public static void martrixMultiply() {
        Map<String, String> path = new HashMap<String, String>();
        path.put("m1", "mapReduce/matrix/m1.csv");// 本地的数据文件
        path.put("m2", "mapReduce/matrix/m2.csv");
        path.put("input", JobConfUtils.HDFS_URL + "/user/hdfs/matrix");// HDFS的目录
        path.put("input1", JobConfUtils.HDFS_URL + "/user/hdfs/matrix/m1");
        path.put("input2", JobConfUtils.HDFS_URL + "/user/hdfs/matrix/m2");
        path.put("output", JobConfUtils.HDFS_URL + "/user/hdfs/matrix/output");

        try {
            MartrixMultiply.run(path);// 启动程序
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
    
    public static void sparseMartrixMultiply() {
        Map<String, String> path = new HashMap<String, String>();
        path.put("m1", MainRun.class.getResource("/mapReduce/matrix/sm1.csv").getPath());// 本地的数据文件
        path.put("m2", MainRun.class.getResource("/mapReduce/matrix/sm2.csv").getPath());
        path.put("input", JobConfUtils.HDFS_URL + "/user/hdfs/matrix");// HDFS的目录
        path.put("input1", JobConfUtils.HDFS_URL + "/user/hdfs/matrix/m1");
        path.put("input2", JobConfUtils.HDFS_URL + "/user/hdfs/matrix/m2");
        path.put("output", JobConfUtils.HDFS_URL + "/user/hdfs/matrix/output");

        try {
            SparseMartrixMultiply.run(path);// 启动程序
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    public static JobConf config() {// Hadoop集群的远程配置信息
        JobConf conf = new JobConf(MainRun.class);
        conf.setJobName("MartrixMultiply");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }

}
