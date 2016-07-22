package com.joandora.hadoop.hdfs;

import org.apache.hadoop.mapred.JobConf;

/**
 * Created by joandora on 2016/7/21.
 * 读取hadoop配置文件 工具类
 */
public class JobConfUtils {
    public static final String  HDFS_URL = "hdfs://masterhadoop:9000/";
    public static final String  JOB_TRACKER_URL = "masterhadoop:9001";
    /**设置配置文件路径**/
    public static JobConf getJobConf(Class<?> clazz, String itemCFHadoop){
        JobConf conf = new JobConf(clazz);
        conf.setJobName("itemCFHadoop");
//        conf.addResource("classpath:/hadoop/core-site.xml");
//        conf.addResource("classpath:/hadoop/hdfs-site.xml");
//        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }
}
