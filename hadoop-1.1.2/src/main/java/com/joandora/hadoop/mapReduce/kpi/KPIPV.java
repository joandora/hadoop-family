package com.joandora.hadoop.mapReduce.kpi;

import com.joandora.hadoop.hdfs.HDFSUtils;
import com.joandora.hadoop.hdfs.JobConfUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
/**
 * 页面访问量统计MapReduce
 * 统计每个页面的访问次数
 * 完成的功能类似于单词统计：
 * 以每个访问网页的资源路径为键，经过mapreduce任务，
 * 最终得到每一个资源路径的访问次数
 * 我的博客：http://blog.csdn.net/u010156024/article/details/50147697
 */
public class KPIPV {

    public static class KPIPVMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            KPIEntity kpi = KPIEntity.filterPVs(value.toString());
            if (kpi.isValid()) {
                /**
                 * 以页面的资源路径为键，每访问一次，值为1
                 * 作为map任务的输出
                 */
                word.set(kpi.getRequest());
                output.collect(word, one);
            }
        }
    }

    public static class KPIPVReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            result.set(sum);
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        String wordNumCountFile = KPIPV.class.getResource("/mapReduce/access.log").getPath();
        String inPath = JobConfUtils.HDFS_URL + "/user/wordNumCount/mapReduce";
        String outPath = JobConfUtils.HDFS_URL + "/user/wordNumCount/dedup_out";
        String outFile = outPath + "/part-00000";
        JobConf jobConf =JobConfUtils.getJobConf(KPIPV.class,"ItemCFHadoop");
        HDFSUtils hdfs = new HDFSUtils(JobConfUtils.HDFS_URL, jobConf);
        hdfs.rmr(inPath);
        hdfs.rmr(outPath);
        hdfs.mkdirs(inPath);
        hdfs.copyFile(wordNumCountFile, inPath);
//        hdfs.ls(inPath);

        JobConf conf = new JobConf(KPIPV.class);
//        conf.addResource("classpath:/hadoop/core-site.xml");
//        conf.addResource("classpath:/hadoop/hdfs-site.xml");
//        conf.addResource("classpath:/hadoop/mapred-site.xml");
        //设置map输出的键类型，对应于map的输出以资源路径字符串作为键
        conf.setMapOutputKeyClass(Text.class);
        //设置map输出的值类型，对应于map输出，值为1
        conf.setMapOutputValueClass(IntWritable.class);
        //设置reduce的输出键类型，和map相同
        conf.setOutputKeyClass(Text.class);
        //设置reduce的输出值类型，和map相同，不过是累加的值
        conf.setOutputValueClass(IntWritable.class);
        //设置map类
        conf.setMapperClass(KPIPVMapper.class);
        //设置合并函数，该合并函数和reduce完成相同的功能，提升性能，减少map和reduce之间数据传输量
        conf.setCombinerClass(KPIPVReducer.class);
        //设置reduce类
        conf.setReducerClass(KPIPVReducer.class);
        //设置输入文件类型，默认TextInputFormat，该行代码可省略
        conf.setInputFormat(TextInputFormat.class);
        //设置输出文件类型，默认TextOutputFormat,该行代码可省略
        conf.setOutputFormat(TextOutputFormat.class);
        //设置输入文件路径
        FileInputFormat.setInputPaths(conf, new Path(inPath));
        //设置输出文件路径。该路径不能存在，否则出错！！！
        FileOutputFormat.setOutputPath(conf, new Path(outPath));
        //运行启动任务
        JobClient.runJob(conf);
        hdfs.cat(outFile);
        System.exit(0);
    }
}
/*
*
*
23:04:45.251 [main] DEBUG com.joandora.hadoop.hdfs.HDFSUtils -
/about	5
/black-ip-list/	2
/cassandra-clustor/	3
/finance-rhive-repurchase/	13
/hadoop-family-roadmap/	13
/hadoop-hive-intro/	14
/hadoop-mahout-roadmap/	20
/hadoop-zookeeper-intro/	6
*/
