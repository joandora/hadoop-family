package mapReduce;
import java.io.IOException;
import java.util.StringTokenizer;

import com.joandora.hadoop.hdfs.HDFSUtils;
import com.joandora.hadoop.hdfs.JobConfUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.conan.myhadoop.hdfs.HdfsDAO;

/**
 * Created by teddy  on 2016/7/2.
 * hadoop 例子 wordcount
 */
public class WordNumCount {

    public static class  TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        /**计数为1**/
        private final static IntWritable one = new IntWritable(1);
        /**代表一个单词**/
        private Text word = new Text();
        /**
         * map方法完成工作就是读取文件
         * 将文件中每个单词作为key键，值设置为1，
         * 然后将此键值对设置为map的输出，即reduce的输入
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /**
             * StringTokenizer：字符串分隔解析类型
             * 之前没有发现竟然有这么好用的工具类
             * java.util.StringTokenizer
             * 1. StringTokenizer(String str) ：
             *  构造一个用来解析str的StringTokenizer对象。
             *  java默认的分隔符是“空格”、“制表符(‘\t’)”、“换行符(‘\n’)”、“回车符(‘\r’)”。
             * 2. StringTokenizer(String str, String delim) ：
             *  构造一个用来解析str的StringTokenizer对象，并提供一个指定的分隔符。
             * 3. StringTokenizer(String str, String delim, boolean returnDelims) ：
             *  构造一个用来解析str的StringTokenizer对象，并提供一个指定的分隔符，同时，指定是否返回分隔符。
             *
             * 默认情况下，java默认的分隔符是“空格”、“制表符(‘\t’)”、“换行符(‘\n’)”、“回车符(‘\r’)”。
             */
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }
    /**
     * reduce的输入即是map的输出，将相同键的单词的值进行统计累加
     * 即可得出单词的统计个数，最后把单词作为键，单词的个数作为值，
     * 输出到设置的输出文件中保存
     */
    public static class  IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception{
        String wordNumCountFile = WordNumCount.class.getResource("/mapReduce/WordNumCount.txt").getPath();
        String inPath = JobConfUtils.HDFS_URL + "/user/wordNumCount/mapReduce";
        String outPath = JobConfUtils.HDFS_URL + "/user/wordNumCount/dedup_out";
        String outFile = outPath + "/part-r-00000";
        JobConf jobConf =JobConfUtils.getJobConf(WordNumCount.class,"ItemCFHadoop");
        HDFSUtils hdfs = new HDFSUtils(JobConfUtils.HDFS_URL, jobConf);
        hdfs.rmr(inPath);
        hdfs.rmr(outPath);
        hdfs.mkdirs(inPath);
        hdfs.copyFile(wordNumCountFile, inPath);
        hdfs.ls(inPath);


        Configuration conf = new Configuration();
        //这句话很关键
        conf.set("mapred.job.tracker", JobConfUtils.JOB_TRACKER_URL);
        conf.set("mapred.jar", "D:/joan/workspace/idea/hadoop-family/hadoop-1.1.2/target/hadoop-1.1.2-1.0-SNAPSHOT.jar");

        Job job = new Job(conf, "Data Deduplication");
        job.setJarByClass(WordNumCount.class);

        //设置Map、Combine和Reduce处理类
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        while(job.waitForCompletion(true)){
            hdfs.cat(outFile);
            hdfs.close();
            System.exit(0);
        }
    }
}
/***
 *
 Delete: hdfs://192.168.144.128:9000/user/hdfs/mapReduce
 Delete: hdfs://192.168.144.128:9000/user/hdfs/dedup_out
 Create: hdfs://192.168.144.128:9000/user/hdfs/mapReduce
 copy from: /D:/joan/workspace/idea/hadoop-family/hadoop-1.1.2/target/classes/mapReduce/WordNumCount.txt to hdfs://192.168.144.128:9000/user/hdfs/mapReduce
 ls: hdfs://192.168.144.128:9000/user/hdfs/mapReduce
 ==========================================================
 name: hdfs://192.168.144.128:9000/user/hdfs/mapReduce/WordNumCount.txt, folder: false, size: 27
 ==========================================================

 16/07/02 16:44:42 INFO mapred.JobClient: Running job: job_201606301535_0022
 16/07/02 16:44:43 INFO mapred.JobClient:  map 0% reduce 0%
 16/07/02 16:44:49 INFO mapred.JobClient:  map 100% reduce 0%
 16/07/02 16:44:57 INFO mapred.JobClient:  map 100% reduce 33%
 16/07/02 16:44:59 INFO mapred.JobClient:  map 100% reduce 100%

 cat: hdfs://192.168.144.128:9000/user/hdfs/dedup_out/part-r-00000
 hadoop	1
 hellow	2
 world	1
 */
