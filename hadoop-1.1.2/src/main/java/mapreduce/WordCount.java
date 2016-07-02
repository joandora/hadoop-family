package mapreduce;
import java.io.IOException;
import java.util.StringTokenizer;

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
 * Created by teddy on 2016/7/2.
 * hadoop 例子 wordcount
 */
public class WordCount {
    public static final String HDFS = "hdfs://192.168.144.128:9000";

    public static class  TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        /**计数为1**/
        private final static IntWritable one = new IntWritable(1);
        /**代表一个单词**/
        private Text word = new Text();
        /**按空格分割每个单词，并计数为1**/
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }
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

    public static JobConf config() {
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("ItemCFHadoop");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }
    public static void main(String[] args) throws Exception{
        String localfile1 = WordCount.class.getResource("/dedup_in/WordCount.txt").getPath();
        String inPath = HDFS + "/user/hdfs/dedup_in";
        String outPath = HDFS + "/user/hdfs/dedup_out";
        String outFile = outPath + "/part-r-00000";
        JobConf jobConf = config();
        HdfsDAO hdfs = new HdfsDAO(HDFS, jobConf);
        hdfs.rmr(inPath);
        hdfs.rmr(outPath);
        hdfs.mkdirs(inPath);
        hdfs.copyFile(localfile1, inPath);
        hdfs.ls(inPath);


        Configuration conf = new Configuration();
        //这句话很关键
        conf.set("mapred.job.tracker", "192.168.144.128:9001");
        conf.set("mapred.jar", "D:/joan/workspace/idea/hadoop-family/hadoop-1.1.2/target/hadoop-1.1.2-1.0-SNAPSHOT.jar");

        Job job = new Job(conf, "Data Deduplication");
        job.setJarByClass(WordCount.class);

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
            System.exit(0);
        }
    }
}
/***
 *
 Delete: hdfs://192.168.144.128:9000/user/hdfs/dedup_in
 Delete: hdfs://192.168.144.128:9000/user/hdfs/dedup_out
 Create: hdfs://192.168.144.128:9000/user/hdfs/dedup_in
 copy from: /D:/joan/workspace/idea/hadoop-family/hadoop-1.1.2/target/classes/dedup_in/WordCount.txt to hdfs://192.168.144.128:9000/user/hdfs/dedup_in
 ls: hdfs://192.168.144.128:9000/user/hdfs/dedup_in
 ==========================================================
 name: hdfs://192.168.144.128:9000/user/hdfs/dedup_in/WordCount.txt, folder: false, size: 27
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
