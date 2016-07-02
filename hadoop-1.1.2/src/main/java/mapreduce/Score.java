package mapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.conan.myhadoop.hdfs.HdfsDAO;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by teddy on 2016/7/2.
 * hadoop 例子 算平均分
 */
public class Score {
    public static final String HDFS = "hdfs://192.168.144.128:9000";

    public static class ScoreMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        // 实现map函数
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 将输入的纯文本文件的数据转化成String
            String line = value.toString();
            // 将输入的数据首先按行进行分割
            StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");
            // 分别对每一行进行处理
            while (tokenizerArticle.hasMoreElements()) {
                // 每行按空格划分
                StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
                String strName = tokenizerLine.nextToken();// 学生姓名部分
                String strScore = tokenizerLine.nextToken();// 成绩部分
                Text name = new Text(strName);
                int scoreInt = Integer.parseInt(strScore);
                // 输出姓名和成绩
                context.write(name, new IntWritable(scoreInt));
            }
        }
    }

    public static class ScoreReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        // 实现reduce函数
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                sum += iterator.next().get();// 计算总分
                count++;// 统计总的科目数
            }
            int average = (int) sum / count;// 计算平均成绩
            context.write(key, new IntWritable(average));
        }
    }

    public static JobConf config() {
        JobConf conf = new JobConf(Score.class);
        conf.setJobName("ItemCFHadoop");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }
    public static void main(String[] args) throws Exception{
        String localfile1 = Score.class.getResource("/mapReduce/Score.txt").getPath();
        String inPath = HDFS + "/user/hdfs/mapReduce";
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
        job.setJarByClass(Score.class);

        //设置Map、Combine和Reduce处理类
        job.setMapperClass(ScoreMap.class);
        job.setCombinerClass(ScoreReduce.class);
        job.setReducerClass(ScoreReduce.class);

        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现
        job.setInputFormatClass(TextInputFormat.class);
        // 提供一个RecordWriter的实现，负责数据输出
        job.setOutputFormatClass(TextOutputFormat.class);

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
 Delete: hdfs://192.168.144.128:9000/user/hdfs/mapReduce
 Delete: hdfs://192.168.144.128:9000/user/hdfs/dedup_out
 Create: hdfs://192.168.144.128:9000/user/hdfs/mapReduce
 copy from: /D:/joan/workspace/idea/hadoop-family/hadoop-1.1.2/target/classes/mapReduce/Score.txt to hdfs://192.168.144.128:9000/user/hdfs/mapReduce
 ls: hdfs://192.168.144.128:9000/user/hdfs/mapReduce
 ==========================================================
 name: hdfs://192.168.144.128:9000/user/hdfs/mapReduce/WordCount.txt, folder: false, size: 27
 ==========================================================

 16/07/02 16:44:42 INFO mapred.JobClient: Running job: job_201606301535_0022
 16/07/02 16:44:43 INFO mapred.JobClient:  map 0% reduce 0%
 16/07/02 16:44:49 INFO mapred.JobClient:  map 100% reduce 0%
 16/07/02 16:44:57 INFO mapred.JobClient:  map 100% reduce 33%
 16/07/02 16:44:59 INFO mapred.JobClient:  map 100% reduce 100%

 cat: hdfs://192.168.144.128:9000/user/hdfs/dedup_out/part-r-00000
 张三	82
 李四	90
 王五	82
 赵六	76
 */
