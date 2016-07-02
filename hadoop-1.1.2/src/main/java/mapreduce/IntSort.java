package mapreduce;

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

import java.io.IOException;

/**
 * Created by teddy on 2016/7/2.
 * hadoop 例子 数字排序
 */
public class IntSort {
    public static final String HDFS = "hdfs://192.168.144.128:9000";
    //map将输入中的value化成IntWritable类型，作为输出的key
    public static class IntSumMap extends Mapper<Object,Text,IntWritable,IntWritable>{
        private static IntWritable data=new IntWritable();
        //实现map函数
        public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
            String line=value.toString();
            data.set(Integer.parseInt(line));
            context.write(data, new IntWritable(1));
        }
}

    //reduce将输入中的key复制到输出数据的key上，
    //然后根据输入的value-list中元素的个数决定key的输出次数
    //用全局linenum来代表key的位次

    public static class IntSumReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
        private static IntWritable linenum = new IntWritable(1);
        //实现reduce函数
        public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
            for(IntWritable val:values){
                context.write(linenum, key);
                linenum = new IntWritable(linenum.get()+1);
            }
        }
}

    public static JobConf config() {
        JobConf conf = new JobConf(IntSort.class);
        conf.setJobName("ItemCFHadoop");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }
    public static void main(String[] args) throws Exception{
        String localfile1 = IntSort.class.getResource("/dedup_in/IntSort.txt").getPath();
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
        job.setJarByClass(IntSort.class);

        //设置Map、Combine和Reduce处理类
        job.setMapperClass(IntSumMap.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        //设置输出类型
        job.setOutputKeyClass(IntWritable.class);
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
 copy from: /D:/joan/workspace/idea/hadoop-family/hadoop-1.1.2/target/classes/dedup_in/IntSort.txt to hdfs://192.168.144.128:9000/user/hdfs/dedup_in
 ls: hdfs://192.168.144.128:9000/user/hdfs/dedup_in
 ==========================================================
 name: hdfs://192.168.144.128:9000/user/hdfs/dedup_in/IntSort.txt, folder: false, size: 34
 ==========================================================

 16/07/02 22:35:49 INFO mapred.JobClient:  map 0% reduce 0%
 16/07/02 22:35:55 INFO mapred.JobClient:  map 100% reduce 0%
 16/07/02 22:36:04 INFO mapred.JobClient:  map 100% reduce 33%
 16/07/02 22:36:05 INFO mapred.JobClient:  map 100% reduce 100%

 cat: hdfs://192.168.144.128:9000/user/hdfs/dedup_out/part-r-00000
 1	1
 2	2
 3	3
 4	4
 5	5
 6	6
 7	7
 8	8
 9	9
 10	10
 11	11
 12	12
 */
