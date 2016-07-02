package mapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.conan.myhadoop.hdfs.HdfsDAO;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by teddy on 2016/7/3.
 * 二次排序，当第一个字段相同时 则对第二个字段进行排序
 */
public class SecondaryIntSort {
    public static final String HDFS = "hdfs://192.168.144.128:9000";

    public static class SecondarySortMapper extends Mapper<LongWritable, Text, TwoInt, IntWritable>{
        final TwoInt k2 = new TwoInt();
        final IntWritable v2 = new IntWritable();
        protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,TwoInt,IntWritable>.Context context) throws java.io.IOException ,InterruptedException {
            final String[] splited = value.toString().split(" ");
            k2.set(Integer.parseInt(splited[0]), Integer.parseInt(splited[1]));
            v2.set(Integer.parseInt(splited[1]));
            context.write(k2, v2);
        }
    }

    //排序后，会产生6个组
    public static class SecondarySortReducer extends Reducer<TwoInt, IntWritable, IntWritable, IntWritable>{
        final IntWritable k3 = new IntWritable();
        protected void reduce(TwoInt key2, java.lang.Iterable<IntWritable> value2s, org.apache.hadoop.mapreduce.Reducer<TwoInt,IntWritable,IntWritable,IntWritable>.Context context) throws java.io.IOException ,InterruptedException {
            k3.set(key2.first);
            for (IntWritable value2 : value2s) {
                context.write(k3, value2);
            }
        }
    }


    public static JobConf config() {
        JobConf conf = new JobConf(ScoreAverage.class);
        conf.setJobName("ItemCFHadoop");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }
    public static void main(String[] args) throws Exception{
        String localfile1 = ScoreAverage.class.getResource("/mapReduce/SecondarySort.txt").getPath();
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
        job.setJarByClass(ScoreAverage.class);

        //设置Map、Combine和Reduce处理类
        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);

        //设置输出类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(TwoInt.class);
        job.setMapOutputValueClass(IntWritable.class);
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
/**为二次排序创建的实体**/
class TwoInt implements WritableComparable<TwoInt> {
    int first;
    int second;

    public void set(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.first);
        out.writeInt(this.second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.first = in.readInt();
        this.second = in.readInt();
    }

    /**
     * 先比较first，如果first相同，再比较second
     * @param o
     * @return
     */

    @Override
    public int compareTo(TwoInt o) {
        if(this.first!=o.first) {
            return this.first-o.first;
        } else {
            return this.second - o.second;
        }
    }
}
/**
 Delete: hdfs://192.168.144.128:9000/user/hdfs/mapReduce
 Delete: hdfs://192.168.144.128:9000/user/hdfs/dedup_out
 Create: hdfs://192.168.144.128:9000/user/hdfs/mapReduce
 copy from: /D:/joan/workspace/idea/hadoop-family/hadoop-1.1.2/target/classes/mapReduce/SecondaryIntSort.txt to hdfs://192.168.144.128:9000/user/hdfs/mapReduce
 ls: hdfs://192.168.144.128:9000/user/hdfs/mapReduce
 ==========================================================
 name: hdfs://192.168.144.128:9000/user/hdfs/mapReduce/SecondaryIntSort.txt, folder: false, size: 28
 ==========================================================

 cat: hdfs://192.168.144.128:9000/user/hdfs/dedup_out/part-r-00000
 1	1
 2	1
 2	2
 3	1
 3	2
 3	3
 **/