package mapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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
 */
public class GroupingComparator {
    public static final String HDFS = "hdfs://192.168.144.128:9000";

    public static class GroupingComparatorMapper extends Mapper<LongWritable, Text, SortAPI, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
            String[] splied = value.toString().split(" ");
            try {
                long first = Long.parseLong(splied[0]);
                long second = Long.parseLong(splied[1]);
                context.write(new SortAPI(first,second), new LongWritable(second));
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public static class GroupingComparatorReduce extends Reducer<SortAPI, LongWritable, LongWritable, LongWritable> {
        @Override
        protected void reduce(SortAPI key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(0), null);
            for (LongWritable value : values) {
                context.write(new LongWritable(key.first), value);
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
        String localfile1 = GroupingComparator.class.getResource("/mapReduce/GroupingComparator.txt").getPath();
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
        job.setMapperClass(GroupingComparatorMapper.class);
        job.setReducerClass(GroupingComparatorReduce.class);
        job.setGroupingComparatorClass(GroupingComparator2.class);

        //设置输出类型
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(SortAPI.class);
        job.setMapOutputValueClass(LongWritable.class);
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
    public static class GroupingComparator2 implements RawComparator<SortAPI> {
        @Override
        public int compare(SortAPI o1, SortAPI o2) {
            return (int)(o1.first - o2.first);
        }
        /**
         * b1    第一个参与比较的字节数组
         * s1    参与比较的起始字节位置
         * l1    参与比较的字节长度
         * b2    第二个参与比较的字节数组
         * s2    参与比较的起始字节位置
         * l2    参与比较的字节长度
         */
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int compareBytes = WritableComparator.compareBytes(b1, s1, 8, b2, s2, 8);
            return compareBytes;
        }
    }

    public static class SortAPI implements WritableComparable<SortAPI> {
        public Long first;
        public Long second;
        public SortAPI(){

        }
        public SortAPI(long first,long second){
            this.first = first;
            this.second = second;
        }

        @Override
        public int compareTo(SortAPI o) {
            long mis = (this.first - o.first);
            if(mis != 0 ){
                return (int)mis;
            }
            else{
                return (int)(this.second - o.second);
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(first);
            out.writeLong(second);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.first = in.readLong();
            this.second = in.readLong();

        }

        @Override
        public int hashCode() {
            return this.first.hashCode() + this.second.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof SortAPI){
                SortAPI o = (SortAPI)obj;
                return this.first == o.first && this.second == o.second;
            }
            return false;
        }

        @Override
        public String toString() {
            return "输出：" + this.first + ";" + this.second;
        }
        /** A Comparator that compares serialized IntPair. */
        public static class Comparator extends WritableComparator {
            public Comparator() {
                super(SortAPI.class);
            }

            public int compare(byte[] b1, int s1, int l1,
                               byte[] b2, int s2, int l2) {
                return compareBytes(b1, s1, l1, b2, s2, l2);
            }
        }

        static {                                        // register this comparator
            WritableComparator.define(SortAPI.class, new Comparator());
        }
    }
}
/**
 Delete: hdfs://192.168.144.128:9000/user/hdfs/mapReduce
 Delete: hdfs://192.168.144.128:9000/user/hdfs/dedup_out
 Create: hdfs://192.168.144.128:9000/user/hdfs/mapReduce
 copy from: /D:/joan/workspace/idea/hadoop-family/hadoop-1.1.2/target/classes/mapReduce/GroupingComparator.txt to hdfs://192.168.144.128:9000/user/hdfs/mapReduce
 ls: hdfs://192.168.144.128:9000/user/hdfs/mapReduce
 ==========================================================
 name: hdfs://192.168.144.128:9000/user/hdfs/mapReduce/GroupingComparator.txt, folder: false, size: 28
 ==========================================================
 *
 * 设置了GroupingComparator
 cat: hdfs://192.168.144.128:9000/user/hdfs/dedup_out/part-r-00000
 0
 1	1
 1	2
 1	2
 0
 2	2
 0
 3	0
 3	2
 * 没有设置GroupingComparator，分组规则为SortAPI的compareTo
 cat: hdfs://192.168.144.128:9000/user/hdfs/dedup_out/part-r-00000
 0
 1	1
 0
 1	2
 1	2
 0
 2	2
 0
 3	0
 0
 3	2
 **/

