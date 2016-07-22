package com.joandora.hadoop.mapReduce;

import com.joandora.hadoop.hdfs.HDFSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by teddy on 2016/7/3.
 */
public class PartitionerDemo {
    public static final String HDFS = "hdfs://192.168.144.128:9000";

    public static class SecondarySortMapper extends Mapper<LongWritable, Text, IntPair, IntWritable> {
        final IntPair k2 = new IntPair();
        final IntWritable v2 = new IntWritable();
        protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,IntPair,IntWritable>.Context context) throws java.io.IOException ,InterruptedException {
            final String[] splited = value.toString().split(" ");
            k2.set(Integer.parseInt(splited[0]), Integer.parseInt(splited[1]));
            v2.set(Integer.parseInt(splited[1]));
            context.write(k2, v2);
        }
    }

    //排序后，会产生6个组
    public static class SecondarySortReducer extends Reducer<IntPair, IntWritable, IntWritable, IntWritable> {
        final IntWritable k3 = new IntWritable();
        protected void reduce(IntPair key2, java.lang.Iterable<IntWritable> value2s, org.apache.hadoop.mapreduce.Reducer<IntPair,IntWritable,IntWritable,IntWritable>.Context context) throws java.io.IOException ,InterruptedException {
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
        String localfile1 = PartitionerDemo.class.getResource("/mapReduce/SecondaryIntSort.txt").getPath();
        String inPath = HDFS + "/user/hdfs/mapReduce";
        String outPath = HDFS + "/user/hdfs/dedup_out";
        String outFile = outPath + "/part-r-00000";
        JobConf jobConf = config();
        HDFSUtils hdfs = new HDFSUtils(HDFS, jobConf);
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
        job.setPartitionerClass(FirstPartitioner.class);
        job.setReducerClass(SecondarySortReducer.class);

        //设置输出类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(IntPair.class);
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
    /**
     * <pre>
     * Partitioner的作用就是决定选择哪一个reducer
     * 但是对于key是对象来说，每一个key的hashcode都有可能会不一样，这样就会造成相同的key落到不同的reducer
     * 在这个例子中key对象为IntPair，其实尅通过重写IntPair的hashcode和equals方法既可以
     * Partitioner只是有自己的路由规则，进行自定义
     * </pre>
     */
    public static class FirstPartitioner extends Partitioner<IntPair,IntWritable> {
        @Override
        public int getPartition(IntPair key, IntWritable value, int numPartitions) {
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }
    }
    /**
     * Define a pair of integers that are writable.
     * They are serialized in a byte comparable format.
     */
    public static class IntPair implements WritableComparable<IntPair> {
        private int first = 0;
        private int second = 0;

        /**
         * Set the left and right values.
         */
        public void set(int left, int right) {
            first = left;
            second = right;
        }
        public int getFirst() {
            return first;
        }
        public int getSecond() {
            return second;
        }
        /**
         * Read the two integers.
         * Encoded as: MIN_VALUE -> 0, 0 -> -MIN_VALUE, MAX_VALUE-> -1
         */
        @Override
        public void readFields(DataInput in) throws IOException {
            first = in.readInt() + Integer.MIN_VALUE;
            second = in.readInt() + Integer.MIN_VALUE;
        }
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(first - Integer.MIN_VALUE);
            out.writeInt(second - Integer.MIN_VALUE);
        }
        @Override
        public int hashCode() {
            return first * 157 + second;
        }
        @Override
        public boolean equals(Object right) {
            if (right instanceof IntPair) {
                IntPair r = (IntPair) right;
                return r.first == first && r.second == second;
            } else {
                return false;
            }
        }

        /** A Comparator that compares serialized IntPair. */
        public static class Comparator extends WritableComparator {
            public Comparator() {
                super(IntPair.class);
            }

            public int compare(byte[] b1, int s1, int l1,
                               byte[] b2, int s2, int l2) {
                return compareBytes(b1, s1, l1, b2, s2, l2);
            }
        }

        static {                                        // register this comparator
            WritableComparator.define(IntPair.class, new Comparator());
        }

        @Override
        public int compareTo(IntPair o) {
            if (first != o.first) {
                return first < o.first ? -1 : 1;
            } else if (second != o.second) {
                return second < o.second ? -1 : 1;
            } else {
                return 0;
            }
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
