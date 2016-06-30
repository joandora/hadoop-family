package org.conan.mymahout.recommendation;

import org.apache.hadoop.mapred.JobConf;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import org.conan.mymahout.hdfs.HdfsDAO;
/**此次试验hadoop版本：1.1.2**/
public class ItemCFHadoop {

    private static final String HDFS = "hdfs://192.168.144.128:9000";
    
    public static void main(String[] args) throws Exception {
        String localFile = ItemCFHadoop.class.getResource("/datafile/item.csv").getPath();
        String inPath = HDFS + "/user/hdfs/userCF";
        String inFile = inPath + "/item.csv";
        String outPath = HDFS + "/user/hdfs/userCF/result/";
        String outFile = outPath + "/part-r-00000";
        String tmpPath = HDFS + "/tmp/" + System.currentTimeMillis();

        JobConf conf = config();
        HdfsDAO hdfs = new HdfsDAO(HDFS, conf);
        hdfs.rmr(inPath);
        hdfs.mkdirs(inPath);
        hdfs.copyFile(localFile, inPath);
        hdfs.ls(inPath);
        hdfs.cat(inFile);

        StringBuilder sb = new StringBuilder();
        sb.append("--input ").append(inPath);
        sb.append(" --output ").append(outPath);
        sb.append(" --booleanData true");
        sb.append(" --similarityClassname org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.EuclideanDistanceSimilarity");
        sb.append(" --tempDir ").append(tmpPath);
        args = sb.toString().split(" ");

        RecommenderJob job = new RecommenderJob();
        job.setConf(conf);
        job.run(args);

        hdfs.cat(outFile);
    }

    public static JobConf config() {
        JobConf conf = new JobConf(ItemCFHadoop.class);
        conf.setJobName("ItemCFHadoop");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }
    /**
     *
     Delete: hdfs://192.168.144.128:9000/user/hdfs/userCF
     Create: hdfs://192.168.144.128:9000/user/hdfs/userCF
     copy from: /D:/joan/workspace/idea/hadoop-family/mahout-0.8/target/classes/datafile/item.csv to hdfs://192.168.144.128:9000/user/hdfs/userCF
     ls: hdfs://192.168.144.128:9000/user/hdfs/userCF
     ==========================================================
     name: hdfs://192.168.144.128:9000/user/hdfs/userCF/item.csv, folder: false, size: 209
     ==========================================================
     cat: hdfs://192.168.144.128:9000/user/hdfs/userCF/item.csv
     1,101,5.0
     1,102,3.0
     1,103,2.5
     2,101,2.0
     2,102,2.5
     2,103,5.0
     2,104,2.0
     3,101,2.5
     3,104,4.0
     3,105,4.5
     3,107,5.0
     4,101,5.0
     4,103,3.0
     4,104,4.5
     4,106,4.0
     5,101,4.0
     5,102,3.0
     5,103,2.0
     5,104,4.0
     5,105,3.5
     5,106,4.0

     cat: hdfs://192.168.144.128:9000/user/hdfs/userCF/result//part-r-00000
     1	[104:1.280239,106:1.1462644,105:1.0653841,107:0.33333334]
     2	[106:1.560478,105:1.4795978,107:0.69935876]
     3	[103:1.2475469,106:1.1944525,102:1.1462644]
     4	[102:1.6462644,105:1.5277859,107:0.69935876]
     5	[107:1.1993587]
     */
}
