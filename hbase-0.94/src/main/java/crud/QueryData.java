package crud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Joandora on 2016/7/26.
 */
public class QueryData {
    /**
     * 查询所有数据
     */
    public static void queryData (String tablename){
        try {
            Configuration cfg= HBaseConfiguration.create();
            HTable table = null;
            table = new HTable(cfg, tablename);
            Scan s = new Scan();
            ResultScanner rs = table.getScanner(s);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())+ "====值:" + new String(keyValue.getValue()));
                }
            }
            rs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 查询所有数据
     */
    public static void queryDataBatch (String tablename){
        try {
            Configuration cfg= HBaseConfiguration.create();
            HTable table = null;
            table = new HTable(cfg, tablename);

            List<Get> batch= new ArrayList<Get>();
            Get get1=new Get("row1".getBytes());
            Get get2=new Get("row3".getBytes());
            Get get3=new Get("row5".getBytes());

            batch.add(get1);
            batch.add(get2);
            batch.add(get3);

            Result[] results=null;
            results = table.get(batch);
            for (Result r : results) {
                if(!r.isEmpty()) {
                    System.out.println("获得到rowkey:" + new String(r.getRow()));
                    for (KeyValue keyValue : r.raw()) {
                        System.out.println("row:"+new String(keyValue.getRow())+" 列：" + new String(keyValue.getFamily())
                                + "====key:"+new String(keyValue.getQualifier())+"   value:"+ new String(keyValue.getValue()));
                    }
                }
            }
            table.close();
            System.out.println("result size : "+results.length);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        CreateTable.creatTable(CreateTable.tablename,CreateTable.family);
        InsertData.insertDataMulti(CreateTable.tablename);
        QueryData.queryDataBatch(CreateTable.tablename);
//        InsertData.insertData(CreateTable.tablename);
//        QueryData.queryData(CreateTable.tablename);

    }
}
