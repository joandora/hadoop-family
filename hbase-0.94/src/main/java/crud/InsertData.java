package crud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by joandora on 2016/7/26.
 */
public class InsertData {
    static long count = 0;
    /**新增数据
     * 使用table.put(putRow);进行数据插入
     * **/
    public static void insertData (String tablename){
        try {
            Configuration cfg= HBaseConfiguration.create();
            HTable table = new HTable(cfg, tablename);

            // 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
            Put putRow = new Put(("row"+count++).getBytes());
            putRow.add(CreateTable.family.getBytes(), "col1".getBytes(), "vaule1".getBytes());  //set the name of column and value.
            putRow.add(CreateTable.family.getBytes(), "col2".getBytes(), "vaule2".getBytes());
            putRow.add(CreateTable.family.getBytes(), "col3".getBytes(), "vaule3".getBytes());
            table.put(putRow);
            System.out.println("insert data ok .");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**新增数据**/
    public static void insertDataMulti (String tablename){
        try {
            Configuration cfg= HBaseConfiguration.create();
            HTable table = new HTable(cfg, tablename);
            for(int i=1;i<=6;i++) {
                // 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
                Put putRow = new Put(("row"+i).getBytes());
                putRow.add(CreateTable.familyAry[0].getBytes(), "col1".getBytes(), "vaule1".getBytes());  //set the name of column and value.
                putRow.add(CreateTable.familyAry[1].getBytes(), "col2".getBytes(), "vaule2".getBytes());
                putRow.add(CreateTable.familyAry[2].getBytes(), "col3".getBytes(), "vaule3".getBytes());
                table.put(putRow);
            }
            System.out.println("insert data ok .");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        CreateTable.creatTable(CreateTable.tablename,CreateTable.family);

        while(true){
            InsertData.insertData(CreateTable.tablename);
            System.out.println(count++);
        }

//        InsertData.insertDataMulti(CreateTable.tablename);
    }
}
