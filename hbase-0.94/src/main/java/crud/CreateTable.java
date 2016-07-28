package crud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Created by joandora on 2016/7/26.
 */
public class CreateTable {
    public static final String tablename="hbNewStatistics";
    public static final String family = "Family";
    public static final String[] familyAry = new String[]{"Family1","Family2","Family3"};
    /**
     * 创建一张表
     */
    public static void creatTable(String tablename,String family){
        String[] familyAry = new String[1];
        familyAry[0] = family;
        creatTable(tablename,familyAry);
    }

    /**
     * 创建一张表
     */
    public static void creatTable(String tablename,String[] familyAry){
        try {
            Configuration cfg= HBaseConfiguration.create();
            HBaseAdmin hbaseAdmin = new HBaseAdmin(cfg);
            if (hbaseAdmin.tableExists(tablename)) {
                DropTable.dropTable(tablename);
                System.out.println(tablename + " is exist,detele....");
            }

            HTableDescriptor tableDesc = new HTableDescriptor(tablename);
            for(String family : familyAry){
                tableDesc.addFamily(new HColumnDescriptor(family));
            }
            hbaseAdmin.createTable(tableDesc);
            System.out.println("create table ok .");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        CreateTable.creatTable(tablename,family);
        CreateTable.creatTable(tablename,familyAry);
    }
}
