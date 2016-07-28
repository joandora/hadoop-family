package crud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by joandora on 2016/7/26.
 */
public class DeleteData {
    public static void deleteRow(String tablename, String rowkey)  {
        try {
            Configuration cfg= HBaseConfiguration.create();
            HTable table = new HTable(cfg,tablename);
            List<Delete> list = new ArrayList<Delete>();
            Delete d1 = new Delete(rowkey.getBytes());
            list.add(d1);
            table.delete(list);
            System.out.println("删除行成功!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        CreateTable.creatTable(CreateTable.tablename,CreateTable.family);
        DeleteData.deleteRow(CreateTable.tablename,"row6");
    }
}
