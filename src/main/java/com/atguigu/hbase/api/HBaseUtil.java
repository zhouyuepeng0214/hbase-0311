package com.atguigu.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HBaseUtil {

    private static Connection connection = null;

    static {

        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum","hadoop110,hadoop111,hadoop112");
            conf.set("hbase.zookeeper.property.clientPort","2181");
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void createTable(String tableName,String...families) throws IOException {
        Admin admin = connection.getAdmin();

       try {
           if (admin.tableExists(TableName.valueOf(tableName))) {
               System.out.println("table:" + tableName + "exists");
               return;
           }
           HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));

           for (String family : families) {
               HColumnDescriptor familyDesc = new HColumnDescriptor(family);
               desc.addFamily(familyDesc);
           }
           admin.createTable(desc);
       } finally {
           admin.close();
       }

    }

    public static void dropTable (String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        try {
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                System.out.println("table" + tableName + "not exits");
                return;
            }
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        } finally {
            admin.close();
        }
    }

    public static boolean tableExists (String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        try {
            return admin.tableExists(TableName.valueOf(tableName));
        } finally {
            admin.close();
        }
    }

    public static void putCell(String tableName, String rowKey, String family, String column, String value) throws IOException {

        if (!tableExists(tableName)) {
            System.out.println("table not exists!!!");
            return;
        }

        Table table = connection.getTable(TableName.valueOf(tableName));

        try {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));

            table.put(put);
        } finally {
            table.close();
        }
    }

    public static void deleteRow (String tableName,String rowKey) throws IOException {
        if (!tableExists(tableName)) {
            System.out.println("table not exists!!!");
            return;
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        try {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } finally {
            table.close();
        }
    }

    /**
     * 删除一行中的一个列族
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @throws IOException
     */
    public static void deleteFamily (String tableName,String rowKey,String family) throws IOException {
        if (!tableExists(tableName)) {
            System.out.println("table not exists!!!");
            return;
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        try {
            Delete delete = new Delete(Bytes.toBytes(rowKey));

            delete.addFamily(Bytes.toBytes(family));
            table.delete(delete);
        } finally {
            table.close();
        }
    }

    /**
     * 删除一行中的一个列的最新版本
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @throws IOException
     */
    public static void deleteCell (String tableName,String rowKey,String family,String column) throws IOException {
        if (!tableExists(tableName)) {
            System.out.println("table not exists!!!");
            return;
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        try {
            Delete delete = new Delete(Bytes.toBytes(rowKey));

            delete.addColumn(Bytes.toBytes(family),Bytes.toBytes(column));
            table.delete(delete);
        } finally {
            table.close();
        }
    }

    /**
     * 删除一行中的一个列的所有版本
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @throws IOException
     */
    public static void deleteCells (String tableName,String rowKey,String family,String column) throws IOException {
        if (!tableExists(tableName)) {
            System.out.println("table not exists!!!");
            return;
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        try {
            Delete delete = new Delete(Bytes.toBytes(rowKey));

            delete.addColumns(Bytes.toBytes(family),Bytes.toBytes(column));
            table.delete(delete);
        } finally {
            table.close();
        }
    }

    /**
     * 获取一行数据
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getRow(String tableName,String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);

        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
//            cell.getFamilyArray();
//            cell.getValueArray();

            byte[] columnBytes = CellUtil.cloneQualifier(cell);
            String columnStr = Bytes.toString(columnBytes);

            byte[] valueBytes = CellUtil.cloneValue(cell);
            String valueStr = Bytes.toString(valueBytes);

            System.out.println(columnStr + ":" + valueStr);
        }
        table.close();

    }

    /**
     *利用过滤器获取多行数据——一个filter
     *
     * @param tableName
     * @param family
     * @param column
     * @param value
     * @throws IOException
     */
    public static void getRowsByColumn(String tableName,String family,String column,String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value));
        filter.setFilterIfMissing(true);

        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                String columnStr = Bytes.toString(CellUtil.cloneQualifier(cell));
                String valueStr = Bytes.toString(CellUtil.cloneValue(cell));

                System.out.println(rowKey + "-" + columnStr + ":" + valueStr);
            }
        }
        scanner.close();
        table.close();
    }

    /**
     *根据过滤器获取多行数据——多个Filter
     *
     * @param tableName
     * @param family
     * @param map
     * @throws IOException
     */
    public static void getRowsByColumns(String tableName, String family, Map<String, String> map) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();

        //Filter1
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(map.keySet().toArray()[0].toString()),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(map.get(map.keySet().toArray()[0].toString())));
        //当过滤条件中的列不存在时，是否将数据过滤掉，默认不过滤掉
        filter1.setFilterIfMissing(true);

        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(map.keySet().toArray()[1].toString()),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(map.get(map.keySet().toArray()[1].toString())));
        filter2.setFilterIfMissing(true);

        //通过一个FilterList将多个过滤连接在一起，让其产生&或|的关系
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        filterList.addFilter(filter1);
        filterList.addFilter(filter2);

        scan.setFilter(filterList);

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {

            for (Cell cell : result.rawCells()) {

                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                String columnStr = Bytes.toString(CellUtil.cloneQualifier(cell));
                String valueStr = Bytes.toString(CellUtil.cloneValue(cell));

                System.out.println(rowKey + "-" + columnStr + ":" + valueStr);
            }
        }
        scanner.close();
        table.close();
    }


    /**
     * 根据rowKey范围获取多行数据
     *
     * @param tableName
     * @param startRow
     * @param stopRow
     * @throws IOException
     */
    public static void getRowsByRange(String tableName, String startRow, String stopRow) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan(Bytes.toBytes(startRow),Bytes.toBytes(stopRow));

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                System.out.println(rowKey + "-" + column + ":" + value);

            }
        }
        scanner.close();
        table.close();

    }

    public static void main(String[] args) throws IOException {
//        createTable("test","info","name");
//        dropTable("test");
//        putCell("student","1002","info","name","zs");
//        putCell("student","1002","info","age","18");
//        putCell("student","1002","info","gender","boy");
//        deleteCell("student","1003","info","name");
//        getRow("student","1003");
//        getRowsByRange("student","1002","1003!");
//        getRowsByColumn("student","info","name","aa");
//        HashMap<String, String> map = new HashMap<String, String>();
//        map.put("age","18");
//        map.put("gender","girl");
//        getRowsByColumns("student","info",map);

        Admin admin = connection.getAdmin();

// 自定义算法，产生一系列Hash散列值存储在二维数组中
        byte[][] splitKeys = new byte[3][];
        splitKeys[0] = Bytes.toBytes("1000");
        splitKeys[1] = Bytes.toBytes("2000");
        splitKeys[2] = Bytes.toBytes("3000");
//创建HBaseAdmin实例
//创建HTableDescriptor实例
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf("staff5"));
        HColumnDescriptor info = new HColumnDescriptor(Bytes.toBytes("info"));
        tableDesc.addFamily(info);
//通过HTableDescriptor实例和散列值二维数组创建带有预分区的HBase表
        admin.createTable(tableDesc, splitKeys);
        admin.close();

    }
}