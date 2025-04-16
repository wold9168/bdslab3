package edu.wold9168;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseOperations2 {

    public static void createTable(String tableName, String[] families) throws IOException {
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
            }
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            for (String family : families) {
                tableDesc.addFamily(new HColumnDescriptor(family));
            }
            admin.createTable(tableDesc);
            System.out.println("Table " + tableName + " created successfully.");
        }
    }

    public static void addRecord(String tableName, String rowKey, String[] columns, String[] values) throws IOException {
        if (columns.length != values.length) {
            throw new IllegalArgumentException("Columns and values must have the same length.");
        }
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));
            for (int i = 0; i < columns.length; i++) {
                String[] parts = columns[i].split(":");
                String family = parts[0];
                String qualifier = (parts.length > 1) ? parts[1] : null;
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(values[i]));
            }
            table.put(put);
        }
    }

    public static void scanColumn(String tableName, String column) throws IOException {
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan();
            if (column.contains(":")) {
                String[] parts = column.split(":");
                scan.addColumn(Bytes.toBytes(parts[0]), Bytes.toBytes(parts[1]));
            } else {
                scan.addFamily(Bytes.toBytes(column));
            }
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    System.out.println("Row: " + Bytes.toString(result.getRow()) + 
                            " | Column: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                            " | Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        }
    }

    public static void modifyData(String tableName, String rowKey, String column, String newValue) throws IOException {
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            String[] parts = column.split(":");
            String family = parts[0];
            String qualifier = (parts.length > 1) ? parts[1] : null;
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(newValue));
            table.put(put);
        }
    }

    public static void deleteRow(String tableName, String rowKey) throws IOException {
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        }
    }

    public static void main(String[] args) {
        try {
            // 创建学生表
            String[] studentFamilies = {"info", "score"};
            createTable("student", studentFamilies);

            // 插入学生数据
            addRecord("student", "2015001", 
                new String[]{"info:S_Name", "info:S_Sex", "info:S_Age"},
                new String[]{"Zhangsan", "male", "23"});
            addRecord("student", "2015002", 
                new String[]{"info:S_Name", "info:S_Sex", "info:S_Age"},
                new String[]{"Mary", "female", "22"});
            addRecord("student", "2015003", 
                new String[]{"info:S_Name", "info:S_Sex", "info:S_Age"},
                new String[]{"Lisi", "male", "24"});

            // 创建课程表
            String[] courseFamilies = {"course_info"};
            createTable("course", courseFamilies);
            addRecord("course", "123001", 
                new String[]{"course_info:C_Name", "course_info:C_Credit"},
                new String[]{"Math", "2.0"});
            addRecord("course", "123002", 
                new String[]{"course_info:C_Name", "course_info:C_Credit"},
                new String[]{"Computer Science", "5.0"});
            addRecord("course", "123003", 
                new String[]{"course_info:C_Name", "course_info:C_Credit"},
                new String[]{"English", "3.0"});

            // 创建选课表
            String[] scFamilies = {"score"};
            createTable("sc", scFamilies);
            addRecord("sc", "2015001", 
                new String[]{"score:123001", "score:123003"},
                new String[]{"86", "69"});
            addRecord("sc", "2015002", 
                new String[]{"score:123002", "score:123003"},
                new String[]{"77", "99"});
            addRecord("sc", "2015003", 
                new String[]{"score:123001", "score:123002"},
                new String[]{"98", "95"});

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}