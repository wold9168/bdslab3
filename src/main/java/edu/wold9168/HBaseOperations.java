package edu.wold9168;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.List;

public class HBaseOperations {

    private static final String ZK_QUORUM = "localhost"; // ZooKeeper地址
    private static final String ZK_PORT = "2181"; // ZooKeeper端口

    // 初始化HBase配置
    private static Configuration getHBaseConfig() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ZK_QUORUM);
        config.set("hbase.zookeeper.property.clientPort", ZK_PORT);
        return config;
    }

    // 任务1：列出所有表名
    public static void listTables() throws IOException {
        try (Connection conn = ConnectionFactory.createConnection(getHBaseConfig());
                Admin admin = conn.getAdmin()) {
            List<TableDescriptor> tables = admin.listTableDescriptors();
            System.out.println("HBase Tables:");
            tables.forEach(table -> System.out.println(" - " + table.getTableName()));
        }
    }

    // 任务2：打印指定表的所有记录
    public static void scanTable(String tableName) throws IOException {
        try (Connection conn = ConnectionFactory.createConnection(getHBaseConfig());
                Table table = conn.getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                System.out.println("[wold9168]Row: " + Bytes.toString(result.getRow()));
                for (Cell cell : result.rawCells()) {
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.printf("  Column: %s:%s, Value: %s%n", family, qualifier, value);
                }
            }
        }
    }

    // 任务3：添加列族
    public static void addColumnFamily(String tableName, String columnFamily) throws IOException {
        try (Connection conn = ConnectionFactory.createConnection(getHBaseConfig());
                Admin admin = conn.getAdmin()) {
            TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(tableName));
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableDesc);
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily));
            admin.modifyTable(builder.build());
            System.out.println("[wold9168]cf " + columnFamily + " add success");
        }
    }

    // 任务3：删除列族
    public static void deleteColumnFamily(String tableName, String columnFamily) throws IOException {
        try (Connection conn = ConnectionFactory.createConnection(getHBaseConfig());
                Admin admin = conn.getAdmin()) {
            admin.disableTable(TableName.valueOf(tableName));
            TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(tableName));
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableDesc);
            builder.removeColumnFamily(Bytes.toBytes(columnFamily));
            admin.modifyTable(builder.build());
            admin.enableTable(TableName.valueOf(tableName));
            System.out.println("[wold9168]cf " + columnFamily + " delete success");
        }
    }

    // 任务4：清空表数据
    public static void truncateTable(String tableName) throws IOException {
        try (Connection conn = ConnectionFactory.createConnection(getHBaseConfig());
                Admin admin = conn.getAdmin()) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.truncateTable(TableName.valueOf(tableName), true);
            System.out.println("[wold9168]table " + tableName + " is cleared");
        }
    }

    // 任务5：统计表的行数
    public static long countRows(String tableName) throws IOException {
        try (Connection conn = ConnectionFactory.createConnection(getHBaseConfig());
                Table table = conn.getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan();
            scan.setFilter(new FirstKeyOnlyFilter()); // 优化行数统计
            long rowCount = 0;
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    rowCount++;
                }
            }
            return rowCount;
        }
    }

    // 主方法测试
    public static void main(String[] args) throws IOException {
        String tableName = "test_table";
        String columnFamily = "cf2";

        // 任务1：列出所有表
        listTables();

        // 任务2：打印表数据
        scanTable(tableName);

        // 任务3：添加列族
        addColumnFamily(tableName, columnFamily);
        String deleteColumnFamily = "cf3";
        // 任务3：删除列族
        deleteColumnFamily(tableName, deleteColumnFamily);

        // 任务4：清空表
        truncateTable(tableName);

        // 任务5：统计行数
        String countTableName = "count_table";
        long count = countRows(countTableName);
        System.out.println("[wold9168] " + countTableName + " linecount: " + count);
    }
}