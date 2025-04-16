package edu.wold9168;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class HBasePriceSort {

    // Mapper类：直接使用HBase 2.6.2的TableMapper
    public static class PriceMapper extends TableMapper<IntWritable, Text> {
        private static final byte[] CF = Bytes.toBytes("info");
        private static final byte[] PRICE_COL = Bytes.toBytes("price");
        private static final byte[] BOOK_COL = Bytes.toBytes("bookName");

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context)
                throws IOException, InterruptedException {
            byte[] priceBytes = value.getValue(CF, PRICE_COL);
            if (priceBytes != null) {
                int price = Bytes.toInt(priceBytes);
                String bookName = Bytes.toString(value.getValue(CF, BOOK_COL));
                context.write(new IntWritable(price), new Text(bookName));
            }
        }
    }

    // Reducer类：使用HBase 2.6.2的TableReducer
    public static class PriceReducer extends TableReducer<IntWritable, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text bookName : values) {
                String rowKey = String.format("%08d", key.get()) + "_" + bookName.toString();
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("bookName"), Bytes.toBytes(bookName.toString()));
                put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("price"), Bytes.toBytes(key.get()));
                context.write(new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put);
            }
        }
    }

    // 主函数：使用HBase 2.6.2的API配置作业
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "HBasePriceSort");
        job.setJarByClass(HBasePriceSort.class);

        // 配置输入表和Scan对象
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("info"));

        // 多表输入配置（HBase 2.6.2支持直接指定表名）
        TableMapReduceUtil.initTableMapperJob(
                "table1,table2", // 输入表名（逗号分隔）
                scan,
                PriceMapper.class,
                IntWritable.class,
                Text.class,
                job);

        // 配置输出表
        TableMapReduceUtil.initTableReducerJob(
                "sorted_prices",
                PriceReducer.class,
                job);

        // 提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}