package com.atguigu.hbase.mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ReadMapper extends TableMapper<ImmutableBytesWritable,Put> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws
            IOException, InterruptedException {
        Put put = new Put(key.copyBytes());
        for (Cell cell : value.rawCells()) {
            if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                put.add(cell);
            }
        }
        context.write(key,put);
    }
}
