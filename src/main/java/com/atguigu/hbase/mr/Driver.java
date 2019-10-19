package com.atguigu.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionPartitioner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class Driver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop110,hadoop111,hadoop112");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        Job job = Job.getInstance(conf);

        job.setJarByClass(Driver.class);

        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob("fruit",
                scan,
                ReadMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job);

        job.setNumReduceTasks(100);
        TableMapReduceUtil.initTableReducerJob("fruit_mr", WriteReducer.class, job, HRegionPartitioner.class);

        job.waitForCompletion(true);
    }
}
