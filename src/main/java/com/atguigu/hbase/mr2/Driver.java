package com.atguigu.hbase.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionPartitioner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class Driver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop110,hadoop111,hadoop112");
        Job job = Job.getInstance(conf);

        job.setJarByClass(Driver.class);

        job.setMapperClass(ReadMapper.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        FileInputFormat.setInputPaths(job,new Path("hdfs://hadoop110:9000/input_fruit/fruit.tsv"));

        job.setNumReduceTasks(10);

        TableMapReduceUtil.initTableReducerJob("fruit_mr",WriteReducer.class,job, HRegionPartitioner.class);

        job.waitForCompletion(true);
    }
}
