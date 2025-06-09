package com.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * WordCount排序程序的主驱动类
 * 功能：配置并执行两阶段的MapReduce作业
 */
public class WordCountSortDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: WordCountSort <input path> <temp path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // 第一阶段：WordCount统计
        Job job1 = Job.getInstance(conf, "word count");
        job1.setJarByClass(WordCountSortDriver.class);

        job1.setMapperClass(WordCountMapper.class);
        job1.setReducerClass(WordCountReducer.class);
        job1.setCombinerClass(WordCountReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        // 等待第一个作业完成
        boolean job1Success = job1.waitForCompletion(true);

        if (!job1Success) {
            System.err.println("Job1 (WordCount) failed!");
            System.exit(1);
        }

        // 第二阶段：排序
        Job job2 = Job.getInstance(conf, "word count sort");
        job2.setJarByClass(WordCountSortDriver.class);

        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        // 设置自定义排序比较器
        job2.setSortComparatorClass(WordCountComparator.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        // 等待第二个作业完成并退出
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}