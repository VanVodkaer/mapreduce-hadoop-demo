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
 * WordCount排序程序的主驱动类 - 简化版
 * 使用默认Hadoop配置，避免手动设置连接参数
 */
public class WordCountSortDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 4) {
            System.err.println(
                    "Usage: hadoop jar <jar-file> com.hadoop.wordcount.WordCountSortDriver <input path> <temp path> <output path>");
            System.exit(-1);
        }

        String inputPath = args[1].trim();
        String tempPath = args[2].trim();
        String outputPath = args[3].trim();

        System.out.println("Input path: " + inputPath);
        System.out.println("Temp path: " + tempPath);
        System.out.println("Output path: " + outputPath);

        // 使用正确的HDFS配置 - 从core-site.xml可以看出NameNode在9820端口
        Configuration conf = new Configuration();
        // 虽然配置文件中已经设置了，但为了确保连接正确，显式设置
        conf.set("fs.defaultFS", "hdfs://hadoop81:9820");

        System.out.println("Starting Job 1: Word Count");
        Job job1 = Job.getInstance(conf, "word count");
        job1.setJarByClass(WordCountSortDriver.class);

        job1.setMapperClass(WordCountMapper.class);
        job1.setReducerClass(WordCountReducer.class);
        job1.setCombinerClass(WordCountReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(tempPath));

        // 等待第一个作业完成
        boolean job1Success = job1.waitForCompletion(true);

        if (!job1Success) {
            System.err.println("Job1 (WordCount) failed!");
            System.exit(1);
        }

        System.out.println("Job 1 completed successfully!");

        // 第二阶段：排序
        System.out.println("Starting Job 2: Sort");
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

        FileInputFormat.addInputPath(job2, new Path(tempPath));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));

        // 等待第二个作业完成并退出
        boolean job2Success = job2.waitForCompletion(true);

        if (job2Success) {
            System.out.println("Job 2 completed successfully!");
            System.out.println("All jobs completed. Results are in: " + outputPath);
        } else {
            System.err.println("Job2 (Sort) failed!");
        }

        System.exit(job2Success ? 0 : 1);
    }
}