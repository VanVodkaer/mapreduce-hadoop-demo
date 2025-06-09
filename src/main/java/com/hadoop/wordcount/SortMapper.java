package com.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 排序阶段的Mapper类
 * 功能：读取第一阶段的输出，为排序做准备
 */
public class SortMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outputKey = new Text();
    private IntWritable outputValue = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 输入格式：单词:次数 \t 次数
        String[] parts = value.toString().split("\t");
        if (parts.length == 2) {
            String wordWithCount = parts[0]; // "单词:次数"
            int count = Integer.parseInt(parts[1]);

            outputKey.set(wordWithCount);
            outputValue.set(count);
            context.write(outputKey, outputValue);
        }
    }
}