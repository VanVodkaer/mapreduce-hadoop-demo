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

        // 输入格式：单词:次数:次数 \t 次数
        // 或者：单词:次数 \t 次数
        String[] parts = value.toString().split("\t");
        if (parts.length == 2) {
            String wordWithCount = parts[0].trim(); // "单词:次数:次数" 或 "单词:次数"

            // 处理重复的次数格式（如 "hello:2:2"）
            String[] keyParts = wordWithCount.split(":");
            if (keyParts.length >= 2) {
                String word = keyParts[0];
                int count = Integer.parseInt(keyParts[1]);

                // 重新构造正确格式的key：单词:次数
                outputKey.set(word + ":" + count);
                outputValue.set(count);
                context.write(outputKey, outputValue);
            }
        }
    }
}