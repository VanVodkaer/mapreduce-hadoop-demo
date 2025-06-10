package com.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 排序阶段的Reducer类
 * 功能：输出排序后的结果
 */
public class SortReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        // 解析单词和计数
        String[] parts = key.toString().split(":");
        if (parts.length == 2) {
            String word = parts[0];
            int count = Integer.parseInt(parts[1]);

            // 输出最终结果：单词 \t 次数
            context.write(new Text(word), new IntWritable(count));
        } else {
            // 如果解析失败，直接输出原始数据进行调试
            context.write(key, new IntWritable(0));
        }
    }
}