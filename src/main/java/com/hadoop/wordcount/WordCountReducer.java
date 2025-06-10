package com.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCount的Reducer类
 * 功能：统计每个单词的出现次数，输出格式为"单词:次数"
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        // 累加每个单词的出现次数
        for (IntWritable value : values) {
            sum += value.get();
        }

        result.set(sum);

        // 输出格式：单词:次数 作为key，1作为value（因为每个单词只输出一次）
        Text outputKey = new Text(key.toString() + ":" + sum);
        context.write(outputKey, new IntWritable(1));
    }
}