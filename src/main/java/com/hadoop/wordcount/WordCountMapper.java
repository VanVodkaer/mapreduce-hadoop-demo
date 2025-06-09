package com.hadoop.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * WordCount的Mapper类
 * 功能：将输入文本分割成单词，输出<单词, 1>键值对
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 转换为小写并移除标点符号
        String line = value.toString().toLowerCase().replaceAll("[^a-zA-Z0-9\\s]", "");

        // 分词
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            // 过滤空字符串
            if (word.getLength() > 0) {
                context.write(word, one);
            }
        }
    }
}