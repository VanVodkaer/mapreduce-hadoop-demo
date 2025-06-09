package com.hadoop.wordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义排序比较器
 * 排序规则：
 * 1. 按单词长度降序（长的在前）
 * 2. 长度相同时按出现次数降序（多的在前）
 * 3. 前两个条件相同时按字典序升序
 */
public class WordCountComparator extends WritableComparator {

    public WordCountComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Text word1 = (Text) a;
        Text word2 = (Text) b;

        // 解析单词和计数 (格式: "word:count")
        String[] parts1 = word1.toString().split(":");
        String[] parts2 = word2.toString().split(":");

        String actualWord1 = parts1[0];
        String actualWord2 = parts2[0];
        int count1 = Integer.parseInt(parts1[1]);
        int count2 = Integer.parseInt(parts2[1]);

        // 1. 首先按单词长度降序排序
        int lengthCompare = Integer.compare(actualWord2.length(), actualWord1.length());
        if (lengthCompare != 0) {
            return lengthCompare;
        }

        // 2. 长度相同时，按出现次数降序排序
        int countCompare = Integer.compare(count2, count1);
        if (countCompare != 0) {
            return countCompare;
        }

        // 3. 前两个条件都相同时，按字典序升序排序
        return actualWord1.compareTo(actualWord2);
    }
}