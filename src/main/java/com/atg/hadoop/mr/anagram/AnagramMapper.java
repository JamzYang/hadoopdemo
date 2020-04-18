package com.atg.hadoop.mr.anagram;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author yang
 * Date 2020/4/18 0:14
 */
public class AnagramMapper extends Mapper<LongWritable, Text, Text, Text> {
    //TODO 这里的输入key是字节偏移量

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String word = value.toString();
        char[] chars = word.toCharArray();
        Arrays.sort(chars);
        String sortedWord = new String(chars);
        context.write(new Text(sortedWord),value);
    }
}
