package com.atg.hadoop.mr.anagram;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author yang
 * Date 2020/4/18 0:44
 */
public  class AnagramReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String result = "";
        for (Text value : values) {
            result+= value.toString()+",";
        }
        context.write(key,new Text(result));
    }
}
