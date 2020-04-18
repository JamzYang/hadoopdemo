package com.atg.hadoop.mr.sequencefile;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author yang
 * Date 2020/4/18 22:57
 */
public class SequenceFileReducer extends Reducer<Text, ByteWritable, Text, ByteWritable> {
    @Override
    protected void reduce(Text key, Iterable<ByteWritable> values, Context context) throws IOException, InterruptedException {
        for (ByteWritable value : values) {
            context.write(key,value);
        }
    }
}
