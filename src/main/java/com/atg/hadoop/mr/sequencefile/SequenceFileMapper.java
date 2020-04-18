package com.atg.hadoop.mr.sequencefile;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yang
 * Date 2020/4/18 22:56
 */
public class SequenceFileMapper extends Mapper<Text, ByteWritable, Text, ByteWritable> {
    @Override
    protected void map(Text key, ByteWritable value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
