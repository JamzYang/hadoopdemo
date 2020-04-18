package com.atg.hadoop.mr.sequencefile;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author yang
 * Date 2020/4/18 23:38
 */
public class WholeFileInputFormat extends FileInputFormat<Text, ByteWritable> {

    @Override
    public RecordReader<Text, ByteWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        WholeRecordReader recordReader = new WholeRecordReader();
        recordReader.initialize(split,context);
        return recordReader;
    }
}
