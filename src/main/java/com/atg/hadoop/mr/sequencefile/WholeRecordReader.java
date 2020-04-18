package com.atg.hadoop.mr.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


import java.io.IOException;

/**
 * @author yang
 * Date 2020/4/18 23:40
 */
public class WholeRecordReader extends RecordReader<Text, ByteWritable> {
    private FileSplit split;
    private Configuration conf ;
    private Text k = new Text();
    private BytesWritable v = new BytesWritable();
    private boolean isProgress = true;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit)split;
        this.conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(isProgress){
            byte[] buf = new byte[(int) split.getLength()];
            Path path = ((FileSplit) split).getPath();
            FileSystem fs = path.getFileSystem(conf);
            FSDataInputStream fis = fs.open(path);
            IOUtils.readFully(fis,buf,0,buf.length);
            v.set(buf,0,buf.length);
            k.set(path.toString());
            IOUtils.closeStream(fis);
            isProgress = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public ByteWritable getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
