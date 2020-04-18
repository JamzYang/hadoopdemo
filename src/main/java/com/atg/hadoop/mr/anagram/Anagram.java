package com.atg.hadoop.mr.anagram;


import com.atg.hadoop.mr.wordcount.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author yang
 * Date 2020/4/17 23:56
 */
public class Anagram {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();
        Path path = new Path("anagram");
        FileSystem fs = path.getFileSystem(config);
        if(fs.isDirectory(path)){
            fs.delete(path,true);
        }

        Job job = Job.getInstance();
        job.setJarByClass(Anagram.class);
        job.setMapperClass(AnagramMapper.class);
        job.setReducerClass(AnagramReducer.class);
        job.setCombinerClass(AnagramReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("words.txt"));
        FileOutputFormat.setOutputPath(job, path);
        job.waitForCompletion(true);

    }

}
