package com.atg.hadoop.mr.partitionsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yang
 * Date 2020/4/18 16:27
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    private FlowBean flowBean = new FlowBean();
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String mobile = fields[0];
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        flowBean.setSumFlow(upFlow,downFlow);
        k.set(mobile);
        context.write(flowBean, k);
    }
    // 7 13560436666 120.196.100.99 1116 954 200
}
