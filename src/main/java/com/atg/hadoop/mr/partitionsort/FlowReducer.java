package com.atg.hadoop.mr.partitionsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author yang
 * Date 2020/4/18 16:47
 */
public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
