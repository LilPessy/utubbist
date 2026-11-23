package it.bigdata.job1;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> v, Context ctx) throws IOException, InterruptedException {
        ctx.write(key, NullWritable.get());
    }
}