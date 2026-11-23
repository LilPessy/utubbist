package it.bigdata.job1;

import it.bigdata.common.CsvUtil;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, NullWritable> {
    private final Text title = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException{

        List<String> f = CsvUtil.parse(value.toString());

        if(f.size()<10){
            return;
        }

        try {
            long like = Long.parseLong(f.get(8).trim());
            long dislike = Long.parseLong(f.get(9).trim());
            if (dislike > like) {
                title.set(f.get(2));
                ctx.write(title, NullWritable.get());
            }

        }catch (RuntimeException | InterruptedException e){
            e.printStackTrace(System.out);
        }
    }
}