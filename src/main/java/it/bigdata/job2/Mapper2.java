package it.bigdata.job2;

import it.bigdata.common.CsvUtil;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
    private final Text channel = new Text();
    private final Text video = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException{

        List<String> f = CsvUtil.parse(value.toString());

        if(f.size()<10){
            return;
        }

        try {
            channel.set(f.get(3));
            video.set(f.get(0));
            ctx.write(channel, video);

        }catch (RuntimeException | InterruptedException e){
            e.printStackTrace(System.out);
        }
    }
}