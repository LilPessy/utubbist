package it.bigdata.job3;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer3 extends Reducer<Text, LongWritable, Text, DoubleWritable> {

    private final Map<String, Double> cateogry = new HashMap<>();

    @Override
    protected void reduce(Text categoryName , Iterable<LongWritable> views, Context ctx) throws IOException, InterruptedException {
        long somma = 0L;
        long n = 0L;
        for (LongWritable v : views){
            somma += Long.parseLong(String.valueOf(v));
            n++;
        }
        cateogry.put(categoryName.toString(), (double)somma/n);

    }

    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
        Map.Entry<String, Double> best = cateogry.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .orElse(null);

        String nome  = best.getKey();
        double media = best.getValue();

        ctx.write(new Text(nome), new DoubleWritable(media));
    }
}