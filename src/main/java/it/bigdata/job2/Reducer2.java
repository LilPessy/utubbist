package it.bigdata.job2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<Text, Text, Text, IntWritable> {

    private final Map<String, Integer> counts = new HashMap<>();

    @Override
    protected void reduce(Text channel, Iterable<Text> video, Context ctx) throws IOException, InterruptedException {
        Set<String> vid = new HashSet<>();
        for(Text id: video){
            vid.add(id.toString());
        }
        counts.put(channel.toString(), vid.size());
    }

    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
        List<Map.Entry<String, Integer>> list = new ArrayList<>(counts.entrySet());
        list.sort((a,b)->{
            if (a.getValue().equals(b.getValue()))      //stesso numero di video?
                return a.getKey().compareTo(b.getKey()); //si: ordine alfabetico del nome
            else
                return b.getValue() - a.getValue();      //no: dal più grande al più piccolo
        });
        for (int i = 0; i < (list.size() > 5 ? 5 : list.size()); i++)
            ctx.write(new Text(list.get(i).getKey()), new IntWritable(list.get(i).getValue()));
    }
}