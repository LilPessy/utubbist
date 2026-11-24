package it.bigdata.job1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver1 {
    public static void main(String[] args) throws Exception {
        // args[0] = cartella input CSV, args[1] = cartella output
        Configuration conf = new Configuration();
        // toglie da args le opzioni generiche di Hadoop (-fs, -jt, -D ...) e le mette in conf
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length < 2) {
            System.err.println("Uso: Driver1 <input> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Job1");
        job.setJarByClass(Driver1.class);
        job.setMapperClass(Mapper1.class);
        job.setCombinerClass(Reducer1.class);
        job.setReducerClass(Reducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        Path out = new Path(args[1]);
        FileSystem fs = out.getFileSystem(conf);
        if (fs.exists(out)) fs.delete(out, true);    // cancella l'output vecchio
        FileOutputFormat.setOutputPath(job, out);

        long inizio = System.currentTimeMillis();
        boolean ok = job.waitForCompletion(true);
        long durata = System.currentTimeMillis() - inizio;
        System.out.println("TEMPO_MS " + durata);

        System.exit(ok ? 0 : 1);
    }
}
