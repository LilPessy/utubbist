package it.bigdata.job3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver3 {
    public static void main(String[] args) throws Exception {
        // args[0] = input CSV, args[1] = cartella output, args[2] = cartella dei JSON
        Configuration conf = new Configuration();
        // toglie da args le opzioni generiche di Hadoop (-fs, -jt, -D ...) e le mette in conf
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length < 3) {
            System.err.println("Uso: Driver3 <input> <output> <cartella json>");
            System.exit(2);
        }
        conf.set("categorie.dir", args[2]);          // letto dal Mapper3 nel setup()

        Job job = Job.getInstance(conf, "Job3");
        job.setJarByClass(Driver3.class);

        job.setMapperClass(Mapper3.class);
        job.setReducerClass(Reducer3.class);
        // niente combiner: il reducer calcola una media e cambia i tipi

        job.setNumReduceTasks(1);                    // un solo reducer = massimo globale

        job.setMapOutputKeyClass(Text.class);        // (categoria, views)
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);           // (categoria, media)
        job.setOutputValueClass(DoubleWritable.class);

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