package it.bigdata.job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver2 {
    public static void main(String[] args) throws Exception {
        // args[0] = cartella input CSV, args[1] = cartella output
        Configuration conf = new Configuration();
        // toglie da args le opzioni generiche di Hadoop (-fs, -jt, -D ...) e le mette in conf
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length < 2) {
            System.err.println("Uso: Driver2 <input> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Job2");
        job.setJarByClass(Driver2.class);

        job.setMapperClass(Mapper2.class);
        job.setReducerClass(Reducer2.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        Path out = new Path(args[1]);
        FileSystem fs = out.getFileSystem(conf);
        if (fs.exists(out)) {
            // Cancello solo una CARTELLA di output vecchia: se out è un file
            // (es. un CSV di input passato per sbaglio come output) mi fermo.
            if (!fs.getFileStatus(out).isDirectory())
                throw new IllegalArgumentException("L'output " + out + " è un file, non lo cancello: controlla l'ordine degli argomenti");
            fs.delete(out, true);                        // cancella l'output vecchio
        }
        FileOutputFormat.setOutputPath(job, out);

        long inizio = System.currentTimeMillis();
        boolean ok = job.waitForCompletion(true);
        long durata = System.currentTimeMillis() - inizio;
        System.out.println("TEMPO_MS " + durata);

        System.exit(ok ? 0 : 1);
    }
}
