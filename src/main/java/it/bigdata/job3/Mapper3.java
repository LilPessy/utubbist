package it.bigdata.job3;

import it.bigdata.common.CsvUtil;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class Mapper3 extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final Text categoryName = new Text();
    private final LongWritable view = new LongWritable();

    private final Map<String, String> categorie = new HashMap<>();

    @Override
    protected void setup(Context ctx) throws IOException {
        String file = ((FileSplit) ctx.getInputSplit()).getPath().getName();
        String regione = file.substring(0, 2);

        Configuration conf = ctx.getConfiguration();
        Path json = new Path(conf.get("categorie.dir"), regione + "_category_id.json");
        FileSystem fs = json.getFileSystem(conf);

        try (InputStream in = fs.open(json)) {
            JsonNode root = new ObjectMapper().readTree(in);
            for (JsonNode item : root.get("items")) {
                String id = item.get("id").asText();
                String nome = item.get("snippet").get("title").asText();
                categorie.put(id, nome);
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context ctx)
            throws IOException, InterruptedException {

        List<String> f = CsvUtil.parse(value.toString());
        if (f.size() < 10) return;

        String nome = categorie.get(f.get(4).trim());
        if (nome == null) return;                    // id categoria non presente nel JSON (o riga intestazione)

        try {
            view.set(Long.parseLong(f.get(7).trim()));
        } catch (NumberFormatException e) {
            return;                                  // "views" non numerico: intestazione o riga spezzata
        }

        categoryName.set(nome);
        ctx.write(categoryName, view);
    }
}