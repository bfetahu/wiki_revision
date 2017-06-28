package mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by besnik on 5/19/17.
 */
public class WikiStats extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            ToolRunner.run(new WikiStats(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        int num_reducers = 10;

        String data_dir = "", out_dir = "";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-data_dir")) {
                data_dir = args[++i];
            } else if (args[i].equals("-out_dir")) {
                out_dir = args[++i];
            } else if (args[i].equals("-reducers")) {
                num_reducers = Integer.valueOf(args[++i]);
            }
        }
        Job job = new Job(conf);
        long milliSeconds = 10000 * 60 * 60;
        conf.setLong("mapred.task.timeout", milliSeconds);
        job.setNumReduceTasks(num_reducers);

        job.setJarByClass(WikiStats.class);
        job.setJobName(WikiStats.class.getName() + "-" + System.currentTimeMillis());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job, data_dir);

        Path outPath = new Path(out_dir);
        FileOutputFormat.setOutputPath(job, outPath);
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        outPath.getFileSystem(conf).delete(outPath, true);

        job.waitForCompletion(true);
        return 0;
    }

    /**
     * Reduces the output from the mappers which measures the frequency of a type assigned to resources in BTC.
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iter = values.iterator();

            double counter = 0.0;
            int total_sections = 0, total_citations = 0;
            while (iter.hasNext()) {
                String[] data = iter.next().toString().split("\t");
                total_sections += Integer.valueOf(data[0]);
                total_citations += Integer.valueOf(data[1]);
                counter += 1;
            }

            total_citations /= counter;
            total_sections /= counter;

            context.write(key, new Text(new StringBuffer().append(total_sections).append("\t").append(total_citations).toString()));
        }
    }

    /**
     * Read entity revisions into the WikipediaEntityRevision class.
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String wiki_text = value.toString();
            wiki_text = wiki_text.substring(wiki_text.indexOf("\t")).trim();
            try {

                //extract the entity title and the year of the revision
                JSONObject entity_json = new JSONObject(wiki_text);

                String timestamp = entity_json.getString("timestamp");
                String year = timestamp.substring(0, timestamp.indexOf("-"));

                String entity = entity_json.getString("title");

                JSONArray sections = entity_json.getJSONArray("sections");
                int num_sections = sections.length();

                int num_citations = 0;
                if (num_sections != 0) {
                    for (int k = 0; k < sections.length(); k++) {
                        JSONObject section = sections.getJSONObject(k);
                        num_citations += section.getJSONArray("citations").length();
                    }
                }

                Text text = new Text();
                text.set(new StringBuffer().append(num_sections).append("\t").append(num_citations).toString());
                context.write(new Text(entity + "\t" + year), text);
            } catch (Exception e) {
                System.out.println(wiki_text);
            }
        }
    }
}
