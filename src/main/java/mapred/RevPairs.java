package mapred;

import entities.WikiEntity;
import mapred.io.WikiText;
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
import org.json.JSONObject;
import revisions.RevContentComparison;
import utils.WikiUtils;

import java.io.IOException;


/**
 * Created by besnik on 3/15/17.
 */
public class RevPairs extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RevPairs(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        int num_reducers = 10;
        long milliSeconds = 10000 * 60 * 60;// <default is 600000, likewise can give any value)

        conf.setLong("mapreduce.task.timeout", milliSeconds);
        conf.set("mapreduce.output.compress", "true");
        conf.set("mapreduce.child.java.opts", "8192m");

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

        job.setNumReduceTasks(num_reducers);

        job.setJarByClass(RevPairs.class);
        job.setJobName(RevPairs.class.getName() + "-" + System.currentTimeMillis());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WikiText.class);

        job.setMapperClass(PairMapper.class);
        job.setReducerClass(PairReducer.class);
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
    public static class PairReducer extends Reducer<Text, WikiText, LongWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<WikiText> values, Context context) throws IOException, InterruptedException {
            RevContentComparison rc = new RevContentComparison();
            StringBuffer sb = new StringBuffer();
            wiki.utils.WikiUtils.timeout = 30000;

            //is the initial revision
            WikiEntity prev_revision = null;
            long counter = key.hashCode();
            for (WikiText value : values) {
                sb.delete(0, sb.length());
                WikiEntity current_revision = RevisionPairComparison.parseEntities(value.text, false);
                if (current_revision == null) {
                    continue;
                }
                if (prev_revision == null) {
                    sb.append(rc.printInitialRevision(current_revision));
                } else {
                    sb.append(rc.compareWithOldRevision(current_revision, prev_revision));
                }
                prev_revision = current_revision;

                counter++;
                context.write(new LongWritable(counter), new Text(sb.toString()));
            }
        }
    }


    /**
     * Read entity revisions into the WikipediaEntityRevision class.
     */
    public static class PairMapper extends Mapper<LongWritable, Text, Text, WikiText> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String rev_text = value.toString();
            rev_text = rev_text.substring(rev_text.indexOf("\t")).trim();

            try {
                JSONObject rev_json = new JSONObject(rev_text);
                String timestamp = rev_json.getString("timestamp");
                int start = timestamp.indexOf("-");
                String key_timestamp = rev_json.getString("title") + "-" + timestamp.substring(0, start);

                WikiText wiki = new WikiText();
                wiki.rev_id = rev_json.getLong("id");
                wiki.text = rev_text;
                context.write(new Text(key_timestamp), wiki);
            } catch (Exception e) {
                //for the few revisions where the username breaks the json.
                System.out.println(e.getMessage());
            }
        }
    }
}
