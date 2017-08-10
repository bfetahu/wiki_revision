package mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;

import java.io.IOException;


/**
 * Created by besnik on 3/15/17.
 */
public class RevisionEntityPairs extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RevisionEntityPairs(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String data_dir = "", out_dir = "";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-data_dir")) {
                data_dir = args[++i];
            } else if (args[i].equals("-out_dir")) {
                out_dir = args[++i];
            }
        }
        Job job = new Job(conf);
        job.setNumReduceTasks(0);

        job.setJarByClass(RevisionEntityPairs.class);
        job.setJobName(RevisionEntityPairs.class.getName() + "-" + System.currentTimeMillis());

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(EntityRevPairMapper.class);
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
     * Read entity revisions into the WikipediaEntityRevision class.
     */
    public static class EntityRevPairMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String rev_text = value.toString();
            rev_text = rev_text.substring(rev_text.indexOf("\t")).trim();

            try {
                JSONObject rev_json = new JSONObject(rev_text);

                long rev_id = rev_json.getLong("id");
                String title = rev_json.getString("title");

                context.write(new LongWritable(rev_id), new Text(title));
            } catch (Exception e) {
                //for the few revisions where the username breaks the json.
                System.out.println(e.getMessage());
            }
        }
    }
}
