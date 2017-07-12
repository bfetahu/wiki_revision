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
import utils.FileUtils;

import java.io.IOException;

/**
 * Created by besnik on 7/12/17.
 */
public class UserRevision extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new UserRevision(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        long milliSeconds = 10000 * 60 * 60;// <default is 600000, likewise can give any value)

        conf.setLong("mapreduce.task.timeout", milliSeconds);
        conf.set("mapreduce.output.compress", "true");
        conf.set("mapreduce.child.java.opts", "8192m");
        String users = "";

        String data_dir = "", out_dir = "";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-data_dir")) {
                data_dir = args[++i];
            } else if (args[i].equals("-out_dir")) {
                out_dir = args[++i];
            } else if (args[i].equals("-users")) {
                users = FileUtils.readText(args[++i]);
                users = "\t" + users.replaceAll("\n", "\t") + "\t";
            }
        }
        conf.set("users", users);
        Job job = new Job(conf);
        job.setNumReduceTasks(0);

        job.setJarByClass(UserRevision.class);
        job.setJobName(UserRevision.class.getName() + "-" + System.currentTimeMillis());

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(UserRevMapper.class);
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
    public static class UserRevMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String rev_text = value.toString();
            rev_text = rev_text.substring(rev_text.indexOf("\t")).trim();

            try {
                JSONObject rev_json = new JSONObject(rev_text);
                String user_name = rev_json.getString("user_name");

                String users = context.getConfiguration().get("users");
                String user = "\t" + user_name + "\t";

                if (!user_name.isEmpty() && users.contains(user)) {
                    context.write(key, new Text(rev_text));
                }
            } catch (Exception e) {
                //for the few revisions where the username breaks the json.
                System.out.println(e.getMessage());
            }
        }
    }


}
