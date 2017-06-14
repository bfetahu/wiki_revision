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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hedera.io.input.WikiRevisionTextInputFormat;
import utils.RevisionUtils;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by besnik on 5/19/17.
 */
public class WikiLine extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            ToolRunner.run(new WikiLine(), args);
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

        job.setJarByClass(WikiLine.class);
        job.setJobName(WikiLine.class.getName() + "-" + System.currentTimeMillis());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(WikiLineMapper.class);
        job.setReducerClass(WikiLineReducer.class);
        job.setInputFormatClass(WikiRevisionTextInputFormat.class);

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
    public static class WikiLineReducer extends Reducer<LongWritable, Text, Text, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iter = values.iterator();
            while (iter.hasNext()) {
                context.write(null, iter.next());
            }
        }
    }

    /**
     * Read entity revisions into the WikipediaEntityRevision class.
     */
    public static class WikiLineMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String json_rev = RevisionUtils.convertToJSON(value.toString());
            context.write(key, new Text(json_rev));
        }
    }
}
