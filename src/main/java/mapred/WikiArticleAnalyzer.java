package mapred;

import entities.WikiEntity;
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
import utils.WikiUtils;

import java.io.IOException;

/**
 * Created by besnik on 14.06.17.
 */
public class WikiArticleAnalyzer extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new WikiArticleAnalyzer(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        int num_reducers = 10;
        long milliSeconds = 10000 * 60 * 60;// <default is 600000, likewise can give any value)

        conf.setLong("mapreduce.task.timeout", milliSeconds);

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

        job.setJarByClass(WikiArticleAnalyzer.class);
        job.setJobName(WikiArticleAnalyzer.class.getName() + "-" + System.currentTimeMillis());

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(WikiAnalyzerMapper.class);
        job.setReducerClass(WikiAnalyzerReducer.class);
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
    public static class WikiAnalyzerReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String rev_text = value.toString();
                rev_text = rev_text.substring(rev_text.indexOf("\t")).trim();
                WikiEntity entity = WikiUtils.parseEntity(rev_text, true);

                if (entity == null) {
                    continue;
                }

                entity.setExtractStatements(false);
                entity.setExtractReferences(true);
                entity.setMainSectionsOnly(false);
                entity.setSplitSections(true);

                entity.parseContent(false);

                String entity_output = entity.printSectionCitations(true);
                context.write(key, new Text(entity_output));
            }
        }
    }


    /**
     * Read entity revisions into the WikipediaEntityRevision class.
     */
    public static class WikiAnalyzerMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }
}
