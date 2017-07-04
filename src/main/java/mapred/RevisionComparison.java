package mapred;

import entities.WikiEntity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import wiki.utils.WikiUtils;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;


/**
 * Created by besnik on 3/15/17.
 */
public class RevisionComparison extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RevisionComparison(), args);
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

        job.setJarByClass(RevisionComparison.class);
        job.setJobName(RevisionComparison.class.getName() + "-" + System.currentTimeMillis());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(WikiRevisionFilterMapper.class);
        job.setReducerClass(WikiRevisionFilterReducer.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job, data_dir);

        Path outPath = new Path(out_dir);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);

        job.waitForCompletion(true);
        return 0;
    }

    /**
     * Reduces the output from the mappers which measures the frequency of a type assigned to resources in BTC.
     */
    public static class WikiRevisionFilterReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            RevContentComparison rc = new RevContentComparison();
            Map<Long, String> sorted_revisions = new TreeMap<>();
            for (Text value : values) {
                String rev_text = value.toString();
                long rev_id = Long.parseLong(rev_text.substring(0, rev_text.indexOf("-")));
                sorted_revisions.put(rev_id, rev_text.substring(rev_text.indexOf("-") + 1));
            }
            WikiEntity prev = null;
            StringBuffer sb = new StringBuffer();

            for (long rev_id : sorted_revisions.keySet()) {
                WikiEntity revision = WikiUtils.parseEntity(sorted_revisions.get(rev_id), true);
                revision.setExtractStatements(false);
                revision.setExtractReferences(true);
                revision.setMainSectionsOnly(false);
                revision.setSplitSections(true);

                revision.parseContent(true);

                //return the revision difference data ending with a "\n"
                if (prev == null) {
                    sb.append(rc.printInitialRevision(revision));
                } else {
                    sb.append(rc.compareWithOldRevision(revision, prev));
                }
                prev = revision;
            }

            context.write(new Text(key.toString()), new Text(sb.toString()));
        }
    }

    /**
     * Read entity revisions into the WikipediaEntityRevision class.
     */
    public static class WikiRevisionFilterMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String revision_text = value.toString();
            revision_text = revision_text.substring(revision_text.indexOf("\t"));
            try {
                JSONObject entity_json = new JSONObject(revision_text);
                String year = entity_json.getString("timestamp");
                year = year.substring(0, year.indexOf("-"));
                String title = entity_json.getString("title");
                long rev_id = entity_json.getLong("id");

                context.write(new Text(title + "-" + year), new Text(rev_id + "-" + revision_text));
            } catch (Exception e) {
                System.out.printf("Error processing %s with message %s.\n", value.toString(), e.getMessage());
            }
        }
    }
}
