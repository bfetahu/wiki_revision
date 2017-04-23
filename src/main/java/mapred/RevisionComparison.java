package mapred;

import entities.WikiEntity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hedera.io.input.WikiRevisionTextInputFormat;
import org.json.JSONObject;
import org.json.XML;
import revisions.RevisionCompare;
import utils.FileUtils;
import utils.NLPUtils;
import utils.RevisionUtils;

import java.io.IOException;
import java.util.*;


/**
 * Created by besnik on 3/15/17.
 */
public class RevisionComparison extends Configured implements Tool {
    public static NLPUtils nlp = new NLPUtils(2);
    public static Set<String> stop_words = new HashSet<>();

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RevisionComparison(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        int num_reducers = 10;
        long milliSeconds = 10000 * 60 * 60;// <default is 600000, likewise can give any value)

        conf.setLong("mapreduce.task.timeout", milliSeconds);
        conf.setLong("mapred.task.timeout", milliSeconds);
        conf.set("mapred.output.compress", "true");
        conf.set("mapred.child.java.opts", "-Xmx8G");

        String data_dir = "", out_dir = "";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-data_dir")) {
                data_dir = args[++i];
            } else if (args[i].equals("-out_dir")) {
                out_dir = args[++i];
            } else if (args[i].equals("-reducers")) {
                num_reducers = Integer.valueOf(args[++i]);
            } else if (args[i].equals("-stop_words")) {
                String stop_word_path = args[++i];
                conf.set("stop_words", FileUtils.readText(stop_word_path));
                stop_words = FileUtils.readIntoSet(stop_word_path, "\n", false);
            }
        }
        Job job = new Job(conf);

        job.setNumReduceTasks(num_reducers);

        job.setJarByClass(RevisionComparison.class);
        job.setJobName(RevisionComparison.class.getName() + "-" + System.currentTimeMillis());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setMapperClass(WikiRevisionFilterMapper.class);
        job.setReducerClass(WikiRevisionFilterReducer.class);
        job.setInputFormatClass(WikiRevisionTextInputFormat.class);

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
    public static class WikiRevisionFilterReducer extends Reducer<Text, BytesWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
            RevisionCompare rc = new RevisionCompare();
            Map<Long, WikiEntity> sorted_revisions = new TreeMap<>();
            for (BytesWritable value : values) {
                WikiEntity entity = new WikiEntity();
                entity.readBytes(value.getBytes());
                sorted_revisions.put(entity.revision_id, entity);
            }
            WikiEntity prev = null;
            List<String> revision_difference_data = new ArrayList<>();
            for (long rev_id : sorted_revisions.keySet()) {
                WikiEntity revision = sorted_revisions.get(rev_id);
                //return the revision difference data ending with a "\n"
                String rev_comparison = "";
                if (prev == null) {
                    rev_comparison = rc.compareWithOldRevision(revision, new WikiEntity(), true);
                } else {
                    rev_comparison = rc.compareWithOldRevision(revision, prev, false);
                }
                prev = revision;
                revision_difference_data.add(rev_comparison.replaceAll("\n+", " "));
            }

            //write the data into HDFS.
            int total_length = revision_difference_data.stream().mapToInt(s -> s.length()).sum();
            int buckets = total_length / Integer.MAX_VALUE;
            buckets = buckets == 0 ? 1 : buckets;
            int length = total_length / buckets;

            Iterator<String> lines = revision_difference_data.iterator();
            for (int i = 0; i < buckets; i++) {
                StringBuffer sb = new StringBuffer();

                while (lines.hasNext()) {
                    if (sb.length() >= length) break;
                    sb.append(lines.next());
                    lines.remove();
                }
                context.write(new Text(key), new Text(sb.toString()));
            }
        }
    }

    /**
     * Read entity revisions into the WikipediaEntityRevision class.
     */
    public static class WikiRevisionFilterMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject entity_json = XML.toJSONObject(value.toString()).getJSONObject("page");
            JSONObject revision_json = entity_json.getJSONObject("revision");
            //get the child nodes
            String title = entity_json.has("title") ? entity_json.get("title").toString() : "";

            if (revision_json.has("timestamp")) {
                WikiEntity revision = RevisionUtils.parseEntity(value.toString(), nlp);
                String year = revision.timestamp.substring(0, revision.timestamp.indexOf("-"));
                context.write(new Text(title + "\t" + year), new BytesWritable(revision.getBytes()));
            }
        }
    }
}
