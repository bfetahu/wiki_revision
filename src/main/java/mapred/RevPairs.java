package mapred;

import entities.WikiEntity;
import gnu.trove.list.array.TLongArrayList;
import io.FileUtils;
import mapred.io.WikiText;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;


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

        job.setMapOutputKeyClass(IntWritable.class);
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
    public static class PairReducer extends Reducer<Text, WikiText, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<WikiText> values, Context context) throws IOException, InterruptedException {
            RevContentComparison rc = new RevContentComparison();
            StringBuffer sb = new StringBuffer();
            wiki.utils.WikiUtils.timeout = 30000;

            //is the initial revision
            Map<Long, WikiText> revisions = new TreeMap<>();
            for (WikiText value : values) {
                revisions.put(value.rev_id, value);
            }

            if (revisions.size() == 0) {
                long rev_id = revisions.keySet().iterator().next();
                WikiEntity current_revision = RevisionPairComparison.parseEntities(revisions.get(rev_id).text, false);
                sb.append(rc.printInitialRevision(current_revision));
            } else {
                WikiEntity prev_revision = null, current_revision = null;
                for (long rev_id : revisions.keySet()) {
                    if (prev_revision != null) {
                        prev_revision = RevisionPairComparison.parseEntities(revisions.get(rev_id).text, false);
                    } else {
                        current_revision = RevisionPairComparison.parseEntities(revisions.get(rev_id).text, false);
                    }
                }
                sb.append(rc.compareWithOldRevision(current_revision, prev_revision));
            }
            context.write(key, new Text(sb.toString()));
        }
    }

    /**
     * Read entity revisions into the WikipediaEntityRevision class.
     */
    public static class PairMapper extends Mapper<LongWritable, Text, IntWritable, WikiText> {
        protected TLongArrayList rev_pairs;

        @Override
        public void setup(Mapper.Context context) throws IOException {
            rev_pairs = (TLongArrayList) FileUtils.readObject("rp");
        }

        /**
         * Find the matching entries for a given revision id.
         *
         * @param rev_id
         * @return
         */
        private int[] generateKey(long rev_id) {
            int index = rev_pairs.indexOf(rev_id);

            //each revision is added into two slots. In the first it is compared against a previous entry,
            //and in the second slot it serves for comparison against a newer entry

            if (index == 0 || rev_pairs.get(index - 1) == -1l) {
                return new int[]{index};
            } else {
                return new int[]{(index-1), index};
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String rev_text = value.toString();
            rev_text = rev_text.substring(rev_text.indexOf("\t")).trim();

            try {
                JSONObject rev_json = new JSONObject(rev_text);
                long rev_id = rev_json.getLong("id");

                int[] keys_out = generateKey(rev_id);

                WikiText wiki = new WikiText();
                wiki.rev_id = rev_id;
                wiki.text = rev_text;

                for (int key_out : keys_out) {
                    context.write(new IntWritable(key_out), wiki);
                }
            } catch (Exception e) {
                //for the few revisions where the username breaks the json.
                System.out.println(e.getMessage());
            }
        }
    }
}
