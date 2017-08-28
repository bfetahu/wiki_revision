package mapred;

import entities.WikiEntity;
import gnu.trove.list.array.TIntArrayList;
import io.FileUtils;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


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

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

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
    public static class PairReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            RevContentComparison rc = new RevContentComparison();
            StringBuffer sb = new StringBuffer();
            wiki.utils.WikiUtils.timeout = 30000;

            //is the initial revision
            List<WikiEntity> sorted_vals = new ArrayList<>();
            for (Text value : values) {
                WikiEntity entity = RevisionPairComparison.parseEntities(value.toString(), false);
                if (entity == null) {
                    return;
                }
                sorted_vals.add(entity);
            }

            if (sorted_vals.size() == 2) {
                boolean is_correct_order = sorted_vals.get(0).getRevisionID() < sorted_vals.get(1).getRevisionID();
                WikiEntity prev_rev = is_correct_order ? sorted_vals.get(0) : sorted_vals.get(1);
                WikiEntity current_rev = is_correct_order ? sorted_vals.get(1) : sorted_vals.get(0);
                sb.append(rc.compareWithOldRevision(current_rev, prev_rev));
            } else if (sorted_vals.size() == 1 && key.toString().endsWith("-0")) {
                WikiEntity current_revision = sorted_vals.get(0);
                sb.append(rc.printInitialRevision(current_revision));
            }

            context.write(key, new Text(sb.toString()));
        }
    }

    /**
     * Read entity revisions into the WikipediaEntityRevision class.
     */
    public static class PairMapper extends Mapper<LongWritable, Text, Text, Text> {
        protected Map<Integer, TIntArrayList> rev_pairs;

        @Override
        public void setup(Mapper.Context context) throws IOException {
            rev_pairs = (Map<Integer, TIntArrayList>) FileUtils.readObject("rp");

            if (FileUtils.fileExists("rp1", false)) {
                Map<Integer, TIntArrayList> rev_pairs_tmp = (Map<Integer, TIntArrayList>) FileUtils.readObject("rp1");
                rev_pairs.putAll(rev_pairs_tmp);
            }

            System.out.println("Finished reading revision list " + rev_pairs.size());
        }

        /**
         * Find the matching entries for a given revision id.
         *
         * @param rev_id
         * @return
         */
        private int[] generateKey(long rev_id, String title) {
            int title_hashcode = title.hashCode();
            int rev_hashcode = Long.hashCode(rev_id);
            if (!rev_pairs.containsKey(title_hashcode)) {
                return null;
            }
            int index = rev_pairs.get(title_hashcode).indexOf(rev_hashcode);

            //each revision is added into two slots. In the first it is compared against a previous entry,
            //and in the second slot it serves for comparison against a newer entry
            return new int[]{index, index + 1};
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String rev_text = value.toString();
            rev_text = rev_text.substring(rev_text.indexOf("\t")).trim();

            try {
                JSONObject rev_json = new JSONObject(rev_text);
                long rev_id = rev_json.getLong("id");
                String title = rev_json.getString("title");

                int[] keys_out = generateKey(rev_id, title);
                if (keys_out == null || rev_text.contains("========")) {
                    return;
                }
                for (int key_out : keys_out) {
                    context.write(new Text(title + "-" + key_out), new Text(rev_text));
                }
            } catch (Exception e) {
                //for the few revisions where the username breaks the json.
                System.out.println(e.getMessage());
            }
        }
    }
}
