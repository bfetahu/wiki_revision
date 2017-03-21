package mapred;

import entities.WikiSection;
import entities.WikipediaEntity;
import entities.WikipediaEntityRevision;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
        conf.set("mapreduce.map.java.opts", "-Xmx2G");

        String data_dir = "", out_dir = "";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-data_dir")) {
                data_dir = args[++i];
            } else if (args[i].equals("-out_dir")) {
                out_dir = args[++i];
            } else if (args[i].equals("-reducers")) {
                num_reducers = Integer.valueOf(args[++i]);
            } else if (args[i].equals("-stop_words")) {
                conf.set("stop_words", FileUtils.readText(args[++i]));
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
    public static class WikiRevisionFilterReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Long, WikipediaEntityRevision> sorted_revisions = new TreeMap<>();
            for (Text value : values) {
                WikipediaEntityRevision revision = parseEntity(value.toString());
                sorted_revisions.put(revision.revision_id, revision);
            }

            RevisionCompare rc = new RevisionCompare(context.getConfiguration().get("stop_words"));
            WikipediaEntityRevision prev = null;
            List<String> revision_difference_data = new ArrayList<>();
            for (long rev_id : sorted_revisions.keySet()) {
                WikipediaEntityRevision revision = sorted_revisions.get(rev_id);

                //extract the section sentences.
                for (String section_key : revision.entity.getSectionKeys()) {
                    WikiSection section = revision.entity.getSection(section_key);
                    section.sentences = rc.getSentences(section.section_text);
                    section.setSectionCitations(revision.entity.getEntityCitations());
                }

                //return the revision difference data ending with a "\n"
                if (prev == null) {
                    revision_difference_data.add(rc.compareWithOldRevision(revision, new WikipediaEntityRevision(), true));
                } else {
                    revision_difference_data.add(rc.compareWithOldRevision(revision, prev, false));
                }
                prev = revision;
            }

            //write the data into HDFS.
            Text revision_output = new Text();
            int last_pos = 0;
            for (String rev_output : revision_difference_data) {
                byte[] data = rev_output.getBytes();
                int len = data.length;

                revision_output.append(data, last_pos, len);
                last_pos = last_pos + len + 1;
            }
            context.write(null, revision_output);
        }

    }

    /**
     * Read entity revisions into the WikipediaEntityRevision class.
     */
    public static class WikiRevisionFilterMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject entity_json = XML.toJSONObject(value.toString()).getJSONObject("page");
            JSONObject revision_json = entity_json.getJSONObject("revision");
            //get the child nodes
            String title = "";
            title = entity_json.has("title") ? entity_json.get("title").toString() : "";

            if (revision_json.has("timestamp")) {
                context.write(new Text(title), value);
            }
        }
    }

    public static WikipediaEntityRevision parseEntity(String value) {
        JSONObject entity_json = XML.toJSONObject(value.toString()).getJSONObject("page");
        JSONObject revision_json = entity_json.getJSONObject("revision");
        JSONObject contributor_json = revision_json.getJSONObject("contributor");
        JSONObject text_json = revision_json.getJSONObject("text");
        //get the child nodes
        String title = "", user_id = "", user = "", text = "";
        title = entity_json.has("title") ? entity_json.get("title").toString() : "";

        user_id = contributor_json != null && contributor_json.has("id") ? contributor_json.get("id").toString() : "";
        user_id = user_id.isEmpty() && contributor_json.has("ip") ? contributor_json.get("ip").toString() : "";
        user = contributor_json != null && contributor_json.has("username") ? contributor_json.get("username").toString() : "";

        text = text_json != null && text_json.has("content") ? text_json.get("content").toString() : "";


        WikipediaEntityRevision revision = new WikipediaEntityRevision();
        revision.user_id = user_id;
        revision.revision_id = revision_json.getLong("id");
        revision.user_id = user_id;
        revision.user_name = user;

        WikipediaEntity entity = new WikipediaEntity();
        entity.setTitle(title);
        entity.setCleanReferences(true);
        entity.setExtractReferences(true);
        entity.setMainSectionsOnly(false);
        entity.setSplitSections(true);
        entity.setContent(text);
        revision.entity = entity;


        return revision;
    }
}
