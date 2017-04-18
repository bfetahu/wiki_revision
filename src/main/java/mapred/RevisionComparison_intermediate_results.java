package mapred;

import entities.WikiEntity;
import entities.WikiSection;
import entities.WikiSectionSimple;
import entities.WikipediaEntity;
import org.apache.commons.lang3.StringEscapeUtils;
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

import java.io.IOException;
import java.util.*;


/**
 * Created by besnik on 3/15/17.
 */
public class RevisionComparison_intermediate_results extends Configured implements Tool {
    public static NLPUtils nlp = new NLPUtils(2);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RevisionComparison_intermediate_results(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        int num_reducers = 10;
        long milliSeconds = 10000 * 60 * 60;// <default is 600000, likewise can give any value)

        conf.setLong("mapreduce.task.timeout", milliSeconds);
        conf.setLong("mapred.task.timeout", milliSeconds);
        conf.set("mapred.output.compress", "true");

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

        job.setJarByClass(RevisionComparison_intermediate_results.class);
        job.setJobName(RevisionComparison_intermediate_results.class.getName() + "-" + System.currentTimeMillis());

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
        Text key_text = new Text(); //+
        Text sb_text = new Text(); //+
        @Override
        protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
            Map<Long, WikiEntity> sorted_revisions = new TreeMap<>();
            List<String> revisions_output = new ArrayList<>();

            for (BytesWritable value : values) {
                WikiEntity entity = new WikiEntity();
                entity.readBytes(value.getBytes());
                sorted_revisions.put(entity.revision_id, entity);
            }

            for (long rev_id : sorted_revisions.keySet()) {
                WikiEntity revision = sorted_revisions.get(rev_id);

                //generate output String for each revision
                StringBuffer sb = new StringBuffer();
                sb.append(revision.revision_id).append("\t")
                        .append(revision.user_id).append("\t")
                        .append(revision.entity_id).append("\t")
                        .append(revision.title).append("\t")
                        .append(revision.timestamp).append("\t");

                for (String section : revision.sections.keySet()){
                    List<String> section_sentences = revision.sections.get(section).sentences;
                    List<String> section_urls = revision.sections.get(section).urls;
                    sb.append(section + "||");
                    for (String sentence : section_sentences){
                        sb.append(sentence + ";");
                    }
                    sb.append("||");
                    for (String url : section_urls){
                        sb.append(url + ";");
                    }
                    sb.append("\t");
                }

               revisions_output.add(sb.toString());
            }

            //write the data into HDFS.
            int total_length = revisions_output.stream().mapToInt(s -> s.length()).sum();
            int buckets = total_length / Integer.MAX_VALUE;
            buckets = buckets == 0 ? 1 : buckets;
            int length = total_length / buckets;

            Iterator<String> lines = revisions_output.iterator();
            for (int i = 0; i < buckets; i++) {
                StringBuffer sb = new StringBuffer();

                while (lines.hasNext()) {
                    if (sb.length() >= length) break;
                    sb.append(lines.next());
                    lines.remove();
                }
                key_text.set(key + "--" + i); //+
                sb_text.set(sb.toString()); //+
                context.write(key_text, sb_text); //!
            }

        }

    }

    /**
     * Read entity revisions into the WikipediaEntityRevision class.
     */
    public static class WikiRevisionFilterMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
        Text text = new Text(); //+
        BytesWritable bytes = new BytesWritable(); //+
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject entity_json = XML.toJSONObject(value.toString()).getJSONObject("page");
            JSONObject revision_json = entity_json.getJSONObject("revision");
            //get the child nodes
            String title = "";
            title = entity_json.has("title") ? entity_json.get("title").toString() : "";

            if (revision_json.has("timestamp")) {
                WikiEntity revision = parseEntity(value.toString());
                text.set(title); //+
                bytes.set(revision.getBytes(),0,revision.getBytes().length); //?
                context.write(text, bytes); //!
            }
        }
    }

    public static WikiEntity parseEntity(String value) {
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


        WikipediaEntity entity = new WikipediaEntity();
        entity.setTitle(title);
        entity.setCleanReferences(true);
        entity.setExtractReferences(true);
        entity.setMainSectionsOnly(false);
        entity.setSplitSections(true);
        entity.setContent(text);

        //extract the section sentences.
        WikiEntity entity_simple = new WikiEntity();
        entity_simple.revision_id = revision_json.getLong("id");
        entity_simple.timestamp = revision_json.get("timestamp").toString();
        entity_simple.title = title;
        entity_simple.user_id = user_id;

        for (String section_key : entity.getSectionKeys()) {
            WikiSection section = entity.getSection(section_key);
            WikiSectionSimple section_simple = new WikiSectionSimple();

            //check for specific sections such as External Links and Notes
            if (section_key.equals("External links") || section_key.equals("Notes")){
                section_simple.sentences = getSentences(section.section_text, false);
            } else{
                section_simple.sentences = getSentences(section.section_text, true);
            }

            entity_simple.sections.put(section_key, section_simple);
        }

        return entity_simple;
    }


    /**
     * Construct the sentence list for each entity sections.
     *
     * @param text
     * @return
     */
    public static List<String> getSentences(String text, boolean running_text) {
        List<String> sentences = new ArrayList<>();
        text = StringEscapeUtils.escapeJson(text);

        //cleaning
        text = text.replace("...", ".");
        text = text.replaceAll("\\{\\{[0-9]+\\}\\}", "");
        text = text.replaceAll("\\[|\\]", "");

        if (running_text) {
            for (String sentence : nlp.getDocumentSentences(text)) {
                sentence = sentence.replace("\\n", "");
                if (sentence.length() > 3) {
                    sentences.add(sentence);
                }
            }
        } else{
            text = StringEscapeUtils.unescapeJson(text);
            String[] parts = text.split("\n");
            for (String part : parts){
                if (part.length() > 3) {
                    sentences.add(part);
                }
            }
        }
        return sentences;
    }
}
