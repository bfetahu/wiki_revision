package mapred;

import entities.WikiEntity;
import entities.WikiSectionSimple;
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
import utils.FileUtils;
import utils.NLPUtils;
import utils.RevisionUtils;

import java.io.IOException;


/**
 * Created by besnik on 3/15/17.
 */
public class WikiRevLineParser extends Configured implements Tool {
    public static NLPUtils nlp = new NLPUtils(2);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new WikiRevLineParser(), args);
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

        job.setJarByClass(WikiRevLineParser.class);
        job.setJobName(WikiRevLineParser.class.getName() + "-" + System.currentTimeMillis());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text .class);

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
            for (Text revision : values) {
                //write the output
                context.write(null, revision);
            }
        }
    }

    /**
     * @param revision
     * @return
     */
    public static String printRevision(WikiEntity revision) {
        StringBuffer sb = new StringBuffer();

        //entity revision header
        sb.append("{\"revision_id\":").append(revision.revision_id).append(",\"timestamp\":\"").append(revision.timestamp).append("\",\"entity_id\":").
                append(revision.entity_id).append(", \"entity_title\":\"").append(StringEscapeUtils.escapeJson(revision.title)).append("\",\"user_id\":\"").
                append(revision.user_id).append("\",\"sections\":[");

        //add the sections
        int added_sections = 0;
        for (String section : revision.sections.keySet()) {
            if (added_sections != 0) {
                sb.append(",");
            }
            added_sections++;
            WikiSectionSimple wiki_section = revision.sections.get(section);

            //add the section title and its text.
            sb.append("{\"section\":\"").append(StringEscapeUtils.escapeJson(section)).
                    append("\",\"section_text\":\"").append(StringEscapeUtils.escapeJson(wiki_section.section_text)).append("\"");
//                    append("\", \"sentences\":[");

//            //add the sentences
//            for (int i = 0; i < wiki_section.sentences.size(); i++) {
//                if (i != 0) {
//                    sb.append(",");
//                }
//                sb.append("\"").append(StringEscapeUtils.escapeJson(wiki_section.sentences.get(i))).append("\"");
//            }
//            sb.append("]");
            //add the urls
            sb.append(",\"urls\":[");
            for (int i = 0; i < wiki_section.urls.size(); i++) {
                if (i != 0) {
                    sb.append(",");
                }
                sb.append("\"").append(StringEscapeUtils.escapeJson(wiki_section.urls.get(i))).append("\"");
            }
            sb.append("]}");

        }
        sb.append("]}");
        return sb.toString();
    }

    /**
     * Read entity revisions into the WikipediaEntityRevision class.
     */
    public static class WikiRevisionFilterMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            WikiEntity revision = RevisionUtils.parseEntity(value.toString(), nlp);
            String entity_text = printRevision(revision);

            context.write(new Text(revision.title), new Text(entity_text));
        }
    }

}
