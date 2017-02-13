package mapred;


import edu.jhu.nlp.wikipedia.WikiPage;
import entities.WikiSection;
import entities.WikipediaEntity;
import entities.WikipediaEntityRevision;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hedera.io.Revision;
import org.hedera.io.input.WikiRevisionPageInputFormat;
import org.joda.time.DateTime;
import utils.RevisionUtils;

import java.io.IOException;
import java.util.*;

/**
 * Created by besnik on 1/18/17.
 */
public class RevisionExtractor extends Configured implements Tool {
    public static String entity_filter = "";

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RevisionExtractor(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        int num_reducers = 10;
        String filter = "";
        conf.set("mapreduce.map.java.opts", "-Xmx2G");

        String data_dir = "", out_dir = "";
        for (int i = 0; i < strings.length; i++) {
            if (strings[i].equals("-data_dir")) {
                data_dir = strings[++i];
            } else if (strings[i].equals("-out_dir")) {
                out_dir = strings[++i];
            } else if (strings[i].equals("-reducers")) {
                num_reducers = Integer.valueOf(strings[++i]);
            } else if (strings[i].equals("-entity_filter")) {
                entity_filter = strings[++i];
            }
        }

        conf.set("entity_filter", entity_filter);

        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path(data_dir), false);
        int i = 0;
        String input_paths = "";
        while (iter.hasNext()) {
            LocatedFileStatus ls = iter.next();
            if (!ls.getPath().toString().contains(filter)) continue;
            if (i != 0) input_paths += ",";

            input_paths += ls.getPath();
            i++;
        }
        Job job = new Job(conf);
        job.setNumReduceTasks(num_reducers);

        job.setJarByClass(RevisionExtractor.class);
        job.setJobName(RevisionExtractor.class.getName() + "-" + System.currentTimeMillis());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Integer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WikipediaEntityRevision.class);

        job.setMapperClass(RevisionExtractorMapper.class);
        job.setReducerClass(RevisionExtractorReducer.class);
        job.setInputFormatClass(WikiRevisionPageInputFormat.class);

        FileInputFormat.setInputPaths(job, input_paths);

        Path outPath = new Path(out_dir);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);

        job.waitForCompletion(true);
        return 0;
    }


    /**
     * Reduces the output from the mappers which measures the frequency of a type assigned to resources in BTC.
     */
    public static class RevisionExtractorReducer extends Reducer<Text, WikipediaEntityRevision, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<WikipediaEntityRevision> values, Context context) throws IOException, InterruptedException {
            //initial revision
            Map<Long, Map<Long, WikipediaEntityRevision>> rev_timestamps = new TreeMap<>();

            for (WikipediaEntityRevision value : values) {
                long rev_id = value.revision_id;
                long timestamp = value.timestamp;

                if (!rev_timestamps.containsKey(timestamp)) {
                    rev_timestamps.put(timestamp, new HashMap<>());
                }
                rev_timestamps.get(timestamp).put(rev_id, value);
            }

            WikipediaEntityRevision prev_revision = null;
            for (long timestamp : rev_timestamps.keySet()) {
                DateTime date = new DateTime(timestamp);

                for (long rev_id : rev_timestamps.get(timestamp).keySet()) {
                    WikipediaEntityRevision revision = rev_timestamps.get(timestamp).get(rev_id);
                    WikipediaEntity entity = revision.entity;

                    Set<String> sections = entity.getSectionKeys();
                    for (String section : sections) {
                        String section_text = entity.getSectionText(section);
                        Map<String, List<String[]>> section_citations = RevisionUtils.getSentenceCitationAttributes(section_text);
                    }

                    prev_revision = revision;
                }
            }
        }
    }


    /**
     * Process Wikipedia revision by extracting the referencing sentences for a given set of URLs and revision IDs.
     */
    public static class RevisionExtractorMapper extends Mapper<LongWritable, Revision, Text, WikipediaEntityRevision> {
        @Override
        protected void map(LongWritable key, Revision value, Context context) throws IOException, InterruptedException {
            String entity_filter = context.getConfiguration().get("entity_filter");
            String title = value.getPageTitle();
            System.out.println("Processing entity " + title);

            //check if its a disambiguation page or if it doesnt match our filter.
            String title_cmp = title.toLowerCase();
            if (title_cmp.contains("disambiguation") || title_cmp.isEmpty() || (!entity_filter.isEmpty() && !title.equals(entity_filter))) {
                return;
            }

            WikiPage page = new WikiPage();
            page.setWikiText(new String(value.getText()));

            //once you set the content it will split it into the corresponding sections.
            WikipediaEntity entity = new WikipediaEntity();
            entity.setTitle(title);
            entity.setCleanReferences(true);
            entity.setExtractReferences(true);
            entity.setMainSectionsOnly(true);
            entity.setContent(page.getText());

            Set<String> section_keys =entity.getSectionKeys();

            for(String section:section_keys){
                WikiSection sub_section = entity.getSection(section);
                sub_section.setCitations(entity.getEntityCitations());
            }
            //page.getCategories().stream().forEach(category -> entity.addCategory(category));

            WikipediaEntityRevision entity_revision = new WikipediaEntityRevision();
            entity_revision.revision_id = value.getRevisionId();
            entity_revision.entity = entity;
            entity_revision.timestamp = value.getTimestamp();

            context.write(new Text(title), entity_revision);
        }
    }
}
