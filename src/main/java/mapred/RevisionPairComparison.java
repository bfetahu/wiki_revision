package mapred;

import entities.WikiEntity;
import mapred.io.PairInputFormat;
import mapred.io.TextList;
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
import revisions.RevContentComparison;
import wiki.utils.WikiUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by besnik on 3/15/17.
 */
public class RevisionPairComparison extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RevisionPairComparison(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        int num_reducers = 10;
        long milliSeconds = 10000 * 60 * 60;// <default is 600000, likewise can give any value)

        conf.setLong("mapreduce.task.timeout", milliSeconds);
        conf.set("mapreduce.output.compress", "true");
        conf.set("mapreduce.child.java.opts", "8192m");
        conf.setInt("mapreduce.input.linerecordreader.line.maxlength", Integer.MAX_VALUE);

        String data_dir = "", out_dir = "";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-data_dir")) {
                data_dir = args[++i];
            } else if (args[i].equals("-out_dir")) {
                out_dir = args[++i];
            } else if (args[i].equals("-reducers")) {
                num_reducers = Integer.valueOf(args[++i]);
            } else if (args[i].equals("-num_lines")) {
                conf.set("mapreduce.input.lineinputformat.linespermap", args[++i]);
            }
        }
        Job job = new Job(conf);

        job.setNumReduceTasks(num_reducers);

        job.setJarByClass(RevisionPairComparison.class);
        job.setJobName(RevisionPairComparison.class.getName() + "-" + System.currentTimeMillis());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(TextList.class);

        job.setMapperClass(PairMapper.class);
        job.setReducerClass(PairReducer.class);
        job.setInputFormatClass(PairInputFormat.class);

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
    public static class PairReducer extends Reducer<LongWritable, TextList, Text, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<TextList> values, Context context) throws IOException, InterruptedException {
            RevContentComparison rc = new RevContentComparison();

            List<String> revision_lines = new ArrayList<>();
            for (TextList value : values) {
                List<String> values_list = value.getValues();
                for (int i = 0; i < values_list.size(); i++) {
                    revision_lines.add(values_list.get(i));
                }
            }

            System.out.println("There are " + revision_lines.size() + " lines for " + key);
            StringBuffer sb = new StringBuffer();

            //is the initial revision
            WikiEntity prev_revision = null;
            for (int i = 0; i < revision_lines.size(); i++) {
                String rev_line = revision_lines.get(i);
                WikiEntity current_revision = parseEntities(rev_line, true);
                if (i == 0) {
                    sb.append(rc.printInitialRevision(current_revision));
                } else if (!prev_revision.title.equals(current_revision.title)) {
                    sb.append(rc.printInitialRevision(current_revision));
                } else {
                    sb.append(rc.compareWithOldRevision(current_revision, prev_revision));
                }

                prev_revision = current_revision;
            }
            context.write(null, new Text(sb.toString()));
        }
    }


    /**
     * Read entity revisions into the WikipediaEntityRevision class.
     */
    public static class PairMapper extends Mapper<LongWritable, TextList, LongWritable, TextList> {
        @Override
        protected void map(LongWritable key, TextList value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static WikiEntity parseEntities(String rev_lines, boolean split) {
        String rev_text = rev_lines;
        if (split)
            rev_text = rev_text.substring(rev_text.indexOf("\t"));

        try {
            WikiEntity revision = WikiUtils.parseEntity(rev_text, true);
            revision.is_citation_hash_key = true;
            revision.setExtractStatements(false);
            revision.setExtractReferences(true);
            revision.setMainSectionsOnly(false);
            revision.setSplitSections(true);

            boolean parsed = revision.parseContent(true);
            if (!parsed) {
                return null;
            }
            return revision;
        } catch (Exception e) {
            return null;
        }
    }
}
