package mapred;

import entities.WikiEntity;
import entities.WikiSectionSimple;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import utils.NLPUtils;
import utils.RevisionUtils;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * Created by besnik on 3/15/17.
 */
public class WikiRevLineParser {
    public static NLPUtils nlp = new NLPUtils(2);

    public static void main(String[] args) throws Exception {
        WikiRevLineParser rev = new WikiRevLineParser();
//        rev.test(FileUtils.getFileReader("/Users/besnik/Desktop/rev_1.txt"));
        rev.run(args);
    }

    public void run(String[] args) throws Exception {
        String data_dir = "", out_dir = "";
        int num_threads = 10;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-data_dir")) {
                data_dir = args[++i];
            } else if (args[i].equals("-out_dir")) {
                out_dir = args[++i];
            } else if (args[i].equals("-num_threads")) {
                num_threads = Integer.valueOf(args[++i]);
            }
        }

        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

        CompressionCodecFactory factory = new CompressionCodecFactory(conf);

        Path pt = new Path(data_dir);
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] files = fs.listStatus(pt);


        ExecutorService threadPool = Executors.newFixedThreadPool(num_threads);
        final String out_dir_f = out_dir;
        Arrays.asList(files).parallelStream().forEach(file -> {
            Runnable r = () -> {
                try {
                    //output the data into HDFs
                    Path out_file_path = new Path("hdfs://nameservice1/" + out_dir_f + "/" + file.getPath().getName().replace(".bz2", ""));
                    if (fs.exists(out_file_path)) {
                        fs.delete(out_file_path, true);
                    }
                    OutputStream os = fs.create(out_file_path);
                    BufferedWriter br_out = new BufferedWriter(new OutputStreamWriter(os));


                    BufferedReader br = getReader(file.getPath(), fs, factory);
                    String line;

                    StringBuffer revision_data = new StringBuffer();
                    StringBuffer entity_data = new StringBuffer();
                    StringBuffer out_sb = new StringBuffer();

                    System.out.println("Processing  file " + file.getPath());
                    int count = 0;
                    boolean is_revision_data = false, is_entity = false;

                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty() || line.equals("</page>")) {
                            continue;
                        }

                        if (line.equals("<page>")) {
                            is_entity = true;
                            entity_data.delete(0, entity_data.length());
                            entity_data.append(line).append("\n");
                        } else if (line.equals("<revision>")) {
                            is_revision_data = true;
                        } else if (!is_revision_data && is_entity) {
                            entity_data.append(line).append("\n");
                        } else if (line.equals("</revision>")) {
                            revision_data.append(line).append("\n");
                            is_revision_data = false;

                            count = processRevisionData(entity_data, revision_data, out_sb, br_out, count, out_file_path.getName());
                            continue;
                        }

                        if (is_revision_data) {
                            revision_data.append(line).append("\n");
                        }
                    }
                    //flush the remaining revision
                    count = processRevisionData(entity_data, revision_data, out_sb, br_out, count, out_file_path.getName());

                    br_out.close();
                    br.close();
                    System.out.println("Finished processing file " + file.getPath());
                } catch (Exception e) {
                    System.out.println("Error processing file " + file.getPath().toString() + " with message " + e.getMessage());
                    e.printStackTrace();
                }
            };
            threadPool.submit(r);
        });
        threadPool.shutdown();
        threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public void test(BufferedReader br) throws IOException {
        String line;

        StringBuffer revision_data = new StringBuffer();
        StringBuffer entity_data = new StringBuffer();
        StringBuffer out_sb = new StringBuffer();

        boolean is_revision_data = false, is_entity = false;
        int count = 0;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty() || line.equals("</page>")) {
                continue;
            }
            if (line.equals("<page>")) {
                is_entity = true;
                entity_data.delete(0, entity_data.length());
                entity_data.append(line).append("\n");
            } else if (line.equals("<revision>")) {
                is_revision_data = true;
            } else if (!is_revision_data && is_entity) {
                entity_data.append(line).append("\n");
            } else if (line.equals("</revision>")) {
                revision_data.append(line).append("\n");
                is_revision_data = false;

                count = processRevisionData(entity_data, revision_data, out_sb, null, count, "");
                continue;
            }

            if (is_revision_data) {
                revision_data.append(line).append("\n");
            }
        }
        //flush the remaining revision
        processRevisionData(entity_data, revision_data, out_sb, null, count, "");
    }

    /**
     * Get file reader.
     */
    public static BufferedReader getReader(Path path, FileSystem fs, CompressionCodecFactory factory) throws IOException, CompressorException {
        InputStream stream;
        CompressionCodec codec = factory.getCodec(path);

        // check if we have a compression codec we need to use
        if (codec != null) {
            stream = codec.createInputStream(fs.open(path));
        } else {
            stream = fs.open(path);
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(stream));
        return br;
    }


    /**
     * Print the revision data into a file.
     *
     * @param entity_data
     * @param revision_data
     * @param out_sb
     * @param br_out
     * @throws IOException
     */
    public int processRevisionData(StringBuffer entity_data, StringBuffer revision_data, StringBuffer out_sb, BufferedWriter br_out, int count, String file_name) throws IOException {
        //process the revision
        WikiEntity revision = RevisionUtils.parseEntity(entity_data.toString() + "\n" + revision_data.append("\n</page>"), nlp);
        if (revision == null) {
            return count;
        }
        String entity_text = printRevision(revision);
        out_sb.append(entity_text).append("\n");
        revision_data.delete(0, revision_data.length());

        if (out_sb.length() > 10000000) {
            br_out.write(out_sb.toString());
            out_sb.delete(0, out_sb.length());

            System.out.printf("Finished processing %d revisions so far for file %s.\n", count, file_name);
        }
        count++;
        return count;
    }

    /**
     * Print the revision information to output into a JSON line format.
     *
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
}
