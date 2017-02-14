package revisions;

import org.apache.commons.lang3.StringEscapeUtils;
import org.battelle.clodhopper.Cluster;
import org.battelle.clodhopper.distance.CosineDistanceMetric;
import org.battelle.clodhopper.tuple.Array2DTupleList;
import org.battelle.clodhopper.tuple.TupleList;
import org.battelle.clodhopper.util.IntIterator;
import org.battelle.clodhopper.xmeans.XMeansClusterer;
import org.battelle.clodhopper.xmeans.XMeansParams;
import utils.FileUtils;
import utils.SimilarityMeasures;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

/**
 * Created by besnik on 2/13/17.
 */
public class SectionClusterer {
    public static void main(String[] args) throws IOException {
        String[] args1 = {"/Users/besnik/Downloads/malaysia_airlins_17_sections_sorted", "/Users/besnik/Datasets/english.stop"};
        args = args1;
        Set<String> stop_words = FileUtils.readIntoSet(args[1], "\n", false);

        //read the sections into the map data structure.
        Map<String, StringBuffer> sections = new HashMap<>();
        BufferedReader reader = FileUtils.getFileReader(args[0]);
        while (reader.ready()) {
            String section_revision_line = reader.readLine().trim();
            String[] data = section_revision_line.split("\t");

            if (data.length != 3) {
                continue;
            }

            String section_title = data[0];
            String section_text = StringEscapeUtils.unescapeJson(data[2]);
            if (!sections.containsKey(section_title)) {
                sections.put(section_title, new StringBuffer());
            }
            sections.get(section_title).append(section_text).append("\n");
        }

        //cluster the sections
        String cluster_output = clusterSections(sections, stop_words);
        System.out.println(cluster_output);
    }

    /**
     * Perform XMeans clustering on the section labels.
     */
    private static String clusterSections(Map<String, StringBuffer> sections, Set<String> stop_words) {
        SimilarityMeasures.stop_words = stop_words;
        XMeansParams xparams = new XMeansParams();
        xparams.setMinClusters(2);
        xparams.setMaxClusters(sections.keySet().size());
        xparams.setDistanceMetric(new CosineDistanceMetric());
        xparams.setWorkerThreadCount(100);
        xparams.setUseOverallBIC(true);

        try {
            String[] section_headlines = new String[sections.size()];
            sections.keySet().toArray(section_headlines);
            double[][] scores = new double[section_headlines.length][section_headlines.length];
            IntStream.range(0, section_headlines.length).parallel().forEach(i -> {
                for (int j = i; j < section_headlines.length; j++) {
                    double score = 0.0;
                    if (i == j) {
                        score = 1.0;
                    } else {
                        score = SimilarityMeasures.computeCosineSimilarity(sections.get(section_headlines[i]).toString(), sections.get(section_headlines[j]).toString());
                    }
                    scores[i][j] = score;
                }
            });

            //update the similarities of the lower triangle of the matrix
            for (int i = 0; i < scores.length; i++) {
                for (int j = 0; j < i; j++) {
                    scores[i][j] = scores[j][i];
                }
            }
            TupleList tpl = new Array2DTupleList(scores);
            System.out.println("Starting to compute the xmeans clusters");

            XMeansClusterer xmeans = new XMeansClusterer(tpl, xparams);
            Thread th = new Thread(xmeans);
            th.start();
            List<Cluster> clusters = xmeans.get();

            int cluster_id = 1;
            //convert the keys to a set
            Map<Integer, Set<String>> cluster_instances = new HashMap<>();

            for (Cluster cluster : clusters) {
                Set<String> sub_cluster_instances = new HashSet<>();
                cluster_instances.put(cluster_id, sub_cluster_instances);

                IntIterator sections_indices = cluster.getMembers();
                while (sections_indices.hasNext()) {
                    int section_index = sections_indices.getNext();
                    String section = section_headlines[section_index];
                    sub_cluster_instances.add(section);
                }
                cluster_id++;
            }

            StringBuffer sb = new StringBuffer();
            for (int cluster_id_key : cluster_instances.keySet()) {
                String clustered_sections = "";
                for (String section : cluster_instances.get(cluster_id_key)) {
                    clustered_sections = clustered_sections + section + ";";
                }
                sb.append(cluster_id_key).append("\t").append(clustered_sections).append("\n");
            }
            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }
}
