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
        xparams.setMinClusters(1);
        xparams.setMaxClusters(sections.keySet().size());
        xparams.setDistanceMetric(new CosineDistanceMetric());
        xparams.setWorkerThreadCount(500);

        try {
            String[] section_headlines = new String[sections.size()];
            sections.keySet().toArray(section_headlines);
            double[][] scores = new double[section_headlines.length][section_headlines.length];
            int index = 0;
            for (int i = 0; i < section_headlines.length; i++) {
                for (int j = 0; j < section_headlines.length; j++) {
                    double score = 0.0;
                    if (i == j) {
                        score = 1.0;
                    } else {
                        score = SimilarityMeasures.computeCosineSimilarity(sections.get(section_headlines[i]).toString(), sections.get(section_headlines[i]).toString());
                    }
                    scores[i][j] = score;
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
            Integer[] section_title_array = new Integer[section_headlines.length];
            Map<Integer, Set<Integer>> cluster_instances = new HashMap<>();

            for (Cluster cluster : clusters) {
                Set<Integer> sub_cluster_instances = new HashSet<>();
                cluster_instances.put(cluster_id, sub_cluster_instances);

                IntIterator sections_indices = cluster.getMembers();
                while (sections_indices.hasNext()) {
                    int section_index = sections_indices.getNext();
                    int section_id = section_title_array[section_index];
                    sub_cluster_instances.add(section_id);
                }
                cluster_id++;
            }

            StringBuffer sb = new StringBuffer();
            for (int cluster_id_key : cluster_instances.keySet()) {
                String clustered_sections = "";
                for (int section_indice : cluster_instances.get(cluster_id_key)) {
                    clustered_sections = section_headlines[section_indice] + ";";
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
