package revisions;

import java.io.IOException;

/**
 * Created by besnik on 2/13/17.
 */
public class SectionClusterer {
    /*public static void main(String[] args) throws IOException {
        String input_file = args[0];
        String out_file = args[1];
        String stop_words_file = args[2];
        boolean is_raw_revision_file = args[3].equals("true");

        Set<String> stop_words = FileUtils.readIntoSet(stop_words_file, "\n", false);
        Map<Long, Set<String>> revision_sections = new HashMap<>();
        Map<String, StringBuffer> sections = is_raw_revision_file ? readSectionsFromRawRevisions(input_file, revision_sections) : readProcessedSectionRevisions(input_file, revision_sections);

        //compute the average number of sections across all revisions
        int min_section_num = revision_sections.values().stream().mapToInt(x -> x.size()).min().getAsInt();
        int max_section_num = revision_sections.values().stream().mapToInt(x -> x.size()).max().getAsInt();

        //cluster the sections
        String cluster_output = clusterSections(sections, stop_words, min_section_num, max_section_num);
        System.out.println(cluster_output);
        FileUtils.saveText(cluster_output, out_file);
    }

    *//**
     * Read the processed revisions. In each line we have a revision and the section text.
     *
     * @param file
     * @return
     * @throws IOException
     *//*
    public static Map<String, StringBuffer> readProcessedSectionRevisions(String file, Map<Long, Set<String>> revision_sections) throws IOException {
        //read the sections into the map data structure.
        Map<String, StringBuffer> sections = new HashMap<>();
        BufferedReader reader = FileUtils.getFileReader(file);
        while (reader.ready()) {
            String section_revision_line = reader.readLine().trim();
            String[] data = section_revision_line.split("\t");

            if (data.length != 3) {
                continue;
            }

            String section_title = data[0].trim().toLowerCase();
            long rev_id = Long.valueOf(data[1]);
            String section_text = StringEscapeUtils.unescapeJson(data[2]);
            if (!sections.containsKey(section_title)) {
                sections.put(section_title, new StringBuffer());
            }
            sections.get(section_title).append(section_text).append("\n");

            if (!revision_sections.containsKey(rev_id)) {
                revision_sections.put(rev_id, new HashSet<>());
            }
            revision_sections.get(rev_id).add(section_title);
        }
        return sections;
    }

    *//**
     * Read the section text from the raw revisions.
     *
     * @param file
     * @return
     *//*
    public static Map<String, StringBuffer> readSectionsFromRawRevisions(String file, Map<Long, Set<String>> revision_sections) throws IOException {
        Map<String, StringBuffer> sections = new HashMap<>();
        BufferedReader reader = FileUtils.getFileReader(file);
        String line;

        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) {
                continue;
            }
            String revision_xml_text = StringEscapeUtils.unescapeJson(line).trim();
            Document revision_doc = FileUtils.readXMLDocumentFromString(revision_xml_text);

            //get the revision text
            String entity_title = ((Element) revision_doc.getElementsByTagName("query").item(0).getFirstChild().getFirstChild()).getAttribute("title");
            String rev_text = revision_doc.getElementsByTagName("query").item(0).getFirstChild().getFirstChild().getFirstChild().getFirstChild().getTextContent();
            long rev_id = Long.parseLong(((Element) revision_doc.getElementsByTagName("query").item(0).getFirstChild().getFirstChild().getFirstChild().getFirstChild()).getAttribute("revid"));

            WikipediaEntity entity = new WikipediaEntity();
            entity.setTitle(entity_title);
            entity.setSplitSections(true);
            entity.setMainSectionsOnly(false);
            entity.setExtractReferences(true);
            entity.setCleanReferences(true);
            entity.setContent(rev_text);

            //extract the references.
            for (String section : entity.getSectionKeys()) {
                String section_text = entity.getSectionText(section);

                String section_label = section.toLowerCase().replaceAll("\\{\\{(.*?)\\}\\}", "").replaceAll("<ref>(.*)</?ref>", "").trim();
                if (!sections.containsKey(section_label)) {
                    sections.put(section_label, new StringBuffer());
                }
                sections.get(section_label).append(section_text).append("\n");

                if (!revision_sections.containsKey(rev_id)) {
                    revision_sections.put(rev_id, new HashSet<>());
                }
                revision_sections.get(rev_id).add(section_label);
            }
        }
        return sections;
    }

    *//**
     * Perform XMeans clustering on the section labels.
     *//*
    private static String clusterSections(Map<String, StringBuffer> sections, Set<String> stop_words, int min_num_clusters, int max_num_clusters) {
        SimilarityMeasures.stop_words = stop_words;
        XMeansParams xparams = new XMeansParams();
        xparams.setMinClusters(min_num_clusters);
        xparams.setMaxClusters(max_num_clusters);
        xparams.setWorkerThreadCount(500);
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
    }*/
}
