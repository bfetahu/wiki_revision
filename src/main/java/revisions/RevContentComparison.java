package revisions;

import entities.WikiEntity;
import entities.WikiSection;
import entities.WikiStatement;
import gnu.trove.set.hash.TIntHashSet;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.tuple.Triple;
import utils.RevisionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by besnik on 07.06.17.
 */
public class RevContentComparison {
    public static final double section_sim_threshold = 0.1;
    public static final double sentence_sim_threshold = 0.3;

    /**
     * Compare revisions.
     *
     * @param current_revision
     * @param prev_revision
     * @return
     * @throws IOException
     */
    public String compareWithOldRevision(WikiEntity current_revision, WikiEntity prev_revision) throws IOException {
        //handle first version of article
        List<Triple<Integer, Integer, Double>> sentence_mappings = new ArrayList<>();
        StringBuffer sb = new StringBuffer();

        sb.append("{\"prev_revision\":").append(prev_revision != null ? prev_revision.getRevisionID() : 0).
                append(", \"current_revision\":").append(current_revision.getRevisionID()).
                append(", \"title\":\"").append(StringEscapeUtils.escapeJson(current_revision.title)).
                append("\", \"timestamp\":\"").append(current_revision.getRevisionTimestamp()).
                append("\", \"user_id\":\"").append(current_revision.user_id).
                append("\", \"user_name\":\"").append(StringEscapeUtils.escapeJson(current_revision.user_name)).
                append("\", \"user_ip\":\"").append(StringEscapeUtils.escapeJson(current_revision.user_ip)).
                append("\", \"sections\":[");

        List<Triple<String, String, Double>> mappings = RevisionUtils.computeSectionMappingsMax(prev_revision, current_revision, section_sim_threshold);
        int section_counter = 0;
        for (Triple<String, String, Double> section_mapping : mappings) {
            sentence_mappings.clear();

            String prev_section_label = section_mapping.getLeft();
            String current_section_label = section_mapping.getMiddle();

            /**
             * in case we are dealing with a section that has been added in the current revision,
             * we do not compute the sentence mappings.
             *
             * In the other case if the current_section is empty then we know that a section from
             * the previous revision has been deleted.
             */
            if (prev_section_label.isEmpty() && current_section_label.isEmpty()) {
                continue;
            } else if (prev_section_label.isEmpty()) {
                if (section_counter != 0) {
                    sb.append(",");
                }
                printInitialSectionRevision(prev_revision, current_revision, current_revision.getSection(current_section_label), sb, true);
                section_counter++;
                continue;
            } else if (current_section_label.isEmpty()) {
                if (section_counter != 0) {
                    sb.append(",");
                }
                printInitialSectionRevision(prev_revision, current_revision, prev_revision.getSection(prev_section_label), sb, false);
                section_counter++;
                continue;
            }

            WikiSection prev_section = prev_revision.getSection(prev_section_label);
            WikiSection current_section = current_revision.getSection(current_section_label);

            //the mapping generates the similarity score between sentences, in case, where the sentence has been
            //deleted, updated, or added we can infer by the sentence IDs.
            //For example, if the sentence ID from the previous revision is missing, we are dealing with a deleted
            //sentence in the current revision [-1 label], in case the sentence from the current revision is not
            //present in the previous revision we are dealing with an added sentence [2 label], otherwise we have [1 label]
            //indicating that the sentence has changed.
            sentence_mappings = compareSectionSentences(prev_section, current_section, sentence_sim_threshold);
            if (sentence_mappings.isEmpty()) {
                continue;
            }

            if (section_counter != 0) {
                sb.append(",");
            }
            //output the comparison between the two mapped sections.
            printRevisionComparison(current_revision, prev_revision, section_mapping, prev_section, current_section, sentence_mappings, sb);
            section_counter++;
        }

        sb.append("]}");

        return sb.toString();
    }


    /**
     * Map the sentences between two mapped sections across the different revisions.
     * We focus on the sentences which have: (i) changed, (ii) deleted, and (iii) added
     *
     * @param prev_section
     * @param current_section
     */
    public List<Triple<Integer, Integer, Double>> compareSectionSentences(WikiSection prev_section, WikiSection current_section, double threshold) {
        List<Triple<Integer, Integer, Double>> content_diff = new ArrayList<>();

        //map sentences that might have been deleted or updated
        Map<Integer, WikiStatement> prev_statements = prev_section.getSectionStatements();
        Map<Integer, WikiStatement> current_statements = current_section.getSectionStatements();
        for (int sentence_id : prev_statements.keySet()) {
            WikiStatement prev_sentence = prev_statements.get(sentence_id);
            if (!current_statements.containsKey(sentence_id)) {
                //check if there is a similar sentence
                Triple<Integer, Integer, Double> mapping = RevisionUtils.findMaxSimSentence(prev_sentence, current_section, threshold);
                content_diff.add(mapping);
            }
        }

        //check if there has been any new sentence added in the current section
        for (int current_sentence_id : current_statements.keySet()) {
            WikiStatement current_sentence = current_statements.get(current_sentence_id);
            //consider all those cases where the sentences cannot be explicitly linked
            if (!prev_statements.containsKey(current_sentence_id)) {
                Triple<Integer, Integer, Double> mapping = RevisionUtils.findMaxSimSentence(current_sentence, prev_section, threshold);
                if (mapping.getMiddle() != -1) {
                    continue;
                }
                content_diff.add(mapping);
            }
        }
        return content_diff;
    }

    /**
     * Print all the information about the initial revision for the specific article.
     *
     * @param revision
     * @return
     */
    public String printInitialRevision(WikiEntity revision) {
        StringBuffer sb = new StringBuffer();
        sb.append("{\"prev_revision\":").append(0).
                append(", \"current_revision\":").append(revision.getRevisionID()).
                append(", \"title\":\"").append(StringEscapeUtils.escapeJson(revision.title)).
                append("\", \"timestamp\":\"").append(revision.getRevisionTimestamp()).
                append("\", \"user_id\":").append(revision.user_id).
                append(", \"user_name\":\"").append(StringEscapeUtils.escapeJson(revision.user_name)).
                append("\", \"user_ip\":\"").append(StringEscapeUtils.escapeJson(revision.user_ip)).
                append("\", \"sections\":[");

        int section_counter = 0;
        for (String section_key : revision.getSectionKeys()) {
            if (section_counter != 0) {
                sb.append(",");
            }
            //sentences
            WikiSection section = revision.getSection(section_key);
            printInitialSectionRevision(null, revision, section, sb, true);
            section_counter++;
        }
        sb.append("]}");
        return sb.toString();
    }

    /**
     * Print the initial revision for a section or for a section that has been added.
     *
     * @param section
     * @param sb
     */
    public void printInitialSectionRevision(WikiEntity prev_revision, WikiEntity current_revision,
                                            WikiSection section, StringBuffer sb, boolean is_new) {

        sb.append("{").append("\"prev_section\":\"NA\", \"current_section\":\"").append(StringEscapeUtils.escapeJson(section.section_label)).append("\", \"score\":0.0, \"statements\":[");


        String label = is_new ? "2" : "-1";
        Map<Integer, WikiStatement> statements = section.getSectionStatements();
        int statement_counter = 0;
        for (int statement_id : statements.keySet()) {
            if (statement_counter != 0) {
                sb.append(",");
            }
            WikiStatement statement = statements.get(statement_id);
            sb.append("{\"prev_statement\":\"\", \"current_statement\":\"").append(StringEscapeUtils.escapeJson(statement.toString())).append("\", \"label\":").append(label).append(", \"score\":0}");
            statement_counter++;
        }
        sb.append("], \"citations\":[");

        Map<Integer, String[]> entity_citations = is_new ? current_revision.getEntityCitations() : prev_revision.getEntityCitations();
        TIntHashSet section_citations = section.getSectionCitations();
        int citation_counter = 0;
        for (int cite_id : entity_citations.keySet()) {
            if (citation_counter != 0) {
                sb.append(",");
            }
            if (!section_citations.contains(cite_id)) {
                continue;
            }
            String[] citation = entity_citations.get(cite_id);
            sb.append("{\"url\":\"").append(StringEscapeUtils.escapeJson(citation[1])).append("\",\"label\":").append(label).append("}");
            citation_counter++;
        }
        sb.append("]}");
    }

    /**
     * Print the comparison between two revisions, specifically for two mapped sections.
     *
     * @param current_revision
     * @param section_mapping
     * @param sentence_mappings
     * @return
     */
    public void printRevisionComparison(WikiEntity current_revision, WikiEntity prev_revision,
                                        Triple<String, String, Double> section_mapping,
                                        WikiSection prev_section, WikiSection current_section,
                                        List<Triple<Integer, Integer, Double>> sentence_mappings, StringBuffer sb) {

        sb.append("{").append("\"prev_section\":\"").append(section_mapping.getLeft()).
                append("\", \"current_section\":\"").append(StringEscapeUtils.escapeJson(section_mapping.getMiddle())).
                append("\", \"score\":").append(section_mapping.getRight()).append(", \"statements\":[");

        //output the sentence mappings
        Map<Integer, WikiStatement> prev_statements = prev_section.getSectionStatements();
        Map<Integer, WikiStatement> current_statements = current_section.getSectionStatements();

        int statement_counter = 0;
        for (Triple<Integer, Integer, Double> sentence_mapping : sentence_mappings) {
            WikiStatement statement_0 = prev_statements.get(sentence_mapping.getLeft());
            WikiStatement statement_1 = current_statements.get(sentence_mapping.getMiddle());
            if (statement_0.toString().isEmpty() && statement_1.toString().isEmpty()) {
                continue;
            }

            double score = sentence_mapping.getRight();
            int label = 1;
            if (sentence_mapping.getLeft() == -1) {
                label = 2;
            } else if (sentence_mapping.getMiddle() == -1) {
                label = -1;
            }

            if (statement_counter != 0) {
                sb.append(",");
            }
            sb.append("{\"prev_statement\":\"").append(StringEscapeUtils.escapeJson(statement_0 != null ? statement_0.toString() : "")).
                    append("\", \"current_statement\":\"").append(StringEscapeUtils.escapeJson(statement_1 != null ? statement_1.toString() : "")).
                    append("\", \"label\":").append(label).append(", \"score\":").append(score).append("}");

            statement_counter++;
        }
        sb.append("], \"citations\":[");

        //output the URLs that are deleted and added
        TIntHashSet prev_section_citations = prev_section.getSectionCitations();
        TIntHashSet current_section_citations = current_section.getSectionCitations();

        int citation_counter = 0;
        for (int cite_id : prev_section_citations.toArray()) {
            if (!current_section_citations.contains(cite_id)) {
                String[] citation = prev_revision.getCitation(cite_id);
                if (citation == null) {
                    continue;
                }
                if (citation_counter != 0) {
                    sb.append(",");
                }
                sb.append("{\"url\":\"").append(StringEscapeUtils.escapeJson(prev_revision.getCitation(cite_id)[1])).append("\",\"label\":-1}");
                citation_counter++;
            }
        }

        citation_counter = 0;
        for (int cite_id : current_section_citations.toArray()) {
            if (statement_counter != 0) {
                sb.append(",");
            }
            if (!prev_section_citations.contains(cite_id)) {
                String[] citation = current_revision.getCitation(cite_id);
                if (citation == null) {
                    continue;
                }
                sb.append("{\"url\":\"").append(StringEscapeUtils.escapeJson(citation[1])).append("\",\"label\":2}");
                citation_counter++;
            }
        }
        sb.append("]}");
    }

}
