package utils;

import entities.WikiEntity;
import entities.WikiSection;
import entities.WikiStatement;
import gnu.trove.set.hash.TIntHashSet;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.json.JSONObject;
import org.json.XML;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by besnik on 1/18/17.
 */
public class RevisionUtils {

    /**
     * Compute sentence similarity based on their BoW representation.
     *
     * @param sentence
     * @param section
     * @return
     */
    public static Triple<Integer, Integer, Double> findMaxSimSentence(WikiStatement sentence, WikiSection section, double threshold) {
        double max_sim_sentence = 0.0;
        int index = -1;

        Map<Integer, WikiStatement> section_statements = section.getSectionStatements();
        for (int statement_id : section_statements.keySet()) {
            WikiStatement statement = section_statements.get(statement_id);
            double score = RevisionUtils.computeJaccardDistance(sentence.sentence_bow, statement.sentence_bow);
            if (score > threshold && score > max_sim_sentence) {
                max_sim_sentence = score;
                index = statement.id;
            }
        }
        return new ImmutableTriple<>(sentence.id, index, max_sim_sentence);
    }

    /**
     * Compute the similarity between sections of two entity revisions.
     *
     * @param prev_revision
     * @param current_revision
     * @param threshold
     */
    public static List<Triple<String, String, Double>> computeSectionMappingsMax(WikiEntity prev_revision, WikiEntity current_revision, double threshold) {
        List<Triple<String, String, Double>> mapping = new ArrayList<>();
        //get sections for both revisions
        Set<String> current_sections = current_revision.getSectionKeys();
        Set<String> prev_sections = prev_revision.getSectionKeys();

        //check for differences between revisions
        for (String prev_section : prev_sections) {
            if (current_sections.contains(prev_section)) {
                mapping.add(new ImmutableTriple<>(prev_section, prev_section, 1.0));
                continue;
            }

            //in case the previous section does not exist in the current revision, we find the closest matching one
            double max_score = 0.0;
            String max_current_section = "";
            for (String current_section : current_sections) {
                double score = computeJaccardDistance(prev_revision.getSection(prev_section).section_bow, current_revision.getSection(current_section).section_bow);
                if (max_score < score && score >= threshold) {
                    max_current_section = current_section;
                    max_score = score;
                }
            }

            //in case there is no matching section above some threshold, then we have an empty string here which indicates
            //that the section has been deleted.
            mapping.add(new ImmutableTriple<>(prev_section, max_current_section, max_score));
        }

        //check for sections that might have been added in this revision
        for (String current_section : current_sections) {
            if (!prev_sections.contains(current_section)) {
                double max_score = 0.0;
                String max_prev_section = "";
                for (String prev_section : prev_sections) {
                    double score = computeJaccardDistance(prev_revision.getSection(prev_section).section_bow, current_revision.getSection(current_section).section_bow);
                    if (max_score < score && score >= threshold) {
                        max_prev_section = current_section;
                        max_score = score;
                    }
                }

                //the section has been added in this revision
                if (max_prev_section.isEmpty()) {
                    mapping.add(new ImmutableTriple<>(max_prev_section, current_section, max_score));
                }
            }
        }
        return mapping;
    }

    /**
     * Computes the Jaccard distance between two sets of values.
     *
     * @param values_a
     * @param values_b
     * @return
     */
    public static double computeJaccardDistance(TIntHashSet values_a, TIntHashSet values_b) {
        if (values_a.isEmpty() || values_b.isEmpty()) {
            return 0;
        }
        TIntHashSet common = new TIntHashSet(values_a);
        common.retainAll(values_b);
        return common.size() / (double) (values_a.size() + values_b.size() - common.size());
    }


    /**
     * Convert the XML Wikipedia revision to JSON.
     *
     * @param value
     * @return
     */
    public static String convertToJSON(String value) {
        JSONObject entity_json = XML.toJSONObject(value.toString()).getJSONObject("page");
        JSONObject revision_json = entity_json.getJSONObject("revision");
        JSONObject contributor_json = revision_json.getJSONObject("contributor");
        JSONObject text_json = revision_json.getJSONObject("text");
        //get the child nodes
        String title, user_id, username, userip, text, comment;
        title = entity_json.has("title") ? entity_json.get("title").toString() : "";

        user_id = contributor_json.has("id") ? contributor_json.get("id").toString() : "";
        username = contributor_json.has("username") ? contributor_json.get("username").toString() : "";
        userip = contributor_json.has("ip") ? contributor_json.get("ip").toString() : "";

        text = text_json != null && text_json.has("content") ? text_json.get("content").toString() : "";
        comment = revision_json != null && revision_json.has("comment") ? revision_json.get("comment").toString() : "";

        boolean is_minor = value.contains("<minor");
        StringBuffer sb = new StringBuffer();
        sb.append("{").append("\"id\":").append(revision_json.getLong("id")).append(",").
                append("\"timestamp\":\"").append(revision_json.get("timestamp").toString()).append("\",").
                append("\"user_id\":\"").append(user_id).append("\",").
                append("\"user_name\":\"").append(username).append("\",").
                append("\"user_ip\":\"").append(userip).append("\",").
                append("\"comment\":\"").append(StringEscapeUtils.escapeJson(comment)).append("\",").
                append("\"minor\":\"").append(is_minor).append("\",").
                append("\"title\":\"").append(StringEscapeUtils.escapeJson(title)).append("\",").
                append("\"text\":\"").append(StringEscapeUtils.escapeJson(text)).append("\"}");
        return sb.toString();
    }

}
