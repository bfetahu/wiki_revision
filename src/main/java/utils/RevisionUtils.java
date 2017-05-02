package utils;

import entities.WikiEntity;
import entities.WikiSectionSimple;
import gnu.trove.set.hash.TIntHashSet;
import org.apache.commons.lang3.StringEscapeUtils;
import org.json.JSONObject;
import org.json.XML;

import java.util.*;

/**
 * Created by besnik on 1/18/17.
 */
public class RevisionUtils {

    public static List<Map.Entry<String, String>> computeIntersection(Map<String, List<Map.Entry<String, String>>> diff_sections) {
        List<Map.Entry<String, String>> diff_intersection = new ArrayList<>();
        String first_section = diff_sections.keySet().iterator().next();
        for (Map.Entry<String, String> difference : diff_sections.get(first_section)) { //pick one random section, check if all other sections contain the same difference
            boolean difference_in_all_sections = true;
            for (String involved_section : diff_sections.keySet()) {
                for (Map.Entry<String, String> involved_sentence : diff_sections.get(involved_section)) {
                    if (!involved_sentence.equals(difference.getKey())) {
                        difference_in_all_sections = false;
                        break;
                    }
                }
            }
            if (difference_in_all_sections) {
                diff_intersection.add(difference);
            }
        }
        return diff_intersection;
    }

    public static Set<String> computeIntersectionSet(Map<String, Set<String>> differences_similarSections) {
        Set<String> differences_intersection = new HashSet<>();

        String first_section = differences_similarSections.keySet().iterator().next();
        for (String difference : differences_similarSections.get(first_section)) { //pick one random section, check if all other sections contain the same difference
            boolean difference_in_all_sections = true;
            for (String involved_section : differences_similarSections.keySet()) {
                if (!differences_similarSections.get(involved_section).contains(difference)) {
                    difference_in_all_sections = false;
                    break;
                }
            }
            if (difference_in_all_sections) {
                differences_intersection.add(difference);
            }
        }
        return differences_intersection;
    }


    /**
     * Compute sentence similarity based on their BoW representation.
     *
     * @param sentence
     * @param sentenceSet
     * @return
     */
    public static int findMaxSimSentence(TIntHashSet sentence, TIntHashSet[] sentenceSet, double threshold) {
        double max_sim_sentence = 0.0;
        int index = -1;
        for (int i = 0; i < sentenceSet.length; i++) {
            double score = RevisionUtils.computeJaccardDistance(sentence, sentenceSet[i]);
            if (score > threshold && score > max_sim_sentence) {
                max_sim_sentence = score;
                index = i;
            }
        }
        return index;
    }

    /**
     * Compute the similarity between sections of two entity revisions.
     *
     * @param rev_a
     * @param rev_b
     * @param sectionMapping
     * @param threshold
     */
    public static void computeSectionMappings(WikiEntity rev_a, WikiEntity rev_b, Map<String, List<Map.Entry<String, Double>>> sectionMapping, double threshold) {
        //get sections for both revisions
        Set<String> rev_a_sections = rev_a.sections.keySet();
        Set<String> rev_b_sections = rev_b.sections.keySet();

        //check for differences between revisions
        for (String section : rev_a_sections) {
            sectionMapping.put(section, new ArrayList<>());
            if (!rev_b_sections.contains(section)) {
                //check if section name has changed
                for (String compared_section : rev_b_sections) {
                    //rename?
                    if (rev_a_sections.contains(compared_section)) {
                        continue;
                    }
                    //compute similarity between sections
                    double score = computeJaccardDistance(rev_a.sections.get(section).section_bow, rev_b.sections.get(compared_section).section_bow);
                    if (score < threshold) {
                        continue;
                    }
                    sectionMapping.get(section).add(new AbstractMap.SimpleEntry<>(compared_section, score));
                }
            }
        }
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
     * Parse the revision string into an entity.
     *
     * @param value
     * @param nlp
     * @return
     */
    public static WikiEntity parseEntity(String value, NLPUtils nlp) {
        JSONObject entity_json = XML.toJSONObject(value.toString()).getJSONObject("page");
        JSONObject revision_json = entity_json.getJSONObject("revision");
        JSONObject contributor_json = revision_json.getJSONObject("contributor");
        JSONObject text_json = revision_json.getJSONObject("text");
        //get the child nodes
        String title = "", user_id = "", text = "";
        title = entity_json.has("title") ? entity_json.get("title").toString() : "";

        user_id = contributor_json != null && contributor_json.has("id") ? contributor_json.get("id").toString() : "";
        user_id = user_id.isEmpty() && contributor_json.has("ip") ? contributor_json.get("ip").toString() : "";

        text = text_json != null && text_json.has("content") ? text_json.get("content").toString() : "";


        WikiEntity entity = new WikiEntity();
        entity.title = (title);
        entity.revision_id = revision_json.getLong("id");
        entity.timestamp = revision_json.get("timestamp").toString();
        entity.title = title;
        entity.user_id = user_id;
        entity.setContent(text);

        entity.setSections(nlp);
        for (String section_key : entity.sections.keySet()) {
            WikiSectionSimple section_simple = entity.sections.get(section_key);
            section_simple.setSectionCitations(entity.entity_citations);

            section_simple.generateSectionBoW();
            section_simple.generateSentenceBoW();
        }

        return entity;
    }


    /**
     * Construct the sentence list for each entity sections.
     *
     * @param text
     * @return
     */
    public static List<String> getSentences(String text, boolean running_text, NLPUtils nlp) {
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
        } else {
            text = StringEscapeUtils.unescapeJson(text);
            String[] parts = text.split("\n");
            for (String part : parts) {
                if (part.length() > 3) {
                    sentences.add(part);
                }
            }
        }
        return sentences;
    }
}
