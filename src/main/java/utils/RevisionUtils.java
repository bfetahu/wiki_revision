package utils;

import entities.WikiEntity;
import entities.WikiSection;
import entities.WikiSectionSimple;
import entities.WikipediaEntity;
import org.apache.commons.lang3.StringEscapeUtils;
import org.json.JSONObject;
import org.json.XML;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by besnik on 1/18/17.
 */
public class RevisionUtils {
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
        String title = "", user_id = "", user = "", text = "";
        title = entity_json.has("title") ? entity_json.get("title").toString() : "";

        user_id = contributor_json != null && contributor_json.has("id") ? contributor_json.get("id").toString() : "";
        user_id = user_id.isEmpty() && contributor_json.has("ip") ? contributor_json.get("ip").toString() : "";
        user = contributor_json != null && contributor_json.has("username") ? contributor_json.get("username").toString() : "";

        text = text_json != null && text_json.has("content") ? text_json.get("content").toString() : "";


        WikipediaEntity entity = new WikipediaEntity();
        entity.setTitle(title);
        entity.setCleanReferences(true);
        entity.setExtractReferences(true);
        entity.setMainSectionsOnly(false);
        entity.setSplitSections(true);
        entity.setContent(text);

        //extract the section sentences.
        WikiEntity entity_simple = new WikiEntity();
        entity_simple.revision_id = revision_json.getLong("id");
        entity_simple.timestamp = revision_json.get("timestamp").toString();
        entity_simple.title = title;
        entity_simple.user_id = user_id;

        for (String section_key : entity.getSectionKeys()) {
            WikiSection section = entity.getSection(section_key);
            WikiSectionSimple section_simple = new WikiSectionSimple();

            section_simple.section_text = section.section_text;

//            //check for specific sections such as External Links and Notes
//            if (section_key.equals("External links") || section_key.equals("Notes")) {
//                section_simple.sentences = getSentences(section.section_text, false, nlp);
//            } else {
//                section_simple.sentences = getSentences(section.section_text, true, nlp);
//            }

            //assign the URLs from the citations to the section
            for (int cite_id : section.getSectionCitations().keySet()) {
                Map<String, String> cite_attributes = section.getSectionCitations().get(cite_id);
                String url_type = new StringBuffer().append(cite_attributes.containsKey("url") ? cite_attributes.get("url") : "").
                        append("  (").append((cite_attributes.containsKey("type") ? cite_attributes.get("type") : "")).
                        append(")").toString();
                section_simple.urls.add(url_type);
            }
//            section_simple.generateSectionBoW();
//            section_simple.generateSentenceBoW();
            entity_simple.sections.put(section_key, section_simple);
        }

        return entity_simple;
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
