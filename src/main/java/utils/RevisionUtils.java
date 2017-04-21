package utils;

import entities.WikiEntity;
import entities.WikiSection;
import entities.WikiSectionSimple;
import entities.WikipediaEntity;
import org.apache.commons.lang3.StringEscapeUtils;
import org.json.JSONObject;
import org.json.XML;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by besnik on 1/18/17.
 */
public class RevisionUtils {
    public static final String citation_type_str = "album notes,av media,book,comic,conference,court,encyclopedia,episode,journal,mailing list,map,news,newsgroup,press release,thesis,video game,web";
    public static Set<String> citation_types = new HashSet<>();

    /**
     * Extract the references from the Wikipedia revision pages. The citations are returned in JSON string output format.
     * In addition we return the section in which the paragraph belongs too.
     *
     * @param text
     * @return
     */
    public static Map<String, List<String[]>> getSentenceCitationAttributes(String text) {
        if (text == null) {
            return null;
        }
        Map<String, List<String[]>> citations = new LinkedHashMap<>();

        Map<Integer, String> citations_sentences = new HashMap<>();
        String parsed_text = parseStringFromReferences(text, citations_sentences);

        //extract the sentences with the corresponding citations.
        Pattern sentence_pattern = Pattern.compile("(.*?)\\.\\s{0,}(\\{\\{[0-9]+\\}\\}\\s?){1,}");
        Matcher sentence_matcher = sentence_pattern.matcher(parsed_text);

        //extract citation number
        Pattern cite_number_pattern = Pattern.compile("\\{\\{[0-9]+\\}\\}");
        boolean added_sentences = false;

        while (sentence_matcher.find()) {
            String sentence = sentence_matcher.group();
            addSentenceCitations(sentence, citations, cite_number_pattern, citations_sentences);
            added_sentences = true;
        }

        //in case we have only a sentence then the pattern wont match.
        if (!added_sentences) {
            addSentenceCitations(parsed_text, citations, cite_number_pattern, citations_sentences);
        }

        return citations;
    }

    /**
     * Parses a Wikipedia text, namely its references and citations into a common format. It is later on used for extracting
     * the citations.
     *
     * @param text
     * @param citations_sentences
     * @return
     */
    public static String parseStringFromReferences(String text, Map<Integer, String> citations_sentences) {
        //remove first references that do not contain any citation.
        Pattern citation_pattern = Pattern.compile("<ref(\\s+name=[\\\"?a-zA-Z0-9\\\"?]*)?/>");
        Matcher citation_matcher = citation_pattern.matcher(text);

        StringBuffer sb = new StringBuffer();
        int last_index = 0;

        //remove the empty references.
        boolean has_updated = false;

        while (citation_matcher.find()) {
            int c_start = citation_matcher.start();
            int c_end = citation_matcher.end();

            sb.append(text.substring(last_index, c_start));
            last_index = c_end;
            has_updated = true;
        }
        if (has_updated) {
            if (last_index < text.length()) {
                sb.append(text.substring(last_index));
            }
            text = sb.toString();
            has_updated = false;
        }
        sb.delete(0, sb.length());
        int citation_counter = 0;

        //find the sentence splits, they are either as "." or ".<ref>" the regex is ((.*?)</ref>)
        last_index = 0;
        citation_pattern = Pattern.compile("<ref(.*?)/?>((.*?)</ref>)?");
        citation_matcher = citation_pattern.matcher(text);
        while (citation_matcher.find()) {
            String citation = citation_matcher.group();
            int c_start = citation_matcher.start();
            int c_end = citation_matcher.end();

            sb.append(text.substring(last_index, c_start)).append("{{").append(citation_counter).append("}} ");
            last_index = c_end;
            citations_sentences.put(citation_counter, citation);
            citation_counter++;
            has_updated = true;
        }

        //in case there are citations that are outside the <ref>.*</ref> tags process them too.
        //check if there is anything left from the citations that are not within the reference tags
        if (has_updated) {
            if (last_index < text.length()) {
                sb.append(text.substring(last_index));
            }
            text = sb.toString();
            has_updated = false;
        }
        sb.delete(0, sb.length());
        last_index = 0;

        citation_pattern = Pattern.compile("(?i)\\{\\{Cit(.*?)\\}\\}");
        citation_matcher = citation_pattern.matcher(text);
        while (citation_matcher.find()) {
            String citation = citation_matcher.group();
            int c_start = citation_matcher.start();
            int c_end = citation_matcher.end();

            sb.append(text.substring(last_index, c_start)).append("{{").append(citation_counter).append("}} ");
            last_index = c_end;
            citations_sentences.put(citation_counter, citation);
            citation_counter++;
            has_updated = true;
        }

        //check if there is anything left from the citations that are not within the reference tags
        if (has_updated) {
            if (last_index < text.length()) {
                sb.append(text.substring(last_index));
            }
            text = sb.toString();
        }
        return text;
    }

    /**
     * Parses a Wikipedia text, namely its references and citations into a common format.
     *
     * @param text
     * @return
     */
    public static String removeWikiReferences(String text) {
        //remove first references that do not contain any citation.
        Pattern citation_pattern = Pattern.compile("<ref(\\s+name=[\\\"?a-zA-Z0-9\\\"?]*)?/>");
        Matcher citation_matcher = citation_pattern.matcher(text);

        StringBuffer sb = new StringBuffer();
        int last_index = 0;

        //remove the empty references.
        boolean has_updated = false;

        while (citation_matcher.find()) {
            int c_start = citation_matcher.start();
            int c_end = citation_matcher.end();

            sb.append(text.substring(last_index, c_start));
            last_index = c_end;
            has_updated = true;
        }
        if (has_updated) {
            if (last_index < text.length()) {
                sb.append(text.substring(last_index));
            }
            text = sb.toString();
            has_updated = false;
        }
        sb.delete(0, sb.length());

        //find the sentence splits, they are either as "." or ".<ref>" the regex is ((.*?)</ref>)
        last_index = 0;
        citation_pattern = Pattern.compile("<ref(.*?)/?>((.*?)</ref>)?");
        citation_matcher = citation_pattern.matcher(text);
        while (citation_matcher.find()) {
            int c_start = citation_matcher.start();
            int c_end = citation_matcher.end();

            sb.append(text.substring(last_index, c_start));
            last_index = c_end;
            has_updated = true;
        }

        //in case there are citations that are outside the <ref>.*</ref> tags process them too.
        //check if there is anything left from the citations that are not within the reference tags
        if (has_updated) {
            if (last_index < text.length()) {
                sb.append(text.substring(last_index));
            }
            text = sb.toString();
            has_updated = false;
        }
        sb.delete(0, sb.length());
        last_index = 0;

        citation_pattern = Pattern.compile("(?i)\\{\\{Cit(.*?)\\}\\}");
        citation_matcher = citation_pattern.matcher(text);
        while (citation_matcher.find()) {
            int c_start = citation_matcher.start();
            int c_end = citation_matcher.end();

            sb.append(text.substring(last_index, c_start));
            last_index = c_end;
            has_updated = true;
        }

        //check if there is anything left from the citations that are not within the reference tags
        if (has_updated) {
            if (last_index < text.length()) {
                sb.append(text.substring(last_index));
            }
            text = sb.toString();
        }
        return text;
    }

    /**
     * Parses a Wikipedia text, namely its references and citations into a common format.
     *
     * @param text
     * @return
     */
    public static String removeWikiFileReferences(String text) {
        Matcher m = Pattern.compile("\\[\\[File[^\\]]*?[^\\]].*\\]\\]").matcher(text);
        StringBuffer sb = new StringBuffer();
        int last_index = 0;

        //remove the empty references.
        boolean has_updated = false;

        while (m.find()) {
            int c_start = m.start();
            int c_end = m.end();

            sb.append(text.substring(last_index, c_start));
            last_index = c_end;
            has_updated = true;
        }
        if (has_updated) {
            if (last_index < text.length()) {
                sb.append(text.substring(last_index));
            }
            text = sb.toString();
            has_updated = false;
        }
        sb.delete(0, sb.length());

        return text;
    }


    /**
     * For a sentence extract all the citations and add them to the corresponding data structure that holds all their attributes.
     *
     * @param sentence
     * @param citations
     * @param cite_number_pattern
     * @param citations_sentences
     */
    private static void addSentenceCitations(String sentence, Map<String, List<String[]>> citations, Pattern cite_number_pattern, Map<Integer, String> citations_sentences) {
        List<String[]> sub_citations = new ArrayList<>();

        Matcher cite_number_matcher = cite_number_pattern.matcher(sentence);
        while (cite_number_matcher.find()) {
            String cite_number = cite_number_matcher.group();
            cite_number = cite_number.replaceAll("\\{|\\}", "");
            try {
                String url_text = citations_sentences.get(Integer.valueOf(cite_number));
                if (url_text == null || url_text.isEmpty()) {
                    continue;
                }
                url_text = url_text.replaceAll("</?ref(.*?)>", "").trim();
                if (url_text.contains("{{") && url_text.contains("}}")) {
                    url_text = url_text.replaceAll("\\{\\{", "");
                    url_text = url_text.replaceAll("\\}\\}", "");
                }
                Map<String, String> cite_attributes = getCitationAttributes(url_text);
                if (cite_attributes.get("type").equals("N/A")) {
                    continue;
                }
                sub_citations.add(new String[]{cite_attributes.get("type"), cite_attributes.get("url")});
            } catch (Exception e) {

            }
        }

        if (!sub_citations.isEmpty()) {
            String sentence_tmp = sentence.replaceAll("\\{\\{[0-9]+?\\}\\}", "");
            citations.put(sentence_tmp, sub_citations);
        }
    }

    /**
     * Splits a Wikipedia citation into its detailed attributes.
     *
     * @param citation_text
     * @return
     */
    public static Map<String, String> getCitationAttributes(String citation_text) {
        //split the citation line into <key, value> pairs.
        String[] url_iter = citation_text.split("\\|");
        Map<String, String> citation_features = new HashMap<>();
        for (int i = 0; i < url_iter.length; i++) {
            String s_tmp = url_iter[i];
            String[] key_s = s_tmp.split("=");
            if (key_s.length <= 1) {
                continue;
            }

            String key = key_s[0].trim().intern();
            String value = s_tmp.substring(s_tmp.indexOf("=") + 1).trim().toLowerCase();
            citation_features.put(key, value);
        }
        citation_features.put("type", getCitationType(citation_text));
        return citation_features;
    }


    /**
     * Returns the citation type from a citation template in Wikipedia.
     *
     * @param url_text
     * @return
     */
    public static String getCitationType(String url_text) {
        if (citation_types.isEmpty()) {
            String[] tmp = citation_type_str.split(",");
            for (String s : tmp) citation_types.add(s);
        }
        int start = url_text.indexOf(" ");
        int end = url_text.indexOf("|");

        if (start != -1 && end != -1 && start < end) {
            String type = url_text.substring(start, end).trim().intern();
            if (!citation_types.contains(type.toLowerCase())) {
                type = "N/A";
            }
            return type;
        }
        return "N/A";
    }

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

            //check for specific sections such as External Links and Notes
            if (section_key.equals("External links") || section_key.equals("Notes")) {
                section_simple.sentences = getSentences(section.section_text, false, nlp);
            } else {
                section_simple.sentences = getSentences(section.section_text, true, nlp);
            }

            //assign the URLs from the citations to the section
            for (int cite_id : section.getSectionCitations().keySet()) {
                Map<String, String> cite_attributes = section.getSectionCitations().get(cite_id);
                String url_type = new StringBuffer().append(cite_attributes.containsKey("url") ? cite_attributes.get("url") : "").
                        append("  (").append((cite_attributes.containsKey("type") ? cite_attributes.get("type") : "")).
                        append(")").toString();
                section_simple.urls.add(url_type);
            }
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


    /**
     * Get the domain from an URL.
     *
     * @param url
     * @return
     */
    public static String getURLDomain(String url) {
        String domain = "";
        try {
            if (url.startsWith("http://")) {
                int end_index = url.indexOf("/", "http://".length());
                if (end_index == -1) {
                    domain = url;
                } else {
                    domain = url.substring(0, end_index);
                }
            } else if (url.startsWith("https://")) {
                int end_index = url.indexOf("/", "https://".length());
                if (end_index == -1) {
                    domain = url;
                } else {
                    domain = url.substring(0, end_index);
                }
            } else if (url.startsWith("www.")) {
                int end_index = url.indexOf("/");
                if (end_index == -1) {
                    domain = url;
                } else {
                    domain = url.substring(0, end_index);
                }
            }

            if (domain.trim().isEmpty()) {
                return "N/A";
            }

            domain = domain.trim().toLowerCase();

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(url);
        }
        domain = domain.replace("http://", "");
        domain = domain.replace("https://", "");
        domain = domain.replace("www.", "");
        return domain;
    }


    public static String removeCategoryInformation(String content) {
        return content.replaceAll("\\[\\[Category:(.*)\\]\\]", "");
    }
}
