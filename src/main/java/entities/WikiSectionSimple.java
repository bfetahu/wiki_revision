package entities;

import gnu.trove.set.hash.TIntHashSet;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by besnik on 9/17/16.
 */
public class WikiSectionSimple implements Serializable {
    public String section_label;
    public String section_text;
    public int section_level = 0;

    //section sentences
    public List<String> sentences;

    //the child sections
    public List<WikiSectionSimple> child_sections;

    public List<String> urls;

    //all the citations within this sections
    private Map<Integer, Map<String, String>> section_citations;

    public TIntHashSet section_bow;
    public TIntHashSet[] sentence_bow;

    public WikiSectionSimple() {
        child_sections = new ArrayList<>();
        sentences = new ArrayList<>();
        section_citations = new HashMap<>();
        urls = new ArrayList<>();
    }

    /**
     * Gets all the section keys.
     */
    public Set<String> getSectionKeys() {
        Set<String> keys = new HashSet<>();
        keys.add(section_label);

        if (!child_sections.isEmpty()) {
            for (WikiSectionSimple child_section : child_sections) {
                keys.addAll(child_section.getSectionKeys());
            }
        }
        return keys;
    }

    public void setSectionCitations(Map<Integer, Map<String, String>> citations) {
        if (section_text == null || section_text.isEmpty()) {
            return;
        }
        Pattern cite_number_pattern = Pattern.compile("\\{\\{[0-9]+\\}\\}");
        Matcher cite_matcher = cite_number_pattern.matcher(section_text);
        while (cite_matcher.find()) {
            int cite_marker = Integer.valueOf(cite_matcher.group().replaceAll("\\{|\\}", ""));
            if (!citations.containsKey(cite_marker)) {
                continue;
            }
            section_citations.put(cite_marker, citations.get(cite_marker));

            //assign the URLs from the citations to the section
            Map<String, String> citation = citations.get(cite_marker);
            String url_type = new StringBuffer().append(citation.containsKey("url") ? citation.get("url") : "").
                    append("  (").append((citation.containsKey("type") ? citation.get("type") : "")).
                    append(")").toString();
            urls.add(url_type);
        }
    }


    @Override
    public String toString() {
        return StringUtils.repeat("=", section_level) + section_label + StringUtils.repeat("=", section_level);
    }

    public void generateSectionBoW() {
        String[] tokens = section_text.toLowerCase().split("\\s+");
        for (String token : tokens) {
            section_bow.add(token.intern().hashCode());
        }
    }

    public void generateSentenceBoW() {
        for (int i = 0; i < sentences.size(); i++) {
            sentence_bow[i] = new TIntHashSet();
            String sentence = sentences.get(i);
            String[] tokens = sentence.toLowerCase().split("\\s+");
            for (String token : tokens) {
                sentence_bow[i].add(token.intern().hashCode());
            }
        }
    }

    /**
     * Returns the parent section.
     *
     * @param label
     * @param parent
     * @return
     */
    public static WikiSectionSimple findSection(String label, WikiSectionSimple parent, int level) {
        if (parent.section_level <= level - 1) {
            if (parent.section_level <= level - 1 && parent.section_label.equals(label)) {
                return parent;
            } else if (!parent.child_sections.isEmpty()) {
                for (WikiSectionSimple child_section : parent.child_sections) {
                    WikiSectionSimple section = findSection(label, child_section, level);

                    if (section != null) {
                        return section;
                    }
                }
            }
        }
        return null;
    }

}
