package entities;

import org.apache.commons.lang3.StringUtils;
import utils.RevisionUtils;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by besnik on 9/14/16.
 */
public class WikipediaEntity implements Serializable {

    private String title;
    private String content;
    private Set<String> categories;

    private boolean clean_references = false;

    //a variable we use to controll wheather we further process the content
    private boolean split_sections = true;

    private WikiSection root_sections;

    public void setCleanReferences(boolean clean_references) {
        this.clean_references = clean_references;
    }

    public boolean getCleanReferences() {
        return clean_references;
    }

    public WikipediaEntity() {

        root_sections = new WikiSection();
        categories = new HashSet<>();

    }

    public void setCategories(Set<String> categories) {
        this.categories = categories;
    }

    public void addCategory(String category) {
        categories.add(category);
    }

    public Set<String> getCategories() {
        return categories;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public WikiSection getSections() {
        return root_sections;
    }

    public boolean getSplitSections() {
        return split_sections;
    }

    public void setSplitSections(boolean split_sections) {
        this.split_sections = split_sections;
    }

    public void setContent(String content) {
        //remove the infobox
        if (content.contains("{{Infobox")) {
            int start_index = content.indexOf("{{Infobox");
            content = content.substring(start_index);
            int last_infobox_pos = extractInfoboxPosition(content) + 1;
            content = content.substring(last_infobox_pos);
            content = content.replaceAll("\\{\\{(Infobox.*\\n(?:\\|.*\\n)+)\\}\\}", "");
        }

        if (clean_references) {
            content = RevisionUtils.removeWikiReferences(content);
            content = RevisionUtils.removeWikiFileReferences(content);
        }
        content = RevisionUtils.removeCategoryInformation(content);
        //\[\[Category:(.*)\]\]
        this.content = content;

        //set the sections
        if (split_sections) {
            splitEntityIntoSections(content);
        }
    }

    public String getContent() {
        return content;
    }

    public boolean hasSection(String section) {
        return root_sections.hasSection(section);
    }

    public String getSectionText(String section) {
        if (hasSection(section)) {
            return root_sections.findSection(section).section_text;
        }

        return "Section does not exist!";
    }

    public Set<String> getSectionKeys() {
        Set<String> keys = new HashSet<>();
        root_sections.getSectionKeys(keys);
        return keys;
    }

    /**
     * Return the keys only up to a specific section level.
     *
     * @param level
     * @return
     */
    public Set<String> getSectionKeys(int level) {
        Set<String> keys = new HashSet<>();
        root_sections.getSectionKeys(keys, level);
        return keys;
    }


    /**
     * Print the time series.
     *
     * @return
     */
    public String printTimeSeries() {
        StringBuffer sb = new StringBuffer();
        root_sections.printTimeSeries(sb);
        return sb.toString();
    }


    public int extractInfoboxPosition(String str) {
        int last_pos = 0;
        Stack<Character> stack = new Stack<>();
        for (int i = 0; i < str.length(); i++) {
            char current = str.charAt(i);
            if (current == '{' || current == '(' || current == '[') {
                stack.push(current);
            }


            if (current == '}' || current == ')' || current == ']') {
                char last = stack.peek();
                if (current == '}' && last == '{' || current == ')' && last == '(' || current == ']' && last == '[') {
                    stack.pop();
                    last_pos = i;

                    if (stack.isEmpty()) {
                        return last_pos;
                    }
                }
            }
        }

        return last_pos;
    }

    /**
     * Splits the entity page text into chunks of text where each chunk belongs to a section. We begin with the main section
     * that is usually the introduction of an entity page. For the subsequent section we extract the text and in case
     * the section is a subsection we denote its parent.
     *
     * @param entity_text
     */
    public void splitEntityIntoSections(String entity_text) {
        Pattern section_pattern = Pattern.compile("(?<!=)==(?!=)(.*?)(?<!=)==(?!=)");
        Matcher section_matcher = section_pattern.matcher(entity_text);

        root_sections = new WikiSection();
        root_sections.section_label = "MAIN_SECTION";

        String section_name = "MAIN_SECTION";

        int start_index = 0;
        int end_index = 0;
        int current_section_level = 0;

        List<Map.Entry<Integer, String>> prev_section_entries = new LinkedList<>();

        while (section_matcher.find()) {
            String new_section = section_matcher.group();
            int tmp_current_section_level = StringUtils.countMatches(new_section, "=") / 2;
            new_section = new_section.replaceAll("=", "");
            end_index = section_matcher.start();

            //the previous text is about the main section.
            if (!section_name.equals(new_section)) {
                //extract the citations and chunk the section text into paragraphs.
                String section_text = entity_text.substring(start_index, end_index);
                if (section_name.toLowerCase().contains("references") || section_name.toLowerCase().contains("notes") || section_name.toLowerCase().contains("see also")) {
                    continue;
                }

                WikiSection section = new WikiSection();
                section.section_label = section_name;
                section.section_text = section_text.replaceAll("\n+", "\n");

                if (current_section_level == 0 && section_name.equals("MAIN_SECTION")) {
                    root_sections = section;
                    root_sections.section_level = 1;

                    prev_section_entries.add(new AbstractMap.SimpleEntry<>(root_sections.section_level, root_sections.section_label));
                } else {
                    String parent_section_label = getParentSection(prev_section_entries, current_section_level);
                    WikiSection root_section = WikiSection.findSection(parent_section_label, root_sections, current_section_level);
                    section.section_level = root_section.section_level + 1;
                    root_section.child_sections.add(section);

                    AbstractMap.SimpleEntry<Integer, String> section_entry = new AbstractMap.SimpleEntry<>(section.section_level, section.section_label);
                    prev_section_entries.add(new AbstractMap.SimpleEntry<>(section_entry));
                }
            }

            //change the parent section only if you go deeper in the section level, for example if you are iterating over the main sections then we keep the "MAIN_SECTION" as the parent section.
            section_name = new_section;
            current_section_level = tmp_current_section_level;
            start_index = section_matcher.end();

        }
    }

    /**
     * Return the parent section. We have a linked list from which we read from the last item to the first and find the
     * first occurrence of a section which has a level - 1 than the one we consider.
     *
     * @param prev_section_entries
     * @param level
     * @return
     */
    public String getParentSection(List<Map.Entry<Integer, String>> prev_section_entries, int level) {
        for (int j = prev_section_entries.size() - 1; j >= 0; j--) {
            Map.Entry<Integer, String> section_entry = prev_section_entries.get(j);
            if (section_entry.getKey() == level - 1) {
                return section_entry.getValue();
            }
        }
        return "MAIN_SECTION";
    }
}
