package entities;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Writable;
import utils.NLPUtils;
import utils.RevisionUtils;
import utils.WikiUtils;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WikiEntity implements Serializable, Writable {
    public long revision_id;
    public String user_id;
    public long entity_id;
    public String timestamp;
    public String title;
    public String content;

    public Map<Integer, Map<String, String>> entity_citations;

    //the sections in this Wikipedia entity page.
    public WikiSection root_sections;
    public Map<String, entities.WikiSectionSimple> sections;

    public WikiEntity() {
        root_sections = new entities.WikiSection();
        entity_citations = new HashMap<>();
        sections = new HashMap<>();
    }

    public void setSections(NLPUtils nlp) {
        Set<String> section_keys = getSectionKeys();
        for (String section_key : section_keys) {
            WikiSection section = getSection(section_key);
            entities.WikiSectionSimple section_simple = new entities.WikiSectionSimple();

            section_simple.section_text = section.section_text;

            //check for specific sections such as External Links and Notes
            if (section_key.equals("External links") || section_key.equals("Notes")) {
                section_simple.sentences = RevisionUtils.getSentences(section.section_text, false, nlp);
            } else {
                section_simple.sentences = RevisionUtils.getSentences(section.section_text, true, nlp);
            }
        }
        root_sections = null;
    }

    /**
     * Get a single section based on the section label. In case this section does not exist NULL is returned.
     *
     * @param section
     * @return
     */
    public entities.WikiSection getSection(String section) {
        return root_sections.findSection(section);
    }


    /**
     * Set the entity text for this entity. Here based on the given variables
     * (split_sections|extract_references|main_sections_only|clean_references)
     * we process the entity page like extracting the sections, the references,
     * and cleaning the content.
     *
     * @param wiki_content
     */
    public void setContent(String wiki_content) {
        //remove the infobox
        this.content = wiki_content;
        content = WikiUtils.removeInfoboxInformaion(content);

        //if we are interested in extracting the citations, do that here.
        content = WikiUtils.extractWikiReferences(content, entity_citations);

        splitEntityIntoSections(content);
    }


    /**
     * Return all the section labels.
     *
     * @return
     */
    public Set<String> getSectionKeys() {
        Set<String> keys = new HashSet<>();
        root_sections.getSectionKeys(keys);
        return keys;
    }

    /**
     * Splits the entity page text into chunks of text where each chunk belongs to a section. We begin with the main section
     * that is usually the introduction of an entity page. For the subsequent section we extract the text and in case
     * the section is a subsection we denote its parent.
     *
     * @param entity_text
     */
    public void splitEntityIntoSections(String entity_text) {
        //determine how to split the section text.
        Pattern section_pattern = Pattern.compile("={2,}(.*?)={2,}");
        Matcher section_matcher = section_pattern.matcher(entity_text);

        root_sections = new entities.WikiSection();
        root_sections.section_label = "MAIN_SECTION";

        String section_name = "MAIN_SECTION";

        int start_index = 0;
        int end_index = 0;
        int current_section_level = 0;

        List<Map.Entry<Integer, String>> prev_section_entries = new LinkedList<>();
        boolean has_sections = false;
        while (section_matcher.find()) {
            has_sections = true;
            String new_section = section_matcher.group();
            int tmp_current_section_level = StringUtils.countMatches(new_section, "=") / 2;
            new_section = new_section.replaceAll("=", "").trim();
            end_index = section_matcher.start();

            //the previous text is about the main section.
            if (!section_name.equals(new_section)) {
                //extract the citations and chunk the section text into paragraphs.
                String section_text = entity_text.substring(start_index, end_index);
                if (section_name.toLowerCase().contains("references") || section_name.toLowerCase().contains("notes") || section_name.toLowerCase().contains("see also")) {
                    continue;
                }

                entities.WikiSection section = new entities.WikiSection();
                section.section_label = section_name;
                section.section_text = section_text.replaceAll("\n+", "\n");

                if (current_section_level == 0 && section_name.equals("MAIN_SECTION")) {
                    root_sections = section;
                    root_sections.section_level = 1;

                    prev_section_entries.add(new AbstractMap.SimpleEntry<>(root_sections.section_level, root_sections.section_label));
                } else {
                    String parent_section_label = getParentSection(prev_section_entries, current_section_level);
                    entities.WikiSection root_section = entities.WikiSection.findSection(parent_section_label, root_sections, current_section_level);
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

        if (!has_sections) {
            root_sections.section_text = entity_text;
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
            if (section_entry.getKey() <= level - 1) {
                return section_entry.getValue();
            }
        }
        return "MAIN_SECTION";
    }


    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(this);

        byte[] bytes = b.toByteArray();
        System.out.printf("The object for entity %s is of length %d\n", title, bytes.length);
        return bytes;
    }

    public void readBytes(byte[] bytes) throws IOException {
        try {
            ObjectInputStream obj = new ObjectInputStream(new ByteArrayInputStream(bytes));
            WikiEntity rev = (WikiEntity) obj.readObject();

            System.out.printf("The object for entity %s is of length %d\n", rev.title, bytes.length);
            this.revision_id = rev.revision_id;
            this.user_id = rev.user_id;
            this.timestamp = rev.timestamp;
            this.entity_id = rev.entity_id;
            this.sections = rev.sections;
            this.title = rev.title;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        byte[] bytes = getBytes();
        out.writeInt(bytes.length);
        out.write(bytes, 0, bytes.length);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes, 0, length);
        readBytes(bytes);
    }
}
