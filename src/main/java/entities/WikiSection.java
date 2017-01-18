package entities;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by besnik on 9/17/16.
 */
public class WikiSection implements Serializable{
    public String section_label;
    public String section_text;
    public int section_level = 0;

    public List<WikiSection> child_sections;
    public Map<String, Integer> time_series;

    public WikiSection() {
        child_sections = new ArrayList<>();
        time_series = new HashMap<>();
    }

    /**
     * Returns the parent section.
     *
     * @param label
     * @param parent
     * @return
     */
    public static WikiSection findSection(String label, WikiSection parent, int level) {
        if (parent.section_level <= level - 1) {
            if (parent.section_level == level - 1 && parent.section_label.equals(label)) {
                return parent;
            } else if (!parent.child_sections.isEmpty()) {
                for (WikiSection child_section : parent.child_sections) {
                    WikiSection section = findSection(label, child_section, level);

                    if (section != null) {
                        return section;
                    }
                }
            }
        }

        return null;
    }

    /**
     * Check if it contains a specific section label.
     *
     * @param section
     * @return
     */
    public boolean hasSection(String section) {
        if (this.section_label.equals(section)) {
            return true;
        }

        if (this.child_sections.isEmpty()) {
            return false;
        }

        for (WikiSection child_section : child_sections) {
            boolean has_section = child_section.hasSection(section);
            if (has_section) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return a specific section object.
     *
     * @param label
     * @return
     */
    public WikiSection findSection(String label) {
        if (section_label.equals(label)) {
            return this;
        } else if (!child_sections.isEmpty()) {
            for (WikiSection child_section : child_sections) {
                WikiSection section = child_section.findSection(label);
                if (section != null) {
                    return section;
                }
            }
        }
        return null;
    }

    /**
     * Gets all the section keys.
     *
     * @param keys
     */
    public void getSectionKeys(Set<String> keys) {
        keys.add(section_label);

        if (!child_sections.isEmpty()) {
            for (WikiSection child_section : child_sections) {
                child_section.getSectionKeys(keys);
            }
        }
    }


    /**
     * Gets all the section keys.
     *
     * @param keys
     */
    public void getSectionKeys(Set<String> keys, int level) {
        if (section_level > level) {
            return;
        }
        keys.add(section_label);

        if (!child_sections.isEmpty()) {
            for (WikiSection child_section : child_sections) {
                child_section.getSectionKeys(keys, level);
            }
        }
    }

    /**
     * Computes the time series of a specific section.
     *
     * @param temporal_regex
     */
    public void computeTimeSeries(Pattern[] temporal_regex) {
        for (int i = 0; i < temporal_regex.length; i++) {
            Matcher m = temporal_regex[i].matcher(section_text);
            while (m.find()) {
                String expr = m.group();
                Integer count = time_series.get(expr);
                count = count == null ? 0 : count;
                count += 1;

                time_series.put(expr, count);
            }
        }

        if (!child_sections.isEmpty()) {
            for (WikiSection child_section : child_sections) {
                child_section.computeTimeSeries(temporal_regex);
            }
        }
    }

    /**
     * Prints the time series.
     *
     * @param sb
     */
    public void printTimeSeries(StringBuffer sb) {
        if (!child_sections.isEmpty()) {
            for (WikiSection child_section : child_sections) {
                child_section.printTimeSeries(sb);
            }
        }

        for (String expr : time_series.keySet()) {
            sb.append(section_label).append("\t").append(expr).append("\t").append(time_series.get(expr)).append("\n");
        }
    }

    /**
     * Print the ground truth mapping between the topic hashtags for an entity and the respective sections.
     *
     * @param tags
     * @param threshold
     * @param sb
     */
    public void printSectionHashtagMapping(Map<String, Integer> tags, int threshold, StringBuffer sb, int section_level_threshold) {
        if (section_level > section_level_threshold) {
            return;
        }
        for (String tag : tags.keySet()) {
            int tag_count = tags.get(tag);
            if (tag_count < threshold) {
                continue;
            }

            sb.append(StringUtils.repeat(" ", this.section_level)).append(section_label).append("\t").append(tag).append("\n");
        }

        if (!child_sections.isEmpty() && (section_level + 1) < section_level_threshold) {
            for (WikiSection child_section : child_sections) {
                child_section.printSectionHashtagMapping(tags, threshold, sb, section_level_threshold);
            }
        }
    }

    @Override
    public String toString() {
        return StringUtils.repeat("=", section_level) + section_label + StringUtils.repeat("=", section_level);
    }

}
