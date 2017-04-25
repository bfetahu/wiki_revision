package revisions;

import entities.WikiEntity;
import entities.WikiSectionSimple;
import gnu.trove.set.hash.TIntHashSet;
import utils.RevisionUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Created by hube on 1/20/2017.
 */
public class RevisionCompare {

    public static final double JACCARD_THRESHOLD_SECTION = 0.1;
    public static final double JACCARD_THRESHOLD_SENTENCE = 0.3;

    //map old section to new section, identify added and removed sections, result: section mappings
    public Map<String, List<Map.Entry<String, Double>>> old_to_new_sections = new HashMap<>();
    public Map<String, List<Map.Entry<String, Double>>> new_to_old_sections = new HashMap<>();

    //for every section: map old sentences to new sentences, identify added and removed sentences
    public Map<String, List<Map.Entry<String, String>>> section_content_added = new HashMap<>();
    public Map<String, List<Map.Entry<String, String>>> section_content_removed = new HashMap<>();
    public Map<String, Set<String>> section_ref_added = new HashMap<>();
    public Map<String, Set<String>> section_ref_removed = new HashMap<>();


    /**
     * Compare revisions.
     *
     * @param revision
     * @param prev_revision
     * @param is_first_rev
     * @return
     * @throws IOException
     */
    public String compareWithOldRevision(WikiEntity revision, WikiEntity prev_revision, boolean is_first_rev) throws IOException {
        //handle first version of article
        clearDataStructures();
        StringBuffer sb = new StringBuffer();
        if (is_first_rev) {
            return outputInitialRevision(revision);
        }

        RevisionUtils.computeSectionMappings(revision, prev_revision, new_to_old_sections, JACCARD_THRESHOLD_SECTION);
        RevisionUtils.computeSectionMappings(prev_revision, revision, old_to_new_sections, JACCARD_THRESHOLD_SECTION);

        computeContentDifferences(new_to_old_sections, revision, prev_revision, section_content_added, section_ref_added);
        computeContentDifferences(old_to_new_sections, prev_revision, revision, section_content_removed, section_ref_removed);

        //output changes in all sections
        for (String section : new_to_old_sections.keySet()) {
            sb.append(outputChanges(section, section_content_added, section_content_removed, section_ref_added, section_ref_removed, false, revision));
        }

        //check for deleted sections
        for (String section : old_to_new_sections.keySet()) {
            if (!new_to_old_sections.keySet().contains(section)) {
                //deleted section
                sb.append(outputChanges(section, section_content_added, section_content_removed, section_ref_added, section_ref_removed, true, revision));
            }
        }

        return sb.toString();
    }


    /**
     * Clear the data structures from the previous entries.
     */
    public void clearDataStructures() {
        section_content_added.clear();
        section_ref_added.clear();
        old_to_new_sections.clear();
        new_to_old_sections.clear();
        section_content_added.clear();
        section_content_removed.clear();
        section_ref_added.clear();
        section_ref_removed.clear();
    }

    /**
     * Output the changes from the initial version.
     *
     * @param revision
     * @return
     */
    private String outputInitialRevision(WikiEntity revision) {
        StringBuffer sb = new StringBuffer();
        for (String section : revision.sections.keySet()) {
            //sentences
            StringBuffer sb_content_added_string = new StringBuffer();
            for (String sentence : revision.sections.get(section).sentences) {
                if (sb_content_added_string.length() != 0) {
                    sb_content_added_string.append("<d>;<d>");
                }
                sb_content_added_string.append(sentence);
            }
            sb.append(revision.user_id).append("\t").append(revision.entity_id).append("\t").append(revision.title).
                    append("\t").append(revision.revision_id).append("\t").append(revision.timestamp).
                    append("\t").append(section).append("\t").append(sb_content_added_string.toString()).
                    append("\t[]\t").append(revision.sections.get(section).urls).
                    append("\t[]\n");
        }
        return sb.toString();
    }

    /**
     * Output the changes between two entity revisions.
     *
     * @param section
     * @param section_added_content
     * @param section_removed_content
     * @param section_ref_added
     * @param section_ref_removed
     * @param deleted_section
     * @param revision
     * @return
     * @throws IOException
     */
    public String outputChanges(String section,
                                Map<String, List<Map.Entry<String, String>>> section_added_content,
                                Map<String, List<Map.Entry<String, String>>> section_removed_content,
                                Map<String, Set<String>> section_ref_added,
                                Map<String, Set<String>> section_ref_removed,
                                boolean deleted_section, WikiEntity revision) throws IOException {
        //create content strings
        StringBuffer content_bf = new StringBuffer();
        if (section_added_content.containsKey(section)) {
            section_removed_content.get(section).forEach(s -> content_bf.append(s.getKey()).append("<SD>").append(s.getKey()).append(";"));
        } else content_bf.append("\t");

        if (section_removed_content == null) content_bf.append("\t");
        else {
            if (section_removed_content.containsKey(section)) {
                section_removed_content.get(section).forEach(s -> content_bf.append(s.getKey()).append("<SD>").append(s.getKey()).append(";"));
            } else content_bf.append("\t");
        }

        String section_string = deleted_section ? section + " (DELETED SECTION)" : section;
        StringBuffer sb = new StringBuffer();
        sb.append(revision.user_id).append("\t").append(revision.entity_id).append("\t").append(revision.title).
                append("\t").append(revision.revision_id).append("\t").append(revision.timestamp).
                append("\t").append(section_string).append("\t").append(content_bf).
                append("\t").append(section_ref_added.get(section)).
                append("\t").append(section_ref_removed.get(section)).append("\n");
        return sb.toString();
    }


    /**
     * Compute the differences between two sections based on the content and references.
     *
     * @param section_mapping
     * @param rev_a
     * @param rev_b
     * @param section_content_diff
     * @param section_ref_diff
     */
    public void computeContentDifferences(Map<String, List<Map.Entry<String, Double>>> section_mapping,
                                          WikiEntity rev_a, WikiEntity rev_b,
                                          Map<String, List<Map.Entry<String, String>>> section_content_diff,
                                          Map<String, Set<String>> section_ref_diff) {
        for (String section : section_mapping.keySet()) {
            if (section_mapping.get(section).size() == 1) {
                compareSections(section, section_mapping.get(section).get(0).getKey(), rev_a, rev_b, section_content_diff, section_ref_diff);
            } else if (section_mapping.get(section).size() > 1) {
                Map<String, List<Map.Entry<String, String>>> content_sim_sections = new HashMap<>();
                Map<String, Set<String>> ref_diff_sections = new HashMap<>();

                //intersection of all comparison results
                for (Map.Entry<String, Double> section_entry : section_mapping.get(section)) {
                    Map<String, List<Map.Entry<String, String>>> content_diff = new HashMap<>();
                    Map<String, Set<String>> ref_diff = new HashMap<>();
                    compareSections(section, section_entry.getKey(), rev_a, rev_b, content_diff, ref_diff);

                    content_sim_sections.put(section_entry.getKey(), content_diff.get(section));
                    ref_diff_sections.put(section_entry.getKey(), ref_diff.get(section));
                }

                List<Map.Entry<String, String>> content_diff_intersection = RevisionUtils.computeIntersection(content_sim_sections);
                Set<String> ref_diff_intersection = RevisionUtils.computeIntersectionSet(ref_diff_sections);

                section_content_diff.put(section, content_diff_intersection);
                section_ref_diff.put(section, ref_diff_intersection);
            }
        }
    }


    /**
     * Compute the difference between two sections across two different revisions.
     *
     * @param section_a
     * @param section_b
     * @param rev_a
     * @param rev_b
     * @param section_content_diff
     * @param section_ref_diff
     */
    public void compareSections(String section_a, String section_b, WikiEntity rev_a, WikiEntity rev_b,
                                Map<String, List<Map.Entry<String, String>>> section_content_diff,
                                Map<String, Set<String>> section_ref_diff) {
        if (rev_a.sections.get(section_a) == null || rev_b.sections.get(section_b) == null) {
            return;
        }

        List<Map.Entry<String, String>> content_diff = new ArrayList<>();

        WikiSectionSimple wiki_section_a = rev_a.sections.get(section_a);
        WikiSectionSimple wiki_section_b = rev_b.sections.get(section_b);

        //content differences
        for (int i = 0; i < wiki_section_a.sentences.size(); i++) {
            TIntHashSet sentence_bow = wiki_section_a.sentence_bow[i];
            String sentence = wiki_section_a.sentences.get(i);
            if (!wiki_section_b.sentences.contains(sentence)) {
                //check if there is a similar sentence
                int max_sim_sentence = RevisionUtils.findMaxSimSentence(sentence_bow, wiki_section_b.sentence_bow, JACCARD_THRESHOLD_SENTENCE);
                if (max_sim_sentence == -1) {
                    //sentence has been added or removed
                    content_diff.add(new AbstractMap.SimpleEntry<>(sentence, null));
                } else {
                    //part of sentence is new, compare bag of words
                    content_diff.add(new AbstractMap.SimpleEntry<>(sentence, wiki_section_b.sentences.get(max_sim_sentence)));
                }
            }
        }
        section_content_diff.put(section_a, content_diff);

        //reference differences
        Set<String> ref_diff = wiki_section_a.urls.stream().filter(s -> !wiki_section_b.urls.contains(s)).collect(Collectors.toSet());
        section_ref_diff.put(section_a, ref_diff);
    }
}