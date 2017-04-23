package revisions;

import entities.WikiEntity;
import entities.WikiSectionSimple;
import gnu.trove.set.hash.TIntHashSet;

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
    public Map<String, Set<String>> old_to_new_sections = new HashMap<>();
    public Map<String, Set<String>> new_to_old_sections = new HashMap<>();

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

        computeSectionMappings(revision, prev_revision, new_to_old_sections);
        computeSectionMappings(prev_revision, revision, old_to_new_sections);

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


    public String outputChanges(String section,
                                Map<String, List<Map.Entry<String, String>>> sectionToContentAdded,
                                Map<String, List<Map.Entry<String, String>>> sectionToContentRemoved,
                                Map<String, Set<String>> sectionToReferencesAdded,
                                Map<String, Set<String>> sectionToReferencesRemoved,
                                boolean deletedSection, WikiEntity revision) throws IOException {

        String output_for_section = "";

        //create content strings
        StringBuffer content_added_string = new StringBuffer();
        if (sectionToContentAdded.containsKey(section)) {
            content_added_string.append("[");

            for (Map.Entry<String, String> sentence_mapping : sectionToContentAdded.get(section)) {
                String sentence = sentence_mapping.getKey();
                String similar_sentence = sentence_mapping.getValue() == null ? null : sentence_mapping.getValue();

                if (!content_added_string.equals("[")) content_added_string.append(";");
                if (similar_sentence == null) content_added_string.append(sentence);
                else content_added_string.append(similar_sentence).append(" --> ").append(sentence);
            }
            content_added_string.append("]");
        } else {
            content_added_string.append("[]");
        }

        StringBuffer content_removed_string = new StringBuffer();
        if (sectionToContentRemoved == null) content_removed_string.append("[]");
        else {
            if (sectionToContentRemoved.containsKey(section)) {
                content_removed_string.append("[");
                for (Map.Entry<String, String> sentence_mapping : sectionToContentRemoved.get(section)) {
                    String sentence = sentence_mapping.getKey();
                    String similar_sentence = sentence_mapping.getValue() == null ? "" : sentence_mapping.getValue();

                    if (content_removed_string.length() != 0 && similar_sentence == null) content_removed_string.append(";");
                    if (similar_sentence == null) content_removed_string.append(sentence);
                }
                content_removed_string.append("]");
            } else content_removed_string.append("[]");
        }

        if (!sectionToReferencesAdded.containsKey(section)) {
            sectionToReferencesAdded.put(section, new HashSet<>());
        }
        StringBuffer references_removed_string = new StringBuffer();
        if (sectionToReferencesRemoved == null || !sectionToReferencesRemoved.containsKey(section))
            references_removed_string.append("[]");
        else {
            references_removed_string.append("[");
            for (String reference : sectionToReferencesRemoved.get(section)) {
                if (references_removed_string.length() != 0) {
                    references_removed_string.append(",");
                }
                references_removed_string.append(reference);
            }
            references_removed_string.append("]");
        }

        if (content_added_string.length() > 2 || content_removed_string.length() > 2 ||
                sectionToReferencesAdded.get(section).size() > 0 || sectionToReferencesAdded.get(section).size() > 0) {
            String section_string = "";
            if (deletedSection) {
                section_string = section + " (deleted section)";
            } else {
                section_string = section;
            }

            StringBuffer sb = new StringBuffer();
            sb.append(revision.user_id).append("\t").append(revision.entity_id).append("\t").append(revision.title).
                    append("\t").append(revision.revision_id).append("\t").append(revision.timestamp).
                    append("\t").append(section_string).append("\t").append(content_added_string).
                    append("\t").append(content_removed_string).append("\t").append(sectionToReferencesAdded.get(section)).
                    append("\t").append(references_removed_string).append("\n");
            output_for_section = sb.toString();
        }
        return output_for_section;
    }


    public void computeSectionMappings(WikiEntity revision, WikiEntity compared_revision, Map<String, Set<String>> sectionMapping) {
        //get sections for both revisions
        Set<String> revision_sections = revision.sections.keySet();
        Set<String> compared_revision_sections = compared_revision.sections.keySet();

        //check for differences between revisions
        for (String section : revision_sections) {
            if (compared_revision_sections.contains(section)) {
                //section name is the same
                Set<String> set = new HashSet<>();
                set.add(section);
                sectionMapping.put(section, set);
            } else {
                Set<String> set = new HashSet<>();
                sectionMapping.put(section, set);

                //check if section name has changed
                for (String compared_section : compared_revision_sections) {
                    //rename?
                    if (revision_sections.contains(compared_section)) {
                        continue;
                    }

                    //compute similarity between sections
                    double jacdis = computeJaccardDistance(revision.sections.get(section).section_bow, compared_revision.sections.get(compared_section).section_bow);
                    if (jacdis > JACCARD_THRESHOLD_SECTION) {
                        sectionMapping.get(section).add(compared_section);
                    }
                }
            }
        }
    }


    public void computeContentDifferences(Map<String, Set<String>> sectionsToSections, WikiEntity revision, WikiEntity comparedRevision,
                                          Map<String, List<Map.Entry<String, String>>> sectionToContentDifferences,
                                          Map<String, Set<String>> sectionToReferenceDifferences) {
        for (String section : sectionsToSections.keySet()) {
            //compare with mapped sections, for now only with top section

            //only one section to compare
            if (sectionsToSections.get(section).size() == 1) {
                compareSections(section, sectionsToSections.get(section).iterator().next(), revision, comparedRevision, sectionToContentDifferences, sectionToReferenceDifferences);
            }

            //no section to compare
            if (sectionsToSections.get(section).size() == 0) {
                //new section
                compareSections(section, "", revision, comparedRevision, sectionToContentDifferences, sectionToReferenceDifferences);
            }

            //multiple sections to compare
            if (sectionsToSections.get(section).size() > 1) {
                Map<String, List<Map.Entry<String, String>>> contentDifferences_similarSections = new HashMap<>();
                Map<String, Set<String>> newsRefDifferences_similarSections = new HashMap<>();

                //intersection of all comparison results
                for (String similarSection : sectionsToSections.get(section)) {
                    Map<String, List<Map.Entry<String, String>>> contentDifferences = new HashMap<>();
                    Map<String, Set<String>> refDifferences = new HashMap<>();
                    compareSections(section, similarSection, revision, comparedRevision, contentDifferences, refDifferences);

                    contentDifferences_similarSections.put(similarSection, contentDifferences.get(section));
                    newsRefDifferences_similarSections.put(similarSection, refDifferences.get(section));
                }

                List<Map.Entry<String, String>> contentDifferences_intersection = computeIntersection(contentDifferences_similarSections);
                Set<String> newsRefDifferences_intersection = computeIntersectionSet(newsRefDifferences_similarSections);

                sectionToContentDifferences.put(section, contentDifferences_intersection);
                sectionToReferenceDifferences.put(section, newsRefDifferences_intersection);
            }
        }
    }


    public List<Map.Entry<String, String>> computeIntersection(Map<String, List<Map.Entry<String, String>>> differences_similarSections) {
        List<Map.Entry<String, String>> differences_intersection = new ArrayList<>();
        String first_section = differences_similarSections.keySet().iterator().next();
        for (Map.Entry<String, String> difference : differences_similarSections.get(first_section)) { //pick one random section, check if all other sections contain the same difference
            boolean difference_in_all_sections = true;
            for (String involved_section : differences_similarSections.keySet()) {
                for (Map.Entry<String, String> involved_sentence : differences_similarSections.get(involved_section)) {
                    if (!involved_sentence.equals(difference.getKey())) {
                        difference_in_all_sections = false;
                        break;
                    }
                }
            }
            if (difference_in_all_sections) {
                differences_intersection.add(difference);
            }
        }
        return differences_intersection;
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


    //computes differences between two sections
    public void compareSections(String section, String comparedSection, WikiEntity revision, WikiEntity comparedRevision,
                                Map<String, List<Map.Entry<String, String>>> sectionToContentDifferences, Map<String, Set<String>> sectionToReferenceDifferences) {
        if (revision.sections.get(section) == null || comparedRevision.sections.get(comparedSection) == null) {
            return;
        }

        List<Map.Entry<String, String>> contentDifferences = new ArrayList<>();

        WikiSectionSimple wiki_section = revision.sections.get(section);
        WikiSectionSimple wiki_section_compare = comparedRevision.sections.get(comparedSection);

        List<String> section_sentences = wiki_section.sentences;
        List<String> compared_section_sentences = wiki_section_compare.sentences;

        //content differences
        for (int i = 0; i < section_sentences.size(); i++) {
            TIntHashSet sentence_bow = wiki_section.sentence_bow[i];
            String sentence = section_sentences.get(i);
            if (!compared_section_sentences.contains(sentence)) {
                //check if there is a similar sentence
                int max_sim_sentence = findSimilarSentences(sentence_bow, wiki_section_compare.sentence_bow);
                if (max_sim_sentence == -1) {
                    //sentence has been added or removed
                    contentDifferences.add(new AbstractMap.SimpleEntry<>(sentence, null));
                } else {
                    //part of sentence is new, compare bag of words
                    contentDifferences.add(new AbstractMap.SimpleEntry<>(sentence, compared_section_sentences.get(max_sim_sentence)));
                }
            }
        }
        sectionToContentDifferences.put(section, contentDifferences);

        //reference differences
        Set<String> referenceDifferences = wiki_section.urls.stream().filter(s -> !wiki_section_compare.urls.contains(s)).collect(Collectors.toSet());
        sectionToReferenceDifferences.put(section, referenceDifferences);
    }


    public  static int findSimilarSentences(TIntHashSet sentence, TIntHashSet[] sentenceSet) {
        double mostSimilarSentenceSimilarity = 0.0;
        int index = -1;
        for (int i = 0; i < sentenceSet.length; i++) {
            double jacdis = computeJaccardDistance(sentence, sentenceSet[i]);
            if (jacdis > JACCARD_THRESHOLD_SENTENCE && jacdis > mostSimilarSentenceSimilarity) {

                mostSimilarSentenceSimilarity = jacdis;
                index = i;
            }
        }
        return index;
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
}
