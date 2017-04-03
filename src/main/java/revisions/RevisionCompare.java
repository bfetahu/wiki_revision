package revisions;

import entities.WikiSection;
import entities.WikipediaEntity;
import entities.WikipediaEntityRevision;
import org.apache.commons.lang3.StringEscapeUtils;
import utils.NLPUtils;
import utils.SimilarityMeasures;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Created by hube on 1/20/2017.
 */
public class RevisionCompare {

    NLPUtils nlp = new NLPUtils(2);
    Set<String> stopWords = new HashSet<String>();
    double JACCARD_THRESHOLD_SECTION = 0.1;
    double JACCARD_THRESHOLD_SENTENCE = 0.3;


    public RevisionCompare(String stop_words) throws IOException {
        String[] words = stop_words.split("\n");
        for (String s : words) {
            stopWords.add(s);
        }
        SimilarityMeasures.stop_words = stopWords;
    }


    public String compareWithOldRevision(WikipediaEntityRevision revision, WikipediaEntityRevision lastRevision, boolean firstRevision) throws IOException {
        if (firstRevision) {
            WikipediaEntity emptyEntity = new WikipediaEntity();
            emptyEntity.setSplitSections(true);

            //set content to empty

            emptyEntity.setContent(new StringBuffer().append("{{Infobox aircraft occurrence\n").append("}}\n").append("==References==\n").append("{{reflist}}").toString());
            lastRevision.entity = emptyEntity;
        }

        String revision_string = new StringBuffer().append("https://en.wikipedia.org/w/index.php?diff=").append(revision.revision_id).append("&oldid=").append(lastRevision.revision_id).toString();

        //map old section to new section, identify added and removed sections, result: section mappings
        Map<String, Set<String>> oldSectionsToNewSections = new HashMap<>();
        Map<String, Set<String>> newSectionsToOldSections = new HashMap<>();

        computeSectionMappings(revision, lastRevision, newSectionsToOldSections);
        computeSectionMappings(lastRevision, revision, oldSectionsToNewSections);

        //for every section: map old sentences to new sentences, identify added and removed sentences
        Map<String, List<Map.Entry<String, String>>> sectionToContentAdded = new HashMap<>();
        Map<String, List<Map.Entry<String, String>>> sectionToContentRemoved = new HashMap<>();
        Map<String, Set<String>> sectionToReferencesAdded = new HashMap<>();
        Map<String, Set<String>> sectionToReferencesRemoved = new HashMap<>();

        computeContentDifferences(newSectionsToOldSections, revision, lastRevision, sectionToContentAdded, sectionToReferencesAdded);
        computeContentDifferences(oldSectionsToNewSections, lastRevision, revision, sectionToContentRemoved, sectionToReferencesRemoved);

        String output_string = "";

        //output changes in all sections
        for (String section : newSectionsToOldSections.keySet()) {
            output_string = output_string + outputChanges(section, sectionToContentAdded, sectionToContentRemoved,
                    sectionToReferencesAdded, sectionToReferencesRemoved, false, revision, revision_string);
        }

        //check for deleted sections
        for (String section : oldSectionsToNewSections.keySet()) {
            if (!newSectionsToOldSections.keySet().contains(section)) {
                //deleted section
                output_string = output_string + outputChanges(section, sectionToContentAdded, sectionToContentRemoved,
                        sectionToReferencesAdded, sectionToReferencesRemoved, true, revision, revision_string);
            }
        }

        return output_string;
    }


    public String outputChanges(String section,
                                Map<String, List<Map.Entry<String, String>>> sectionToContentAdded,
                                Map<String, List<Map.Entry<String, String>>> sectionToContentRemoved,
                                Map<String, Set<String>> sectionToReferencesAdded,
                                Map<String, Set<String>> sectionToReferencesRemoved,
                                boolean deletedSection, WikipediaEntityRevision revision, String revision_string) throws IOException {

        String output_for_section = "";

        //create content strings

        String content_added_string = "";
        if (sectionToContentAdded.containsKey(section)) {
            content_added_string = "[";
            for (Map.Entry<String, String> sentence_mapping : sectionToContentAdded.get(section)) {
                String sentence = sentence_mapping.getKey();
                String similar_sentence = "";
                try {
                    similar_sentence = sentence_mapping.getValue();
                } catch (NullPointerException e) {

                }
                if (!content_added_string.equals("[")) {
                    content_added_string = content_added_string + ";";
                }
                if (similar_sentence == null) {
                    content_added_string = content_added_string + sentence;
                } else {
                    content_added_string = content_added_string + similar_sentence + " --> " + sentence;
                }
            }
            content_added_string = content_added_string + "]";
        } else {
            content_added_string = "[]";
        }

        String content_removed_string = "";
        if (sectionToContentRemoved.containsKey(section)) {
            content_removed_string = "[";
            for (Map.Entry<String, String> sentence_mapping : sectionToContentRemoved.get(section)) {
                String sentence = sentence_mapping.getKey();
                String similar_sentence = "";
                try {
                    similar_sentence = sentence_mapping.getValue();
                } catch (NullPointerException e) {

                }
                if (!content_removed_string.equals("[") && similar_sentence == null) {
                    content_removed_string = content_removed_string + ";";
                }
                if (similar_sentence == null) {
                    content_removed_string = content_removed_string + sentence;
                }
            }
            content_removed_string = content_removed_string + "]";
        } else {
            content_removed_string = "[]";
        }

        if (!sectionToReferencesAdded.containsKey(section)) {
            Set<String> set = new HashSet<>();
            sectionToReferencesAdded.put(section, set);
        }
        if (!sectionToReferencesRemoved.containsKey(section)) {
            Set<String> set = new HashSet<>();
            sectionToReferencesRemoved.put(section, set);
        }

        if (content_added_string.length() > 2 || content_removed_string.length() > 2
                || sectionToReferencesAdded.get(section).size() > 0 || sectionToReferencesAdded.get(section).size() > 0) {

            String section_string = "";
            if (deletedSection) {
                section_string = section + " (deleted section)";
            } else {
                section_string = section;
            }

            output_for_section = revision.user_id + "\t" + revision.user_name + "\t" + revision.entity_id + "\t" + revision.revision_id + "\t"
                    + revision.timestamp + "\t" + revision.revision_comment + "\t" + section_string + "\t" + content_added_string
                    + "\t" + content_removed_string + "\t" + sectionToReferencesAdded.get(section)
                    + "\t" + sectionToReferencesRemoved.get(section) + "\t" + revision_string + "\n";
        }
        return output_for_section;
    }


    public void computeSectionMappings(WikipediaEntityRevision revision, WikipediaEntityRevision compared_revision, Map<String, Set<String>> sectionMapping) {
        //get sections for both revisions
        Set<String> revision_sections = revision.entity.getSectionKeys();
        Set<String> compared_revision_sections = compared_revision.entity.getSectionKeys();

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
                    double jacdis = SimilarityMeasures.computeJaccardDistance(revision.entity.getSectionText(section), compared_revision.entity.getSectionText(compared_section));
                    if (jacdis > JACCARD_THRESHOLD_SECTION) {
                        sectionMapping.get(section).add(compared_section);
                    }
                }
            }
        }
    }


    public void computeContentDifferences(Map<String, Set<String>> sectionsToSections, WikipediaEntityRevision revision, WikipediaEntityRevision comparedRevision,
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

    public Set<String> computeIntersectionSet(Map<String, Set<String>> differences_similarSections) {
        Set<String> differences_intersection = new HashSet<>();

        try {
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
        } catch (NullPointerException e) {

        }
        return differences_intersection;
    }


    //computes differences between two sections
    public void compareSections(String section, String comparedSection, WikipediaEntityRevision revision, WikipediaEntityRevision comparedRevision,
                                Map<String, List<Map.Entry<String, String>>> sectionToContentDifferences, Map<String, Set<String>> sectionToReferenceDifferences) {
        if (revision.entity.getSection(section) == null || comparedRevision.entity.getSection(comparedSection) == null) {
            return;
        }

        List<Map.Entry<String, String>> contentDifferences = new ArrayList<>();
        Set<String> referenceDifferences = new HashSet<>();

        WikiSection wiki_section = revision.entity.getSection(section);
        WikiSection wiki_section_compare = comparedRevision.entity.getSection(comparedSection);
        ;
        List<String> section_sentences = wiki_section.sentences;
        List<String> compared_section_sentences = wiki_section_compare.sentences;

        //content differences
        for (String sentence : section_sentences) {
            if (!compared_section_sentences.contains(sentence)) {
                //check if there is a similar sentence
                String similarSentence = findSimilarSentences(sentence, compared_section_sentences);
                if (similarSentence.isEmpty()) {
                    //sentence has been added or removed
                    contentDifferences.add(new AbstractMap.SimpleEntry<>(sentence, null));
                } else {
                    //part of sentence is new, compare bag of words
                    contentDifferences.add(new AbstractMap.SimpleEntry<>(sentence, similarSentence));
                }
            }
        }
        sectionToContentDifferences.put(section, contentDifferences);

        //reference differences
        Set<String> urls = wiki_section.getSectionCitations().values().stream().filter(c -> c.containsKey("url")).map(x -> x.get("url")).collect(Collectors.toSet());
        Set<String> compared_urls = wiki_section_compare.getSectionCitations().values().stream().filter(c -> c.containsKey("url")).map(x -> x.get("url")).collect(Collectors.toSet());
        for (String url : urls) {
            if (!compared_urls.contains(url)) {
                referenceDifferences.add(url);
            }
        }
        sectionToReferenceDifferences.put(section, referenceDifferences);
    }


    public String findSimilarSentences(String sentence, List<String> sentenceSet) {
        String mostSimilarSentence = "";
        double mostSimilarSentenceSimilarity = 0.0;

        for (String candidate_sentence : sentenceSet) {
            double jacdis = SimilarityMeasures.computeJaccardDistance(sentence, candidate_sentence);

            if (jacdis > JACCARD_THRESHOLD_SENTENCE && jacdis > mostSimilarSentenceSimilarity) {
                mostSimilarSentence = candidate_sentence;
                mostSimilarSentenceSimilarity = jacdis;
            }
        }
        return mostSimilarSentence;
    }


    /**
     * Construct the sentence list for each entity sections.
     *
     * @param text
     * @return
     */
    public List<String> getSentences(String text) {
        List<String> sentences = new ArrayList<>();

        text = StringEscapeUtils.escapeJson(text);

        //cleaning
        text = text.replace("...", ".");
        text = text.replace("\n\n", ".");

        text = text.replace("�", "");
        text = text.replaceAll("\\{\\{[0-9]+\\}\\}", "");
        text = text.replaceAll("â€", "");
        text = text.replaceAll("\\[|\\]", "");

        for (String sentence : nlp.getDocumentSentences(text)) {
            sentence = sentence.replace("\\n", "");
            if (sentence.length() > 3) {
                sentences.add(sentence);
            }
        }
        return sentences;
    }
}

