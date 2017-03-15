package revisions;

import entities.WikiSection;
import entities.WikipediaEntity;
import entities.WikipediaEntityRevision;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import utils.NLPUtils;
import utils.SimilarityMeasures;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;


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
            emptyEntity.setContent("{{Infobox aircraft occurrence\n" +
                    "}}\n" +
                    "==References==\n" +
                    "{{reflist}}");
            lastRevision.entity = emptyEntity;
        }

        String revision_string = "https://en.wikipedia.org/w/index.php?diff=" + revision.revision_id
                + "&oldid=" + lastRevision.revision_id;

        //map old section to new section, identify added and removed sections, result: section mappings
        Map<String, Set<String>> oldSectionsToNewSections = new HashMap<>();
        Map<String, Set<String>> newSectionsToOldSections = new HashMap<>();

        computeSectionMappings(lastRevision, revision, oldSectionsToNewSections);
        computeSectionMappings(revision, lastRevision, newSectionsToOldSections);

        //for every section: map old sentences to new sentences, identify added and removed sentences
        Map<String, List<Map.Entry<String, String>>> sectionToContentAdded = new HashMap<>();
        Map<String, List<Map.Entry<String, String>>> sectionToContentRemoved = new HashMap<>();
        Map<String, Set<String>> sectionToNewsReferencesAdded = new HashMap<>();
        Map<String, Set<String>> sectionToNewsReferencesRemoved = new HashMap<>();
        Map<String, Set<String>> sectionToOtherReferencesAdded = new HashMap<>();
        Map<String, Set<String>> sectionToOtherReferencesRemoved = new HashMap<>();

        computeContentDifferences(newSectionsToOldSections, revision, lastRevision, sectionToContentAdded, sectionToNewsReferencesAdded, sectionToOtherReferencesAdded);
        computeContentDifferences(oldSectionsToNewSections, lastRevision, revision, sectionToContentRemoved, sectionToNewsReferencesRemoved, sectionToOtherReferencesRemoved);

        String output_string = "";

        //output changes in all sections
        for (String section : newSectionsToOldSections.keySet()){
            output_string = output_string + outputChanges(section, sectionToContentAdded, sectionToContentRemoved,
                    sectionToNewsReferencesAdded, sectionToNewsReferencesRemoved,
                    sectionToOtherReferencesAdded, sectionToOtherReferencesRemoved,
                    false, revision) + "\n";
        }

        //check for deleted sections
        for (String section : oldSectionsToNewSections.keySet()){
            if (!newSectionsToOldSections.keySet().contains(section)){
                //deleted section
                output_string = output_string + outputChanges(section, sectionToContentAdded, sectionToContentRemoved,
                        sectionToNewsReferencesAdded, sectionToNewsReferencesRemoved,
                        sectionToOtherReferencesAdded, sectionToOtherReferencesRemoved,
                        true, revision) + "\n";
            }
        }

        output_string = output_string + revision_string + "\n";

        return output_string;
    }


    public String outputChanges(String section,
                                Map<String, List<Map.Entry<String, String>>> sectionToContentAdded,
                                Map<String, List<Map.Entry<String, String>>> sectionToContentRemoved,
                                Map<String, Set<String>> sectionToNewsReferencesAdded,
                                Map<String, Set<String>> sectionToNewsReferencesRemoved,
                                Map<String, Set<String>> sectionToOtherReferencesAdded,
                                Map<String, Set<String>> sectionToOtherReferencesRemoved,
                              boolean deletedSection, WikipediaEntityRevision revision) throws IOException {

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
                if (content_added_string.equals("[")) {
                    content_added_string = content_added_string + similar_sentence + " --> " + sentence;
                } else {
                    content_added_string = content_added_string + ";" + similar_sentence + " --> " + sentence;
                }
            }
            content_added_string = content_added_string + "]";
        } else{
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
                if (content_removed_string.equals("[")) {
                    content_removed_string = content_removed_string + similar_sentence + " --> " + sentence;
                } else {
                    content_removed_string = content_removed_string + ";" + similar_sentence + " --> " + sentence;
                }
            }
            content_removed_string = content_removed_string + "]";
        } else{
            content_removed_string = "[]";
        }

        if (sectionToContentAdded.get(section).size() > 0 || sectionToContentRemoved.get(section).size() > 0
                || sectionToNewsReferencesAdded.get(section).size() > 0 || sectionToNewsReferencesAdded.get(section).size() > 0
                || sectionToOtherReferencesAdded.get(section).size() > 0 || sectionToOtherReferencesAdded.get(section).size() > 0) {

            String section_string = "";
            if (deletedSection) {
                section_string = section + " (deleted section)";
            } else {
                section_string = section;
            }

            output_for_section = revision.user_id + "\t" + revision.user_name + "\t" + revision.entity_id + "\t" + revision.revision_id + "\t"
                    + revision.timestamp + "\t" + revision.revision_comment + "\t" + section_string + "\t" + content_added_string
                    + "\t" + content_removed_string + "\t" + sectionToNewsReferencesAdded.get(section)
                    + "\t" + sectionToNewsReferencesRemoved.get(section) + "\t" + sectionToOtherReferencesAdded.get(section)
                    + "\t" + sectionToOtherReferencesRemoved.get(section);
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
                                          Map<String, Set<String>> sectionToNewsReferenceDifferences,
                                          Map<String, Set<String>> sectionToOtherReferenceDifferences) {
        for (String section : sectionsToSections.keySet()) {
            //compare with mapped sections, for now only with top section

            //only one section to compare
            if (sectionsToSections.get(section).size() == 1) {
                compareSections(section, sectionsToSections.get(section).iterator().next(), revision, comparedRevision, sectionToContentDifferences, sectionToNewsReferenceDifferences, sectionToOtherReferenceDifferences);
            }

            //no section to compare
            if (sectionsToSections.get(section).size() == 0) {
                //new section
                compareSections(section, "", revision, comparedRevision, sectionToContentDifferences, sectionToNewsReferenceDifferences, sectionToOtherReferenceDifferences);
            }

            //multiple sections to compare
            if (sectionsToSections.get(section).size() > 1) {

//                System.out.println("Section: " + section + " mapped to: " + sectionsToSections.get(section) + " " + revision_string);

                Map<String, List<Map.Entry<String, String>>> contentDifferences_similarSections = new HashMap<>();
                Map<String, Set<String>> newsRefDifferences_similarSections = new HashMap<>();
                Map<String, Set<String>> otherRefDifferences_similarSections = new HashMap<>();

                //intersection of all comparison results
                for (String similarSection : sectionsToSections.get(section)) {
                    Map<String, List<Map.Entry<String, String>>> contentDifferences = new HashMap<>();
                    Map<String, Set<String>> newsRefDifferences = new HashMap<>();
                    Map<String, Set<String>> otherRefDifferences = new HashMap<>();
                    compareSections(section, similarSection, revision, comparedRevision, contentDifferences, newsRefDifferences, otherRefDifferences);

                    contentDifferences_similarSections.put(similarSection, contentDifferences.get(section));
                    newsRefDifferences_similarSections.put(similarSection, newsRefDifferences.get(section));
                    otherRefDifferences_similarSections.put(similarSection, otherRefDifferences.get(section));
                }

                List<Map.Entry<String, String>> contentDifferences_intersection = computeIntersection(contentDifferences_similarSections);
                Set<String> newsRefDifferences_intersection = computeIntersectionSet(newsRefDifferences_similarSections);
                Set<String> otherRefDifferences_intersection = computeIntersectionSet(otherRefDifferences_similarSections);

//                System.out.println("Section: " + section + " similarSections: " + sectionsToSections.get(section)
//                        + " contentDifferences: " + contentDifferences_intersection + " "
//                        + " newsRefDifferences: " + newsRefDifferences_intersection + " "
//                        + " otherRefDifferences: " + otherRefDifferences_intersection + " "
//                        + revision_string);

                sectionToContentDifferences.put(section, contentDifferences_intersection);
                sectionToNewsReferenceDifferences.put(section, newsRefDifferences_intersection);
                sectionToOtherReferenceDifferences.put(section, otherRefDifferences_intersection);
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
    public void compareSections(String section, String comparedSection, WikipediaEntityRevision revision, WikipediaEntityRevision comparedRevision,
                                Map<String, List<Map.Entry<String, String>>> sectionToContentDifferences,
                                Map<String, Set<String>> sectionToNewsReferenceDifferences,
                                Map<String, Set<String>> sectionToOtherReferenceDifferences) {
        List<Map.Entry<String, String>> contentDifferences = new ArrayList<>();
        Set<String> newsReferenceDifferences = new HashSet<>();
        Set<String> otherReferenceDifferences = new HashSet<>();

        //extract references and sentences for both sections
        WikiSection wiki_section = revision.entity.getSection(section);
        WikiSection wiki_section_compare = comparedRevision.entity.getSection(comparedSection);

        List<String> section_sentences = revision.entity.getSection(section).sentences;
        List<String> compared_section_sentences = comparedRevision.entity.getSection(section).sentences;

        Map<String, Set<String>> urls = getURLs(wiki_section.getSectionCitations());
        Map<String, Set<String>> compared_urls = getURLs(wiki_section_compare.getSectionCitations());

        //content differences
        for (String sentence : section_sentences) {
            if (!compared_section_sentences.contains(sentence)) {
                //check if there is a similar sentence
//                Set<String> similarSentences = findSimilarSentences(sentence, compared_section_sentences);
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
        //news urls
        if (urls.containsKey("news")) {
            for (String url : urls.get("news")) {
                if (!compared_urls.containsKey("news") || !compared_urls.get("news").contains(url)) {
                    //reference added
                    newsReferenceDifferences.add(url);
                }
            }
            sectionToNewsReferenceDifferences.put(section, newsReferenceDifferences);
        }
        //other urls
        if (urls.containsKey("other")) {
            for (String url : urls.get("other")) {
                if (!compared_urls.containsKey("other") || !compared_urls.get("other").contains(url)) {
                    //reference added
                    otherReferenceDifferences.add(url);
                }
            }
            sectionToOtherReferenceDifferences.put(section, otherReferenceDifferences);
        }
    }


    public String findSimilarSentences(String sentence, List<String> sentenceSet) {
//        Set<String> similarSentences = new HashSet<>();

        String mostSimilarSentence = "";
        double mostSimilarSentenceSimilarity = 0.0;

        for (String candidate_sentence : sentenceSet) {
            double jacdis = SimilarityMeasures.computeJaccardDistance(sentence, candidate_sentence);

            if (jacdis > JACCARD_THRESHOLD_SENTENCE && jacdis > mostSimilarSentenceSimilarity) {
                mostSimilarSentence = candidate_sentence;
                mostSimilarSentenceSimilarity = jacdis;
            }
        }
//
//        if (!mostSimilarSentence.equals("")) similarSentences.add(mostSimilarSentence);

        return mostSimilarSentence;
    }


    public Map<String, Set<String>> getURLs(Map<Integer, Map<String, String>> references) {
        Map<String, Set<String>> urls = new HashMap<>();
        for (int ref : references.keySet()) {
            if (references.get(ref).containsKey("url")) {
                continue;
            }
            String type = references.get(ref).containsKey("type") ? references.get(ref).get("type") : "null";
            String url = references.get(ref).get("url");
//
//            url = url.replace("}}", "");
//            url = url.replace("url=", "");

            if (type.equals("news")) {
                if (!urls.containsKey(type)) {
                    urls.put(type, new HashSet<>());
                }
                urls.get(type).add(url);
            } else {
                if (!urls.containsKey("other")) {
                    urls.put("other", new HashSet<>());
                }
                String clean_url = url;
//                String[] parts = url.split(" ");
//
//                for (String part : parts) {
//                    if (part.contains("http")) {
//                        String[] parts2 = part.split("\\|");
//                        for (String part2 : parts2) {
//                            if (part2.contains("http")) {
//                                clean_url = part2;
//                            }
//                        }
//                    }
//                }
                urls.get("other").add(clean_url);
            }
        }
        return urls;
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
//        text = text.replaceAll("<ref>(.*?)(<\\\\/ref>)", "");
//        text = text.replaceAll("<ref(.*?)(\\\\/>|\\\\/ref>)", "");
        text = text.replace("...", ".");
        text = text.replace("\n\n", ".");

        text = text.replace("�", "");
        text = text.replaceAll("\\{\\{[0-9]+\\}\\}", "");
        text = text.replaceAll("â€", "");
        text = text.replaceAll("\\[|\\]", "");

        for (String sentence : nlp.getDocumentSentences(text)) {
            sentence = sentence.replace("\\n", "");
            if (sentence.length() > 3) {
                sentence = StringEscapeUtils.unescapeJson(sentence);
                sentences.add(sentence);
            }
        }
        return sentences;
    }


    public Set<String> genereateWordBag(String sentence) {

        Set<String> word_bag = new HashSet<>();

        //clean
        sentence = sentence.replace("\n", " ");
        sentence = sentence.replace("\\n", " ");
        sentence = sentence.replaceAll("[\\[\\].:,!?;()\"\'{}|=/<>+*]", " ");
        sentence = sentence.toLowerCase();
        sentence = sentence.replaceAll("â€", "");

        String[] sentence_split = sentence.split(" ");
        for (String word : sentence_split) {
            word = word.replaceAll("\\s+", "");
            word = StringEscapeUtils.unescapeJson(word);
            if (!stopWords.contains(word) && !word.equals("") && word.length() > 1) word_bag.add(word);
        }

        return word_bag;
    }
}

