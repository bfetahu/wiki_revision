package revisions;

import entities.WikipediaEntity;
import entities.WikipediaEntityRevision;
import org.apache.commons.lang3.StringEscapeUtils;
import org.hedera.io.Revision;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import utils.FileUtils;
import utils.NLPUtils;
import utils.SimilarityMeasures;
import utils.WikiUtils;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Created by hube on 1/20/2017.
 */
public class RevisionReader {


    WikipediaEntityRevision lastRevision = new WikipediaEntityRevision();

    boolean firstRevision = true;
    NLPUtils nlp = new NLPUtils(2);
    Set<String> stopWords = new HashSet<String>();
    String user_name = "";
    double JACCARD_THRESHOLD_SECTION = 0.1;
    double JACCARD_THRESHOLD_SENTENCE = 0.3;
    String revision_string;

    Map<String,String> sentences_old_revision = new HashMap<>();
    Map<String,String> urls_old_revision = new HashMap<>();

    FileWriter fw = new FileWriter("C:\\Users\\hube\\bias\\generated_datasets\\tests");

    public RevisionReader() throws IOException {

    }


    public void readRevisions(String revisionsFile) throws Exception {

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(revisionsFile), "UTF8"));

        stopWords = FileUtils.readIntoSet("C:\\Users\\hube\\bias\\datasets\\stop_words", "\n", false);
        SimilarityMeasures.stop_words = stopWords;

        String line = "";
        String revision_id = "";
        String user_id = "";
        String revision_comment = "";
        String timestamp = "";
        String entity_title = "";
        String entity_id = "";

        while ((line = reader.readLine()) != null) {

            WikipediaEntityRevision revision = new WikipediaEntityRevision();

            line = StringEscapeUtils.unescapeJson(line).trim();
            //read xml
            Document doc = FileUtils.readXMLDocumentFromString(line);
            if (doc == null) {
                System.out.println("Document was read as null!");
                System.out.println(line);
                continue;
            }
            Element root = doc.getDocumentElement();

            NodeList nList = root.getElementsByTagName("query");
            Node node = nList.item(0);
            Element element = (Element) node;

            nList = element.getElementsByTagName("pages");
            node = nList.item(0);
            element = (Element) node;

            nList = element.getElementsByTagName("page");
            node = nList.item(0);
            element = (Element) node;
            entity_id = element.getAttribute("pageid");
            entity_title = element.getAttribute("title");

            nList = element.getElementsByTagName("revisions");
            node = nList.item(0);
            element = (Element) node;

            nList = element.getElementsByTagName("rev");
            node = nList.item(0);
            element = (Element) node;
            revision_id = element.getAttribute("revid");
            user_id = element.getAttribute("userid");
            user_name = element.getAttribute("user");
            revision_comment = element.getAttribute("comment");
            timestamp = element.getAttribute("timestamp");

            WikipediaEntity entity = new WikipediaEntity();

            entity.setTitle(entity_title);
            entity.setCleanReferences(true);
            entity.setExtractReferences(false);
            entity.setMainSectionsOnly(false);
            entity.setSplitSections(true);
            entity.setContent(element.getTextContent());

            //create revision
            revision.revision_id = Long.parseLong(revision_id);
            try {
                revision.user_id = Long.parseLong(user_id);
            } catch(NumberFormatException e){
                continue;
            }
            revision.revision_comment = revision_comment;

            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            Date date = formatter.parse(timestamp);

            revision.timestamp = (date.getTime());
            revision.entity_id = Long.parseLong(entity_id);
            revision.entity = entity;

            if (firstRevision){
                WikipediaEntity emptyEntity = new WikipediaEntity();
                entity.setSplitSections(true);

                //set content to empty
                emptyEntity.setContent("{{Infobox aircraft occurrence\n" +
                        "}}\n" +
                        "==References==\n" +
                        "{{reflist}}");
                lastRevision.entity = emptyEntity;
            }

            compareWithOldRevision(revision);

            if (firstRevision) firstRevision = false;

            //current revision becomes last revision
            lastRevision = revision;
        }

    }


    public void compareWithOldRevision(WikipediaEntityRevision revision) throws IOException {
        revision_string = "https://en.wikipedia.org/w/index.php?diff=" + revision.revision_id
                + "&oldid=" + lastRevision.revision_id;

        //map old section to new section, identify added and removed sections, result: section mappings
        Map<String,Set<String>> oldSectionsToNewSections = new HashMap<>();
        Map<String,Set<String>> newSectionsToOldSections = new HashMap<>();

        computeSectionMappings(lastRevision, revision, oldSectionsToNewSections);
        computeSectionMappings(revision, lastRevision, newSectionsToOldSections);

        //for every section: map old sentences to new sentences, identify added and removed sentences
        Map<String, Set<String>> sectionToContentAdded = new HashMap<>();
        Map<String, Set<String>> sectionToContentRemoved = new HashMap<>();
        Map<String, Set<String>> sectionToNewsReferencesAdded = new HashMap<>();
        Map<String, Set<String>> sectionToNewsReferencesRemoved = new HashMap<>();
        Map<String, Set<String>> sectionToOtherReferencesAdded = new HashMap<>();
        Map<String, Set<String>> sectionToOtherReferencesRemoved = new HashMap<>();

        computeContentDifferences(newSectionsToOldSections, revision, lastRevision, sectionToContentAdded, sectionToNewsReferencesAdded, sectionToOtherReferencesAdded);
        computeContentDifferences(oldSectionsToNewSections, lastRevision, revision, sectionToContentRemoved, sectionToNewsReferencesRemoved, sectionToOtherReferencesRemoved);

        //output changes in all sections
        for (String section : newSectionsToOldSections.keySet()){
            outputChanges(section, sectionToContentAdded, sectionToContentRemoved,
                    sectionToNewsReferencesAdded, sectionToNewsReferencesRemoved,
                    sectionToOtherReferencesAdded, sectionToOtherReferencesRemoved,
                    false, revision);
        }

        //check for deleted sections
        for (String section : oldSectionsToNewSections.keySet()){
            if (!newSectionsToOldSections.keySet().contains(section)){
                //deleted section
                outputChanges(section, sectionToContentAdded, sectionToContentRemoved,
                        sectionToNewsReferencesAdded, sectionToNewsReferencesRemoved,
                        sectionToOtherReferencesAdded, sectionToOtherReferencesRemoved,
                        true, revision);
            }
        }
    }


    public void outputChanges(String section, Map<String, Set<String>> sectionToContentAdded, Map<String, Set<String>> sectionToContentRemoved,
                              Map<String, Set<String>> sectionToNewsReferencesAdded, Map<String, Set<String>> sectionToNewsReferencesRemoved,
                              Map<String, Set<String>> sectionToOtherReferencesAdded, Map<String, Set<String>> sectionToOtherReferencesRemoved,
                              Boolean deletedSection, WikipediaEntityRevision revision){
        //add empty set in case that there is no change in this section
        if (!sectionToContentAdded.containsKey(section)) {
            Set<String> set = new HashSet<>();
            sectionToContentAdded.put(section,set);
        }
        if (!sectionToContentRemoved.containsKey(section)) {
            Set<String> set = new HashSet<>();
            sectionToContentRemoved.put(section,set);
        }
        if (!sectionToNewsReferencesAdded.containsKey(section)) {
            Set<String> set = new HashSet<>();
            sectionToNewsReferencesAdded.put(section,set);
        }
        if (!sectionToNewsReferencesRemoved.containsKey(section)) {
            Set<String> set = new HashSet<>();
            sectionToNewsReferencesRemoved.put(section,set);
        }
        if (!sectionToOtherReferencesAdded.containsKey(section)) {
            Set<String> set = new HashSet<>();
            sectionToOtherReferencesAdded.put(section,set);
        }
        if (!sectionToOtherReferencesRemoved.containsKey(section)) {
            Set<String> set = new HashSet<>();
            sectionToOtherReferencesRemoved.put(section,set);
        }

        //create content strings
        String content_added_string = "[";
        for (String string : sectionToContentAdded.get(section)){
            if (content_added_string.equals("[")){
                content_added_string = content_added_string + string;
            } else{
                content_added_string = content_added_string + ";" + string;
            }
        }
        content_added_string = content_added_string + "]";

        String content_removed_string = "[";
        for (String string : sectionToContentRemoved.get(section)){
            if (content_removed_string.equals("[")){
                content_removed_string = content_removed_string + string;
            } else{
                content_removed_string = content_removed_string + ";" + string;
            }
        }
        content_removed_string = content_removed_string + "]";

        //output
        if (sectionToContentAdded.get(section).size() > 0 || sectionToContentRemoved.get(section).size() > 0
                || sectionToNewsReferencesAdded.get(section).size() > 0 || sectionToNewsReferencesAdded.get(section).size() > 0
                || sectionToOtherReferencesAdded.get(section).size() > 0 || sectionToOtherReferencesAdded.get(section).size() > 0) {

            String section_string = "";
            if (deletedSection){
                section_string = section + " (deleted section)";
            } else{
                section_string = section;
            }

            System.out.println(revision.user_id + "\t" + user_name + "\t" + revision.entity_id + "\t" + revision.revision_id + "\t"
                    + revision.timestamp + "\t" + revision.revision_comment + "\t" + section_string + "\t" + content_added_string
                    + "\t" + content_removed_string + "\t" + sectionToNewsReferencesAdded.get(section)
                    + "\t" + sectionToNewsReferencesRemoved.get(section) + "\t" + sectionToOtherReferencesAdded.get(section)
                    + "\t" + sectionToOtherReferencesRemoved.get(section) + "\t" + revision_string);
//                fw.write(revision.user_id + "\t" + user_name + "\t" + revision.entity_id + "\t" + revision.revision_id + "\t"
//                        + revision.timestamp + "\t" + revision.revision_comment + "\t" +  section + "\t" + content_added_string
//                        + "\t" + content_removed_string + "\t" + sectionToNewsReferencesAdded.get(section)
//                        + "\t" + sectionToNewsReferencesRemoved.get(section) + "\t" + sectionToOtherReferencesAdded.get(section)
//                        + "\t" + sectionToOtherReferencesRemoved.get(section) + "\t" +  revision_string + "\n");
//                fw.flush();
        }
    }


    public void computeSectionMappings(WikipediaEntityRevision revision, WikipediaEntityRevision compared_revision, Map<String,Set<String>> sectionMapping){
        //get sections for both revisions
        Set<String> revision_sections = revision.entity.getSectionKeys();
        Set<String> compared_revision_sections = compared_revision.entity.getSectionKeys();

        //check for differences between revisions
        for (String section : revision_sections){
            if (compared_revision_sections.contains(section)){
                //section name is the same
                Set<String> set = new HashSet<>();
                set.add(section);
                sectionMapping.put(section, set);
            } else{

                Set<String> set = new HashSet<>();
                sectionMapping.put(section, set);

                //check if section name has changed
                for (String compared_section : compared_revision_sections){

                    //rename?
                    if (revision_sections.contains(compared_section)){
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


    public void computeContentDifferences(Map<String,Set<String>> sectionsToSections, WikipediaEntityRevision revision, WikipediaEntityRevision comparedRevision,
                                          Map<String, Set<String>> sectionToContentDifferences,  Map<String, Set<String>> sectionToNewsReferenceDifferences, Map<String, Set<String>> sectionToOtherReferenceDifferences){
        for (String section : sectionsToSections.keySet()) {
            //compare with mapped sections, for now only with top section

            //only one section to compare
            if (sectionsToSections.get(section).size() == 1){
                compareSections(section, sectionsToSections.get(section).iterator().next(), revision, comparedRevision, sectionToContentDifferences, sectionToNewsReferenceDifferences, sectionToOtherReferenceDifferences);
            }

            //no section to compare
            if (sectionsToSections.get(section).size() == 0){
                //new section
                compareSections(section, "", revision, comparedRevision, sectionToContentDifferences, sectionToNewsReferenceDifferences, sectionToOtherReferenceDifferences);
            }

            //multiple sections to compare
            if (sectionsToSections.get(section).size() > 1){

                System.out.println("Section: " + section + " mapped to: " + sectionsToSections.get(section) + " " + revision_string);

                Map<String, Set<String>> contentDifferences_similarSections = new HashMap<>();
                Map<String, Set<String>> newsRefDifferences_similarSections = new HashMap<>();
                Map<String, Set<String>> otherRefDifferences_similarSections = new HashMap<>();

                //intersection of all comparison results
                for (String similarSection : sectionsToSections.get(section)){
                    Map<String, Set<String>> contentDifferences = new HashMap<>();
                    Map<String, Set<String>> newsRefDifferences = new HashMap<>();
                    Map<String, Set<String>> otherRefDifferences = new HashMap<>();
                    compareSections(section, similarSection, revision, comparedRevision, contentDifferences, newsRefDifferences, otherRefDifferences);

                    contentDifferences_similarSections.put(similarSection, contentDifferences.get(section));
                    newsRefDifferences_similarSections.put(similarSection, newsRefDifferences.get(section));
                    otherRefDifferences_similarSections.put(similarSection, otherRefDifferences.get(section));
                }

                Set<String> contentDifferences_intersection = computeIntersection(contentDifferences_similarSections);
                Set<String> newsRefDifferences_intersection = computeIntersection(newsRefDifferences_similarSections);
                Set<String> otherRefDifferences_intersection = computeIntersection(otherRefDifferences_similarSections);

                System.out.println("Section: " + section + " similarSections: " + sectionsToSections.get(section)
                        + " contentDifferences: " + contentDifferences_intersection + " "
                        + " newsRefDifferences: " + newsRefDifferences_intersection + " "
                        + " otherRefDifferences: " + otherRefDifferences_intersection + " "
                        + revision_string);

                sectionToContentDifferences.put(section, contentDifferences_intersection);
                sectionToNewsReferenceDifferences.put(section, newsRefDifferences_intersection);
                sectionToOtherReferenceDifferences.put(section, otherRefDifferences_intersection);
            }
        }
    }


    public Set<String> computeIntersection(Map<String, Set<String>> differences_similarSections){
        Set<String> differences_intersection = new HashSet<>();
        String first_section = differences_similarSections.keySet().iterator().next();
        for (String difference : differences_similarSections.get(first_section)){ //pick one random section, check if all other sections contain the same difference
            Boolean difference_in_all_sections = true;
            for (String involved_section : differences_similarSections.keySet()){
                if (!differences_similarSections.get(involved_section).contains(difference)) {
                    difference_in_all_sections = false;
                    break;
                }
            }
            if (difference_in_all_sections){
                differences_intersection.add(difference);
            }
        }
        return differences_intersection;
    }


    //computes differences between two sections
    public void compareSections(String section, String comparedSection, WikipediaEntityRevision revision, WikipediaEntityRevision comparedRevision,
                                Map<String, Set<String>> sectionToContentDifferences,  Map<String, Set<String>> sectionToNewsReferenceDifferences, Map<String, Set<String>> sectionToOtherReferenceDifferences){
        Set<String> contentDifferences = new HashSet<>();
        Set<String> newsReferenceDifferences = new HashSet<>();
        Set<String> otherReferenceDifferences = new HashSet<>();

        //extract references and sentences for both sections
        String section_text = revision.entity.getSectionText(section);

        if (section_text == null){
            System.out.println("section_text is null");
            return;
        }

        String compared_section_text = "";
        if (!comparedSection.equals("")){
            compared_section_text = comparedRevision.entity.getSectionText(comparedSection);
        }

//        if (revision.revision_id==617330000){
//            System.out.println("");
//        }

        Set<String> section_sentences = getSentences(section_text);
        Set<String> compared_section_sentences = getSentences(compared_section_text);
        Set<String> section_other_urls = new HashSet<>();
        Set<String> compared_section_other_urls = new HashSet<>();
        Set<String> section_news_urls = getURLs(section_text, section_other_urls);
        Set<String> compared_section_news_urls = getURLs(compared_section_text, compared_section_other_urls);

        //content differences
        for (String sentence : section_sentences) {
            if (!compared_section_sentences.contains(sentence)) {
                //check if there is a similar sentence
                Set<String> similarSentences = findSimilarSentences(sentence,compared_section_sentences);

                if (similarSentences.size() == 0){
                    //sentence has been added
                    contentDifferences.add(sentence);
                } else if (similarSentences.size() == 1){
                    //part of sentence is new, compare bag of words
                    Set<String> words_sentence = genereateWordBag(sentence);
                    Set<String> words_similar_sentence = genereateWordBag(similarSentences.iterator().next());

                    for (String word : words_sentence) {
                        if (!words_similar_sentence.contains(word)) {
                            contentDifferences.add(word);
                        }
                    }
                } else{
                    //solution so far: we take the most similar sentence
                }
            }
        }
        sectionToContentDifferences.put(section, contentDifferences);

        //reference differences
        //news urls
        for (String url : section_news_urls){
            if (!compared_section_news_urls.contains(url) && !compared_section_other_urls.contains(url)){
                //reference added
                newsReferenceDifferences.add(url);
            }
        }

        sectionToNewsReferenceDifferences.put(section, newsReferenceDifferences);
        //other urls
        for (String url : section_other_urls){
            if (!compared_section_news_urls.contains(url) && !compared_section_other_urls.contains(url)){
                //reference added
                otherReferenceDifferences.add(url);
            }
        }
        sectionToOtherReferenceDifferences.put(section, otherReferenceDifferences);
    }


    public Set<String> findSimilarSentences(String sentence, Set<String> sentenceSet){
        Set<String> similarSentences = new HashSet<>();

        String mostSimilarSentence = "";
        double mostSimilarSentenceSimilarity = 0.0;

        for (String candidate_sentence : sentenceSet){
            double jacdis = SimilarityMeasures.computeJaccardDistance(sentence, candidate_sentence);

            if (jacdis > JACCARD_THRESHOLD_SENTENCE && jacdis > mostSimilarSentenceSimilarity) {
                mostSimilarSentence = candidate_sentence;
                mostSimilarSentenceSimilarity = jacdis;
            }
        }

        if (!mostSimilarSentence.equals("")) similarSentences.add(mostSimilarSentence);

        return similarSentences;
    }


    public Set<String> getURLs(String text, Set<String> otherURLs){
        Set<String> news_urls = new HashSet<>();
        Map<Integer, Map<String, String>> references = new HashMap<>();

        text = StringEscapeUtils.unescapeJson(text);

        text = text.replace("\\n","");
        text = text.replace("\n","");
        text = text.toLowerCase();

        WikiUtils.extractWikiReferences(text, references);
        for (int ref : references.keySet()) {
            String type = "";
            String url = "";

            if (references.get(ref).get("url") == null) {
//                System.out.println("url is null: " + ref);
                continue;
            } else {
                url = references.get(ref).get("url");
            }
            if (references.get(ref).get("type") == null) {
                type = "null";
            } else {
                type = references.get(ref).get("type");
            }

            url = url.replace("}}", "");
            url = url.replace("url=", "");

            if (type.equals("news")) {
                news_urls.add(url);
            } else {
                String clean_url = url;
                String[] parts = url.split(" ");
                if (parts.length > 1) {
//                    System.out.println("");
                }
                for (String part : parts) {
                    if (part.contains("http")) {
                        String[] parts2 = part.split("\\|");
                        for (String part2 : parts2) {
                            if (part2.contains("http")) {
                                clean_url = part2;
//                                System.out.println("url: " + url);
                            }
                        }
                    }
                }
                otherURLs.add(clean_url);
            }
        }
        return news_urls;
    }


    public Set<String> getSentences(String text){
        Set<String> sentences = new HashSet<>();

        text = StringEscapeUtils.escapeJson(text);

        //cleaning
        text = text.replaceAll("<ref>(.*?)(<\\\\/ref>)", "");
        text = text.replaceAll("<ref(.*?)(\\\\/>|\\\\/ref>)", "");
        text = text.replace("...",".");
        text = text.replace("\\n\\n",".");
        text = text.replace("�", "");
        text = text.replaceAll("\\{\\{[0-9]+}}", "");
        text = text.replaceAll("â€","");
        text = text.replaceAll("[\\[\\]]","");

        for (String sentence : nlp.getDocumentSentences(text)){
            sentence = sentence.replace("\\n","");
            sentence = sentence.replace("\n","");
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
        sentence = sentence.replaceAll("â€","");

        String[] sentence_split = sentence.split(" ");
        for (String word : sentence_split) {
            word = word.replaceAll("\\s+", "");
            word = StringEscapeUtils.unescapeJson(word);
            if (!stopWords.contains(word) && !word.equals("") && word.length() > 1) word_bag.add(word);
        }

        return word_bag;
    }


    public static void main(String[] args) throws Exception {
        RevisionReader rr = new RevisionReader();

        System.out.println("start");
        System.out.println("user_id" + "\t" + "user_name" + "\t" + "entity_id" + "\t" + "revision_id" + "\t"
                + "timestamp" + "\t" + "revision_comment" + "\t" +  "section_label" + "\t" + "text_added"
                + "\t" + "text_removed" + "\t" + "news_references_added" + "\t" + "news_references_removed"
                + "\t" + "other_references_added" + "\t" + "other_references_removed" + "\t" + "URL");
        rr.fw.write("user_id" + "\t" + "user_name" + "\t" + "entity_id" + "\t" + "revision_id" + "\t"
                + "timestamp" + "\t" + "revision_comment" + "\t" +  "section_label" + "\t" + "text_added"
                + "\t" + "text_removed" + "\t" + "news_references_added" + "\t" + "news_references_removed"
                + "\t" + "other_references_added" + "\t" + "other_references_removed" + "\t" + "URL" + "\n");
        rr.fw.flush();
        rr.readRevisions("C:\\Users\\hube\\bias\\datasets\\revisions_gamergate_part1_reverted");
        System.out.println("finished");
    }
}

