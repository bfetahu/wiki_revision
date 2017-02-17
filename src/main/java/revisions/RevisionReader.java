package revisions;

import entities.WikiSection;
import entities.WikipediaEntity;
import entities.WikipediaEntityRevision;
import org.apache.commons.lang3.StringEscapeUtils;
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

    FileWriter fw = new FileWriter("C:\\Users\\hube\\bias\\generated_datasets\\malaysia_airlins_17_sections");

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

        int counter = 0;


        while ((line = reader.readLine()) != null && counter < 5000) {

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

            counter++;

            entity.setTitle(entity_title);
            entity.setCleanReferences(true);
            entity.setExtractReferences(true);
            entity.setMainSectionsOnly(false);
            entity.setSplitSections(true);
            entity.setContent(element.getTextContent());

            //create revision
            revision.revision_id = Long.parseLong(revision_id);
            revision.user_id = Long.parseLong(user_id);
            revision.revision_comment = revision_comment;

            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            Date date = formatter.parse(timestamp);

            revision.timestamp = (date.getTime());
            revision.entity_id = Long.parseLong(entity_id);
            revision.entity = entity;


            //sections
            Set<String> section_keys = entity.getSectionKeys(); //all section names of the article

            for (String section : section_keys) {
                WikiSection sub_section = entity.getSection(section);

                //summary
//                System.out.println(user_id + "\t" + user_name + "\t" + entity_id + "\t" + entity_title + "\t"
//                        + revision_id + "\t" + revision_comment + "\t" + timestamp + "\t" + section);
            }

            compareWithOldRevision(revision);

            if (firstRevision) firstRevision = false;

            //current revision becomes last revision
            lastRevision = revision;
        }

    }


    public void compareWithOldRevision(WikipediaEntityRevision revision) throws IOException {

        WikipediaEntity entity = revision.entity;

        Set<String> section_keys = entity.getSectionKeys(); //all section names of the article, also subsections
        Set<String> old_section_keys = lastRevision.entity.getSectionKeys();

        Set<String> removed_section_keys = new HashSet<>();
        Set<String> added_section_keys = new HashSet<>();

        //compare sections with their counterpart in the old revision, for now: only exact matches

        //check for deleted sections
        for (String old_section : old_section_keys) {
            if (section_keys.contains(old_section)) {
                continue;
            } else {
                //section has been deleted or renamed
                removed_section_keys.add(old_section);
            }
        }

        //check for new,renamed,merged,split sections
        for (String section : section_keys) {

            if (section.equals("")){
//                System.out.println("empty label");
            }

            if (old_section_keys.contains(section)) {

                for (String old_section : old_section_keys) {
                    if (old_section.equals(section)) {
                        compareTwoSections(entity, entity.getSectionText(section), lastRevision.entity.getSectionText(old_section), revision);
                        break;
                    }
                }
            } else {
                //check if section name has been changed
                String mostSimilarSection = "";
                double mostSimilarSectionDistance = 0.0;

                //if multiple similar sections in old version exit --> possible section merge
                Set<String> similarSections = new HashSet<>();

                //testing
                added_section_keys.add(section);

                if (!firstRevision) {
                    for (String old_section : removed_section_keys) {

                        //pairs for ground truth check
//                        Date old_revision_date = new Date(lastRevision.timestamp);
//                        Date new_revision_date = new Date(revision.timestamp);
//                        System.out.println(old_revision_date + " " + new_revision_date + " " + user_name + " old_section_name:"
//                                + old_section + " new_section_name:" + section);

                        //compare section texts
                        double jacdis = SimilarityMeasures.computeJaccardDistance(revision.entity.getSectionText(section), lastRevision.entity.getSectionText(old_section));
                        if (jacdis > 0.5 && jacdis > mostSimilarSectionDistance) {
                            mostSimilarSection = old_section;
                            mostSimilarSectionDistance = jacdis;
                            similarSections.add(old_section);
                        }
                    }
                }

                if (mostSimilarSection.equals("")) {
                    compareTwoSections(entity, entity.getSectionText(section), "", revision); //compare with blank

//                    System.out.println("rename: no");
                } else {
                    //check for possible section merge
                    if (similarSections.size() > 1){

                        System.out.println("section merge! old section:" + similarSections + " new section: "
                                + section + " time: " + revision.timestamp + " user: " + user_name);

                        //merge old section texts and compare with new section
                        String mergedSections = "";
                        for (String similarSection : similarSections){
                            mergedSections = mergedSections + lastRevision.entity.getSectionText(similarSection);
                            compareTwoSections(entity, entity.getSectionText(section), mergedSections, revision);
                        }
                    } else{
                        compareTwoSections(entity, entity.getSectionText(section), lastRevision.entity.getSectionText(mostSimilarSection), revision);

//                        System.out.println("rename: yes, " + mostSimilarSection);
                        //new section
                    }
                }
            }
        }

    }


    public void compareTwoSections(WikipediaEntity entity, String section_text, String old_section_text, WikipediaEntityRevision revision) throws IOException {

        if (section_text == null) {
            System.out.println("section_text is NULL!");
            return;
        }
        if (old_section_text == null) {
            System.out.println("old_section_text is NULL!");
            return;
        }

        //citations
        Set<String> referencesAdded = getReferencesAddedOrRemoved(section_text, old_section_text);
        Set<String> referencesRemoved = getReferencesAddedOrRemoved(old_section_text, section_text);
        //if (referencesAdded.size() > 0) System.out.println("References added: " + referencesAdded
        //    + " by user: " + revision.user_id + " in revision: " + revision.revision_id);
        //if (referencesRemoved.size() > 0) System.out.println("References removed: " + referencesRemoved
        //        + " by user: " + revision.user_id + " in revision: " + revision.revision_id);

        //text
        Set<String> textAdded = getTextDifferences(section_text, old_section_text);
        Set<String> textRemoved = getTextDifferences(old_section_text, section_text);

//        System.out.println(revision.user_id + "\t" + user_name + "\t" + revision.entity_id + "\t" + section_title + "\t" + revision.revision_id
//                + "\t" + revision.timestamp + "\t" + revision.revision_comment + "\t" + referencesAdded + "\t" + referencesRemoved
//                + "\t" + textAdded + "\t" + textRemoved);
//        fw.write(revision.user_id + "\t" + user_name + "\t" + revision.entity_id + "\t" + section_title + "\t" + revision.revision_id
//                + "\t" + revision.timestamp + "\t" + revision.revision_comment + "\t" + referencesAdded + "\t" + referencesRemoved
//                + "\t" + textAdded + "\t" + textRemoved + "\n");
//        fw.write(section_title + "\t" + revision.revision_id + "\t" + StringEscapeUtils.escapeJson(section_text) + "\n");
//        fw.flush();
    }


    //compare two section texts
    public Set<String> getTextDifferences(String text1, String text2) {
        Set<String> different_text = new HashSet<>();

        //check what text text1 contains that is not contained in text2

        //escape instead?
        text1 = text1.replace("�", "");
        text2 = text2.replace("�", "");
        text1 = text1.replaceAll("\\{\\{[0-9]+}}", "");
        text2 = text2.replaceAll("\\{\\{[0-9]+}}", "");

        List<String> sentences_text1 = nlp.getDocumentSentences(text1);
        List<String> sentences_text2 = nlp.getDocumentSentences(text2);

        for (String sentence_text1 : sentences_text1) {

            Set<String> sentence_words_text1 = genereateWordBag(sentence_text1);

            if (!sentences_text2.contains(sentence_text1)) { //does not work for all cases
                //sentence has been added, changed or moved from another section

                //check if there is a similar sentence in the old revision. If there are multiple ones, pick the most similar one.
                String mostSimilarSentence = "";
                double mostSimilarSentenceDis = 0.0;

                for (String sentence_text2 : sentences_text2) {
                    double jacdis = SimilarityMeasures.computeJaccardDistance(sentence_text1, sentence_text2);

                    if (jacdis > 0.5 && jacdis > mostSimilarSentenceDis) {
                        mostSimilarSentence = sentence_text2;
                        mostSimilarSentenceDis = jacdis;
                    }
                }
                if (!mostSimilarSentence.equals("")) {
                    //System.out.println("old: " + mostSimilarSentence + "\n" + "new: " + sentence_text1);

                    Set<String> mostSimilarSentence_words = genereateWordBag(mostSimilarSentence);

                    for (String word : sentence_words_text1) {
                        if (!mostSimilarSentence_words.contains(word)) {
                            different_text.add(word);
                        }
                    }
                } else {
                    //no similar sentence found --> new (removed) sentence
                    for (String word : sentence_words_text1) {
                        different_text.add(word);
                    }
                }
            } else {
                //nothing has been added or removed
                //System.out.println("No changes");
            }
        }

        return different_text;
    }


    public Set<String> genereateWordBag(String sentence) {

        Set<String> word_bag = new HashSet<>();

        //clean
        sentence = sentence.replace("\n", " ");
        sentence = sentence.replaceAll("[\\[\\].:,!?;()\"\'{}|=/<>+*]", " ");
        sentence = sentence.toLowerCase();

        String[] sentence_split = sentence.split(" ");
        for (String word : sentence_split) {
            word = word.replaceAll("\\s+", "");
            if (!stopWords.contains(word) && !word.equals("")) word_bag.add(word);
        }

        return word_bag;
    }


    //checks what refs are contained in text1 but not in text2
    public Set<String> getReferencesAddedOrRemoved(String text1, String text2) {

        Map<Integer, Map<String, String>> ref_1 = new HashMap<>();
        Map<Integer, Map<String, String>> ref_2 = new HashMap<>();
        WikiUtils.extractWikiReferences(text1, ref_1);
        WikiUtils.extractWikiReferences(text2, ref_2);

        Set<String> different_refs = new HashSet<>();

        Set<String> ref_1_urls = new HashSet<>();
        Set<String> ref_2_urls = new HashSet<>();
        for (int ref : ref_1.keySet()) {
            String url = ref_1.get(ref).get("url");
            ref_1_urls.add(url);
        }

        for (int ref : ref_2.keySet()) {
            String url = ref_2.get(ref).get("url");
            ref_2_urls.add(url);
        }

        different_refs.addAll(ref_1_urls);
        different_refs.removeAll(ref_2_urls);
        return different_refs;
    }


    public static void main(String[] args) throws Exception {
        RevisionReader rr = new RevisionReader();

        System.out.println("start");
        rr.readRevisions("C:\\Users\\hube\\bias\\datasets\\Malaysia_Airlines_allRevisions_reversed");
        System.out.println("finished");
    }
}

