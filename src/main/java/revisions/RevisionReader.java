package revisions;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.xerces.parsers.DOMParser;
import org.w3c.dom.*;
import javax.xml.parsers.*;
import java.io.*;

import entities.WikiSection;
import entities.WikipediaEntity;
import entities.WikipediaEntityRevision;
import org.xml.sax.InputSource;
import org.xml.sax.SAXParseException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static utils.Utils.extractReferences;

/**
 * Created by hube on 1/20/2017.
 */
public class RevisionReader {

    //format of header: pageid="43326718" ns="0" title="Malaysia Airlines Flight 17"><revisions><rev revid="760816086" parentid="760525417" user="60.49.104.223" anon="" userid="0" timestamp="2017-01-19T06:40:29Z" comment="" contentformat="text/x-wiki" contentmodel="wikitext" xml:space="preserve">{{

    WikipediaEntityRevision lastRevision = new WikipediaEntityRevision();

    boolean firstRevision = true;


    public void readRevisions(String revisionsFile) throws Exception {

        BufferedReader reader = new BufferedReader(new FileReader(revisionsFile));
        String line = "";
        String revision_id = "";
        String user_id = "";
        String user_name = "";
        String revision_comment = "";
        String timestamp = "";
        String entity_title = "";
        String entity_id = "";

        int counter = 0;


        while ((line = reader.readLine()) != null && counter < 5000) {

            WikipediaEntityRevision revision = new WikipediaEntityRevision();

            line = StringEscapeUtils.unescapeJson(line).trim();
            //read xml
            Document doc = loadXMLFromString(line);
            if (doc == null){
                System.out.println("Document was read as null!");
                System.out.println(line);
                continue;
            }
            Element root = doc.getDocumentElement();
            //System.out.println(root.getNodeName());

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
            System.out.println(counter);

            entity.setTitle(entity_title);
            entity.setCleanReferences(true);
            entity.setExtractReferences(true);
            entity.setMainSectionsOnly(true);
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
            WikiSection main_section = entity.getSections();

            for (String section : section_keys) {

                WikiSection sub_section = main_section.findSection(section);

                //summary
                //System.out.println(user_id + "\t" + user_name + "\t" + entity_id + "\t" + entity_title + "\t"
                //        + revision_id + "\t" + revision_comment + "\t" + timestamp + "\t" + section);

                //TODO: content added to section, content removed from section, sources added to section, sources removed from section
            }

            if (firstRevision) {
                firstRevision = false;
            } else {
                //compare current revision with last one
                compareWithOldRevision(revision);
            }

            //current revision becomes last revision
            lastRevision = revision;

        }

    }


    public void compareWithOldRevision(WikipediaEntityRevision revision) {

        WikipediaEntity entity = revision.entity;

        Set<String> section_keys = entity.getSectionKeys(); //all section names of the article
        Set<String> old_section_keys = lastRevision.entity.getSectionKeys();
        WikiSection main_section = entity.getSections();

        //compare sections with their counterpart in the old revision, for now: only exact matches

        //check for new sections
        for (String section : section_keys) {
            //System.out.println(section);
            if (old_section_keys.contains(section)) {
                for (String old_section : old_section_keys) {
                    if (old_section.equals(section)) {
                        compareTwoSections(entity, section, old_section, revision);
                        break;
                    }
                }
            } else {
                //new section
                System.out.println("New section: " + section + " added by user: " + revision.user_id
                        + " in revision: " + revision.revision_id);
            }
        }

        //check for deleted sections
        for (String old_section : old_section_keys) {
            if (section_keys.contains(old_section)) {
                continue;
            } else {
                //deleted section
                System.out.println("Deleted section: " + old_section + " deleted by user: " + revision.user_id
                        + " in revision: " + revision.revision_id);
            }
        }
    }


    public void compareTwoSections(WikipediaEntity entity, String section_title, String old_section_title, WikipediaEntityRevision revision) {
        //retrieve the sections
        String section_text = entity.getSectionText(section_title);

        if (section_text == null){
            System.out.println("Got null for section: " + section_title + " revision_id: " + revision.revision_id);
            return;
        }

        if (section_text.equals("Section does not exist!")){
            System.out.println("Section " + section_title + " does not exist!");
        }

        String old_section_text = lastRevision.entity.getSectionText(old_section_title);

        if (section_text == null){
            System.out.println("section_text is NULL!");
            return;
        }
        if (old_section_text == null){
            System.out.println("old_section_text is NULL!");
            return;
        }

        Set<String> referencesAdded = getReferencesAddedOrRemoved(section_text, old_section_text);
        Set<String> referencesRemoved = getReferencesAddedOrRemoved(old_section_text, section_text);

        if (referencesAdded.size() > 0) System.out.println("References added: " + referencesAdded
            + " by user: " + revision.user_id + " in revision: " + revision.revision_id);
        if (referencesRemoved.size() > 0) System.out.println("References removed: " + referencesRemoved
                + " by user: " + revision.user_id + " in revision: " + revision.revision_id);
    }


    //checks what refs are contained in text1 but not in text2
    public Set<String> getReferencesAddedOrRemoved(String text1, String text2) {
        //System.out.println("text1: " + text1 + " text2: " + text2);
        Set<String> text1_refs = extractReferences(text1);

        Set<String> text2_refs = extractReferences(text2);
        Set<String> different_refs = new HashSet<String>();

        for (String ref : text1_refs) {
            if (!text2_refs.contains(ref)) {
                different_refs.add(ref);
            }
        }

        //testing
        //if (different_refs.size() > 0) System.out.println(different_refs);

        return different_refs;
    }


    public void compareTwoTexts(String a, String b) {
        //return the differences between a and b
    }


    public static Document loadXMLFromString(String xml) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        // InputSource is = new InputSource(new FileReader("C:\\Users\\hube\\Desktop\\mh17.xml"));
        InputSource is = new InputSource(new StringReader(xml));
        //System.out.println(xml);
        DOMParser parser = new DOMParser();
        xml = xml.replaceAll("^(.*?)$", "");
        //System.out.println(xml);
        Document doc = null;
        try {
            parser.parse(new InputSource(new java.io.StringReader((xml))));
            doc = parser.getDocument();
        }catch(SAXParseException e){
            e.printStackTrace();
            //System.out.println(xml);
        }

        return doc;
    }


    public static void main(String[] args) throws Exception {
        RevisionReader rr = new RevisionReader();

        System.out.println("start");
        rr.readRevisions("C:\\Users\\hube\\bias\\datasets\\Malaysia_Airlines_allRevisions_reversed");
        System.out.println("finished");
    }
}

