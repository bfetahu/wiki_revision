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
import utils.SimilarityMeasures;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

/**
 * Created by hube on 3/13/2017.
 */
public class RevisionReader_Local {

    RevisionCompare rc;

    File revision_folder;
    File output_folder;


    public RevisionReader_Local(String revision_folder, String output_folder, String stop_words_file) throws IOException {
        this.revision_folder = new File (revision_folder);
        this.output_folder = new File(output_folder);

        rc = new RevisionCompare(stop_words_file);
    }


    public void readRevisions_local() throws Exception {

        for (File file : revision_folder.listFiles()) {

            Boolean firstRevision = true;
            WikipediaEntityRevision lastRevision = new WikipediaEntityRevision();

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(
                            new FileInputStream(file), "UTF8"));

            String line = "";
            String revision_id = "";
            String user_id = "";
            String revision_comment = "";
            String timestamp = "";
            String entity_title = "";
            String entity_id = "";
            String user_name = "";

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
                entity.setExtractReferences(true);
                entity.setExtractBrokenReferences(true); //?
                entity.setMainSectionsOnly(false);
                entity.setSplitSections(true);
                entity.setContent(element.getTextContent());

                //create revision
                revision.revision_id = Long.parseLong(revision_id);
                revision.user_id = user_id;
                revision.user_name = user_name;

                revision.revision_comment = revision_comment;

                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                Date date = formatter.parse(timestamp);

                revision.timestamp = (date.getTime());
                revision.entity_id = Long.parseLong(entity_id);
                revision.entity = entity;

                //extract the section sentences.
                for (String section_key : revision.entity.getSectionKeys()) {
                    WikiSection section = revision.entity.getSection(section_key);
                    section.sentences = rc.getSentences(section.section_text);
                    section.setSectionCitations(revision.entity.getEntityCitations());
                }

                String output = rc.compareWithOldRevision(revision, lastRevision, firstRevision);
                if (!output.equals("")){
                    System.out.print(output);
                }

                if (firstRevision) firstRevision = false;

                //current revision becomes last revision
                lastRevision = revision;
            }
        }

    }


    public static void main(String[] args) throws Exception {

        RevisionReader_Local rr_l = new RevisionReader_Local(
                "C:\\Users\\hube\\bias\\datasets\\test_datasets",
                "C:\\Users\\hube\\bias\\generated_datasets\\local_test_outputs",
                "C:\\Users\\hube\\bias\\datasets\\stop_words");

//        System.out.println("start");
//        System.out.println("user_id" + "\t" + "user_name" + "\t" + "entity_id" + "\t" + "revision_id" + "\t"
//                + "timestamp" + "\t" + "revision_comment" + "\t" +  "section_label" + "\t" + "text_added"
//                + "\t" + "text_removed" + "\t" + "news_references_added" + "\t" + "news_references_removed"
//                + "\t" + "other_references_added" + "\t" + "other_references_removed" + "\t" + "URL");
//        rr.fw.write("user_id" + "\t" + "user_name" + "\t" + "entity_id" + "\t" + "revision_id" + "\t"
//                + "timestamp" + "\t" + "revision_comment" + "\t" +  "section_label" + "\t" + "text_added"
//                + "\t" + "text_removed" + "\t" + "news_references_added" + "\t" + "news_references_removed"
//                + "\t" + "other_references_added" + "\t" + "other_references_removed" + "\t" + "URL" + "\n");
//        rr.fw.flush();

        rr_l.readRevisions_local();
    }

}
