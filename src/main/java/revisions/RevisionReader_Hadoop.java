package revisions;

import entities.WikipediaEntity;
import entities.WikipediaEntityRevision;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import utils.FileUtils;
import utils.NLPUtils;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by hube on 3/13/2017.
 */
public class RevisionReader_Hadoop {

    RevisionCompare rc;

    File revision_folder;
    File output_folder;


    public RevisionReader_Hadoop(String revision_folder, String output_folder, String stop_words_file) throws IOException {
        this.revision_folder = new File (revision_folder);
        this.output_folder = new File(output_folder);

        this.rc = new RevisionCompare(stop_words_file);
    }


    public void readRevisions_hadoop() throws Exception {

        for (File file : revision_folder.listFiles()) {

            Document doc = FileUtils.readXMLDocument(file.getAbsolutePath());
            if (doc == null) {
                System.out.println("Document was read as null!");
                System.out.println(file.getAbsolutePath());
                continue;
            }
            Element root = doc.getDocumentElement();

            NodeList nList = root.getElementsByTagName("page");

            for (int i = 0; i < nList.getLength(); i++) {
                Element el = (Element) nList.item(i);
                String ns = el.getElementsByTagName("ns").item(0).getTextContent();
//                    System.out.println(el.getElementsByTagName("ns").item(0).getTextContent() + " "
//                        + el.getElementsByTagName("title").item(0).getTextContent());

                //filter entities
                if (ns.equals("0")){

                    //new entity
                    String entity_title = el.getElementsByTagName("title").item(0).getTextContent();

                    if (entity_title.contains("Implicit-association test")){
                        System.out.println("");
                    }


                    String entity_id = el.getElementsByTagName("id").item(0).getTextContent();
                    NodeList nList_revisions = el.getElementsByTagName("revision");

                    FileWriter fw = new FileWriter("./test");
                    try {
                        fw = new FileWriter(output_folder + "/" + entity_title);
                    } catch (FileNotFoundException e){
                        System.out.println("Could not write results for entity: " + entity_title);
                    }


                    WikipediaEntityRevision lastRevision = new WikipediaEntityRevision();
                    boolean firstRevision = true;

                    for (int j = 0; j < nList_revisions.getLength(); j++){
                        Element el_rev = (Element) nList_revisions.item(j);
                        String revision_id = el_rev.getElementsByTagName("id").item(0).getTextContent();

                        String revision_comment = "";
                        try {
                            revision_comment = el_rev.getElementsByTagName("comment").item(0).getTextContent();
                        } catch (NullPointerException e){

                        }

                        String revision_text = el_rev.getElementsByTagName("text").item(0).getTextContent();
                        //filter redirects & disambiguation pages
//                        if (revision_text.startsWith("#REDIRECT")) break;
//                        if (revision_text.contains("{{disambig}}")) break;

//                        System.out.println(revision_text);

                        String timestamp = el_rev.getElementsByTagName("timestamp").item(0).getTextContent();
                        NodeList nList_editor = el_rev.getElementsByTagName("contributor");

                        String user_id = "";
                        String user_name = "";
                        String user_ip = "";

                        for (int k = 0; k < nList_editor.getLength(); k++) {
                            Element el_contr = (Element) nList_editor.item(k);
                            try {
                                user_id = el_contr.getElementsByTagName("id").item(0).getTextContent();
                            } catch (NullPointerException e){
                                user_id = "";
                            }
                            try {
                                user_name = el_contr.getElementsByTagName("username").item(0).getTextContent();
                            } catch (NullPointerException e){
                                user_name = "";
                            }
                            try {
                                user_ip = el_contr.getElementsByTagName("ip").item(0).getTextContent();
                            } catch (NullPointerException e){
                                user_ip = "";
                            }
                        }

                        WikipediaEntityRevision revision = new WikipediaEntityRevision();
                        WikipediaEntity entity = new WikipediaEntity();

                        entity.setTitle(entity_title);
                        entity.setCleanReferences(true);
                        entity.setExtractReferences(false);
                        entity.setMainSectionsOnly(false);
                        entity.setSplitSections(true);
                        entity.setContent(revision_text);

                        //create revision
                        revision.revision_id = Long.parseLong(revision_id);
                        if (user_id.equals("")){
                            revision.user_id = 0;
                            user_name = user_ip;
                        } else{
                            revision.user_id = Long.parseLong(user_id);
                        }

                        revision.revision_comment = revision_comment;

                        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                        Date date = formatter.parse(timestamp);

                        revision.timestamp = (date.getTime());
                        revision.entity_id = Long.parseLong(entity_id);
                        revision.entity = entity;

                        rc.compareWithOldRevision(revision, lastRevision, firstRevision, fw, user_name);

                        if (firstRevision) firstRevision = false;

                        //current revision becomes last revision
                        lastRevision = revision;
                    }
                }
            }
        }

    }




    public static void main(String[] args) throws Exception {
        RevisionReader_Hadoop rr_h = new RevisionReader_Hadoop("C:\\Users\\hube\\bias\\revisions_on_cluster",
                "C:\\Users\\hube\\bias\\generated_datasets", "C:\\Users\\hube\\bias\\datasets\\stop_words"); //revision_folder, output_folder, stop_words_file

        rr_h.readRevisions_hadoop();
    }
}


























