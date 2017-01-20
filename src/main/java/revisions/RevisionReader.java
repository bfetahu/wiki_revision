package revisions;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hube on 1/20/2017.
 */
public class RevisionReader {

    //format of header: pageid="43326718" ns="0" title="Malaysia Airlines Flight 17"><revisions><rev revid="760816086" parentid="760525417" user="60.49.104.223" anon="" userid="0" timestamp="2017-01-19T06:40:29Z" comment="" contentformat="text/x-wiki" contentmodel="wikitext" xml:space="preserve">{{

    public void readRevisions(String revisionsFile) throws IOException {

        BufferedReader reader = new BufferedReader(new FileReader(revisionsFile));
        String line;
        String revision_id;
        String user_id;
        String revision_comment;
        String timestamp;
        String entity_title;
        String entity_id;
        String content;

        while ((line = reader.readLine()) != null) {

            //TODO: use a matcher

            //extract revision_id, user_id, revision_comment, timestamp, entity_title, content
            entity_title = getAttribute(line, "title");
            revision_id = getAttribute(line, "revid");
            user_id = getAttribute(line, "userid");
            revision_comment = getAttribute(line, "comment");
            timestamp = getAttribute(line, "timestamp");
            entity_id = getAttribute(line, "pageid");


            //testing
            System.out.println(entity_title);


        }
    }


    public String getAttribute(String line, String name){
        if (line.contains(name + "=\"")){
            String[] parts = line.split(name + "=\"");
            String[] parts2 = parts[1].split("\"");
            return parts2[0];
        }
        return null;
    }


    public static void main(String[] args) throws IOException {
        RevisionReader rr = new RevisionReader();

        rr.readRevisions("C:\\Users\\hube\\bias\\datasets\\Malaysia_Airlines_Flight_17_allRevisions");
    }


}
