

import entities.WikiEntity;
import revisions.RevContentComparison;
import utils.FileUtils;

import java.io.IOException;

/**
 * Created by besnik on 6/23/17.
 */
public class Test {
    public static void main(String[] args) throws IOException {
        String obama_1 = FileUtils.readText("/Users/besnik/Desktop/obama.xml");
        String obama_2 = FileUtils.readText("/Users/besnik/Desktop/obama2.xml");

        WikiEntity e1 = new WikiEntity();
        e1.title = "Barack Obama";
        e1.setRevisionID(1l);
        e1.content = obama_1;
        e1.setExtractStatements(false);
        e1.setExtractReferences(true);
        e1.setMainSectionsOnly(false);
        e1.setSplitSections(true);
        e1.parseContent(true);


        WikiEntity e2 = new WikiEntity();
        e2.title = "Barack Obama";
        e2.setRevisionID(2l);
        e2.content = obama_2;
        e2.setExtractStatements(false);
        e2.setExtractReferences(true);
        e2.setMainSectionsOnly(false);
        e2.setSplitSections(true);
        e2.parseContent(true);


        RevContentComparison rc = new RevContentComparison();
        StringBuffer sb = new StringBuffer();
        sb.append(rc.compareWithOldRevision(e2, e1)).append("\n");
        sb.append(rc.printInitialRevision(e1));
        FileUtils.saveText(sb.toString(), "/Users/besnik/Desktop/obama_out.json");
    }
}
