import entities.WikiEntity;
import org.json.JSONObject;
import revisions.RevContentComparison;
import utils.FileUtils;
import utils.WikiUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by besnik on 09.06.17.
 */
public class Test {
    public static void parseRevisionDifferences(String file) throws IOException {
        BufferedReader reader = FileUtils.getFileReader(file);
        String line;
        Map<String,Map<Long, String>> revisions = new TreeMap<>();
        while((line = reader.readLine()) != null) {
            String revision_text = line.substring(line.indexOf("\t")).trim();
            JSONObject entity_json = new JSONObject(revision_text);
            String title = entity_json.getString("title");
            long rev_id = entity_json.getLong("id");

            if (!revisions.containsKey(title)) {
                revisions.put(title, new TreeMap<>());
            }
            revisions.get(title).put(rev_id, revision_text);
        }
        for(String entity:revisions.keySet()) {
            WikiEntity prev = null;
            StringBuffer sb = new StringBuffer();
            RevContentComparison rc = new RevContentComparison();
            for (long rev_id : revisions.get(entity).keySet()) {
                String revision_text = revisions.get(entity).get(rev_id);
                WikiEntity revision = WikiUtils.parseEntity(revision_text, true);
                revision.setExtractStatements(false);
                revision.setExtractReferences(true);
                revision.setMainSectionsOnly(false);
                revision.setSplitSections(true);

                revision.parseContent(true);

                //return the revision difference data ending with a "\n"
                if (prev == null) {
                    sb.append(rc.printInitialRevision(revision));
                } else {
                    rc.compareWithOldRevision(revision, prev).forEach(s -> sb.append(s));
                }
                prev = revision;
            }

            FileUtils.saveText(sb.toString(), "/home/besnik/Desktop/revision_comparison.txt", true);
        }
    }

    public static void testPrintUtils(String in_file) throws IOException {
        BufferedReader reader = FileUtils.getFileReader(in_file);
        String line;
        Map<String, Map<Long, String>> revisions = new TreeMap<>();

        while ((line = reader.readLine()) != null) {
            String revision_text = line.substring(line.indexOf("\t")).trim();
            WikiEntity entity = WikiUtils.parseEntity(revision_text, true);
            entity.setExtractStatements(true);
            entity.setExtractReferences(true);
            entity.setMainSectionsOnly(false);
            entity.setSplitSections(true);

            entity.parseContent(true);
        }
    }

    public static void main(String[] args) throws IOException {
        testPrintUtils("/home/besnik/Desktop/part-m-03919.gz");
    }
}
