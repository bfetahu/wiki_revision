package revisions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hube on 1/27/2017.
 */
public class TestClass {

    public static void main(String[] args) throws IOException {
        Map<String, StringBuffer> sections = SectionClusterer.readSectionsFromRawRevisions("/Users/besnik/Desktop/top_1.xml", new HashMap<>());
        System.out.println(sections.keySet());
    }
}
