package revisions;

import entities.WikipediaEntity;
import org.apache.hadoop.io.Text;
import utils.FileUtils;
import utils.SimilarityMeasures;
import utils.WikiUtils;

import java.io.IOException;
import java.util.*;

/**
 * Created by hube on 1/27/2017.
 */
public class TestClass {


    public static void main(String[] args) throws IOException {
//        Map<String, StringBuffer> sections = SectionClusterer.readSectionsFromRawRevisions("/Users/besnik/Desktop/top_1.xml", new HashMap<>());
//        System.out.println(sections.keySet());

//        Set<String> stopWords = new HashSet<String>();
//
//        stopWords = FileUtils.readIntoSet("C:\\Users\\hube\\bias\\datasets\\stop_words", "\n", false);
//        SimilarityMeasures.stop_words = stopWords;

//        double jacdis = SimilarityMeasures.computeJaccardDistance("foo bar","foo bar");
//        System.out.println(jacdis);

        String text = "<ref>{{Cite web | title = Ð’ Ð¢Ð¾Ñ€ÐµÐ·Ðµ Ñ�Ð±Ð¸Ð»Ð¸ Ñ�Ð°Ð¼Ð¾Ð»ÐµÑ‚ | url = https://www.youtube.com/watch?v=48YlDSVFVMI}}</ref>";


//        WikiUtils.extractWikiReferences(text);

//        System.out.println(entity.getSectionKeys());

        List<String> revision_difference_data = new ArrayList<>();
        revision_difference_data.add("test1");
        revision_difference_data.add("test2");
        revision_difference_data.add("test3");

        Text revision_output = new Text();
        int last_pos = 0;
        for (String rev_output : revision_difference_data) {
            byte[] data = rev_output.getBytes();
            int len = data.length;

            revision_output.append(data, 0, len); //!ArrayIndexOutOfBounds
//            revision_output.append(rev_output);
//            last_pos = last_pos + len + 1;
        }

        System.out.println(revision_output);
    }
}
