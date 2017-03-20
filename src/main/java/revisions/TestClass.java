package revisions;

import entities.WikipediaEntity;
import utils.FileUtils;
import utils.SimilarityMeasures;
import utils.WikiUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
    }
}
