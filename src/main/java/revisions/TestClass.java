package revisions;

import entities.WikipediaEntity;
import mapred.RevisionComparison;
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

        String text2 = "* [http://www.iana.org/root-whois/ls.htm IANA .ls whois information]\n" +
                "* [http://www.co.ls/ .ls domain registration website]\n" +
                "\n" +
                "{{CcTLD}}\n" +
                "\n" +
                "{{DEFAULTSORT:LS}}\n" +
                "\n" +
                "[[Category:Country code top-level domains]]\n" +
                "[[Category:Communications in Lesotho]]\n" +
                "\n" +
                "{{Compu-domain-stub}}\n" +
                "\n" +
                "[[af:.ls]]\n" +
                "[[ar:.ls]]\n" +
                "[[ast:.ls]]\n" +
                "[[az:.ls]]\n" +
                "[[be:.ls]]\n" +
                "[[be-x-old:.ls]]\n" +
                "[[bg:.ls]]\n" +
                "[[bs:.ls]]\n" +
                "[[cv:.ls]]\n" +
                "[[cs:.ls]]\n" +
                "[[cy:.ls]]\n" +
                "[[da:.ls]]\n" +
                "[[et:.ls]]\n" +
                "[[el:.ls]]\n" +
                "[[es:.ls]]\n" +
                "[[eo:.ls]]\n" +
                "[[eu:.ls]]\n" +
                "[[fa:.ls]]\n" +
                "[[fr:.ls]]\n" +
                "[[xal:.ls]]\n" +
                "[[ko:.ls]]\n" +
                "[[hy:.ls]]\n" +
                "[[hr:.ls]]\n" +
                "[[bpy:.এলএস]]\n" +
                "[[id:.ls]]\n" +
                "[[is:.ls]]\n" +
                "[[it:.ls]]\n" +
                "[[krc:.ls]]\n" +
                "[[ka:.ls]]\n" +
                "[[lv:.ls]]\n" +
                "[[lb:.ls]]\n" +
                "[[hu:.ls]]\n" +
                "[[mk:.ls]]\n" +
                "[[arz:.ls]]\n" +
                "[[ms:.ls]]\n" +
                "[[nl:.ls]]\n" +
                "[[ja:.ls]]\n" +
                "[[ce:.ls]]\n" +
                "[[no:.ls]]\n" +
                "[[oc:.ls]]\n" +
                "[[uz:.ls]]\n" +
                "[[nds:.ls]]\n" +
                "[[pl:.ls]]\n" +
                "[[pt:.ls]]\n" +
                "[[ro:.ls]]\n" +
                "[[ru:.ls]]\n" +
                "[[sah:.ls]]\n" +
                "[[sq:.ls]]\n" +
                "[[sk:.ls]]\n" +
                "[[sr:.ls]]\n" +
                "[[sh:.ls]]\n" +
                "[[fi:.ls]]\n" +
                "[[sv:Toppdomän#L]]\n" +
                "[[tl:.ls]]\n" +
                "[[tt:.ls]]\n" +
                "[[th:.ls]]\n" +
                "[[tg:.ls]]\n" +
                "[[tr:.ls]]\n" +
                "[[tk:.ls]]\n" +
                "[[uk:.ls]]\n" +
                "[[ur:Ls.]]\n" +
                "[[vi:.ls]]\n" +
                "[[fiu-vro:.ls]]\n" +
                "[[war:.ls]]\n" +
                "[[yo:.ls]]\n" +
                "[[diq:.ls]]\n" +
                "[[zh:.ls]]\n";

//        WikiUtils.extractWikiReferences(text);

//        System.out.println(entity.getSectionKeys());

//        List<String> revision_difference_data = new ArrayList<>();
//        revision_difference_data.add("test1");
//        revision_difference_data.add("test2");
//        revision_difference_data.add("test3");
//
//        Text revision_output = new Text();
//        int last_pos = 0;
//        for (String rev_output : revision_difference_data) {
//            byte[] data = rev_output.getBytes();
//            int len = data.length;
//
//            revision_output.append(data, 0, len); //!ArrayIndexOutOfBounds
////            revision_output.append(rev_output);
////            last_pos = last_pos + len + 1;
//        }
//
//        System.out.println(revision_output);

        System.out.println(RevisionComparison.getSentences(text2, false));

        String test = null;

        if (test == null || test.equals("bla")){
            System.out.println("test123");
        } else{
            System.out.println("else");
        }

    }
}
