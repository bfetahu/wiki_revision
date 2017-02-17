package revisions;

import entities.WikipediaEntity;
import utils.FileUtils;
import utils.SimilarityMeasures;

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

        String text = "{{For|the aircraft that disappeared over the Indian Ocean|Malaysia Airlines Flight 370}}\n" +
                "{{pp-semi|expiry=13 April 2015|small=yes}}\n" +
                "{{Use dmy dates|date=July 2014}}\n" +
                "{{Use British (Oxford) English|date=July 2014}}\n" +
                "{{Infobox aircraft occurrence\n" +
                "|survivors        = 0\n" +
                "}}\n" +
                "{{Campaignbox 2014 pro-Russian conflict in Ukraine}}\n" +
                "'''Malaysia Airlines Flight 17''' ('''MH17'''/'''MAS17'''){{efn|MH is the [[IATA airline designator|IATA designator]] and MAS is the [[ICAO airline designator|ICAO designator]]. The flight is also marketed as [[KLM]] Flight 4103 (KL4103) through a [[codeshare agreement]].<ref>{{cite web|title=Statement Malaysia Airlines MH17|url=http://news.klm.com/en/incident-with-malaysia-airlines-flight-mh017-e/|website=[[KLM]]|accessdate=18 July 2014}}</ref> }} was a scheduled international passenger flight from [[Amsterdam Airport Schiphol|Amsterdam]] to [[Kuala Lumpur International Airport|Kuala Lumpur]] that crashed on 17 July 2014, presumed to have been shot down, killing all 283 passengers and 15 crew on board.<ref name=\"dsb1\"/> The [[Boeing 777|Boeing 777-200ER]] airliner lost contact about {{convert|50|km|mi|abbr=on}} from the Ukraine–Russia border and crashed near [[Torez]] in [[Donetsk Oblast]], Ukraine, {{convert|40|km|mi|abbr=on}} from the border,<ref name=telegraph>{{cite news |url=http://www.telegraph.co.uk/news/worldnews/europe/ukraine/10974050/Malaysia-Airlines-plane-crashes-on-Ukraine-Russia-border-live.html |title=Malaysia Airlines plane crashes on Ukraine-Russia border – live |date=17 July 2014 |first=Harriet |last=Alexander |work=[[The Daily Telegraph]] |accessdate=17 July 2014|archivedate=18 July 2014|archiveurl=https://web.archive.org/web/20140718001608/http://www.telegraph.co.uk/news/worldnews/europe/ukraine/10974050/Malaysia-Airlines-plane-crashes-on-Ukraine-Russia-border-live.html}}</ref> over territory controlled by pro-Russian separatists.<ref>{{cite web |url=http://www.nytimes.com/2014/09/10/world/europe/malaysian-airliner-ukraine.html?_r=0 |title=Malaysian Jet Over Ukraine Was Downed by ‘High-Energy Objects,’ Dutch Investigators Say |last=Higgins |first=Andrew |last2=Clark |first2=Nicola |date=9 September 2014 |website=[[The New York Times]] }}</ref> The crash occurred during the [[Battle in Shakhtarsk Raion]], part of the ongoing [[war in Donbass]], in an area controlled by the [[Donbass People's Militia]]. According to American intelligence sources, intelligence assembled in the five days after the crash pointed overwhelmingly to [[2014 pro-Russian insurgency in Ukraine|pro-Russian separatists]] having shot down the plane using a [[Buk missile system|Buk]] [[surface-to-air missile]] fired from the territory which they controlled. The Russian government however blamed the Ukrainian Government.<ref name=\"WaPoJuly22\"/> The [[Dutch Safety Board]] is currently leading an investigation into the incident and issued a preliminary report on 9 September 2014 while a final accident report is expected in mid-2015.<ref>{{cite news|url=http://articles.economictimes.indiatimes.com/2014-09-02/news/53479788_1_air-crash-crash-site-interim-report|title=First MH17 crash report due in next two weeks: Investigators|date=2 September 2014|agency=[[Agence France-Presse]]|newspaper=[[The Economic Times]]|accessdate=2 September 2014|location=The Hague|quote=hopefully by the summer of 2015}}</ref><ref>{{cite news|url=http://www.kyivpost.com/content/ukraine/dutch-foreign-minister-final-report-on-causes-of-mh17-crash-could-be-published-next-summer-365338.html|title=Dutch foreign minister: Final report on causes of MH17 crash could be published next summer|newspaper=Kyiv Post|agency=Interfax-Ukraine|date=20 September 2014|accessdate=23 September 2014|location=Ukraine}}</ref>\n" +
                "\n" +
                "==Passengers and crew==\n" +
                "===Passengers and crew2===\n" +
                "====Background====\n" +
                "{{See also|2014 pro-Russian unrest in Ukraine}}\n" +
                "==Investigation==\n" +
                "<!--{{2013–2014 unrest in Ukraine}} Already covered and more appropriate at bottom of article in a collapsible bar anyway -->\n" +
                "{{see also|Trilateral Contact Group on Ukraine}}\n" +
                "==External links==\n";


        WikipediaEntity entity = new WikipediaEntity();
        entity.setMainSectionsOnly(false);
        entity.setSplitSections(true);
        entity.setContent(text);

        System.out.println(entity.getSectionKeys());
    }
}
