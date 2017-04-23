package entities;

import gnu.trove.set.hash.TIntHashSet;

import java.io.Serializable;
import java.util.*;

/**
 * Created by besnik on 4/5/17.
 */
public class WikiSectionSimple implements Serializable {
    public String section_text;
    public List<String> sentences;
    public List<String> urls;

    public TIntHashSet section_bow;
    public TIntHashSet[] sentence_bow;

    public WikiSectionSimple() {
        sentences = new ArrayList<>();
        urls = new ArrayList<>();
        section_bow = new TIntHashSet();
    }

    /**
     * Generate the BoW representation for each section.
     */
    public void generateSectionBoW() {
        if(section_text == null){
            return;
        }
        String[] tmp_a = section_text.toLowerCase().split("\\s+");
        for(String s:tmp_a){
            section_bow.add(s.intern().hashCode());
        }
    }

    /**
     * Generate the BoW for each sentence.
     */
    public void generateSentenceBoW() {
        if(sentences == null){
            return;
        }

        sentence_bow = new TIntHashSet[sentences.size()];
        for (int i = 0; i < sentences.size(); i++) {
            sentence_bow[i] = new TIntHashSet();
            String sentence = sentences.get(i);
            String[] tmp_a = sentence.toLowerCase().split("\\s+");
            for(String s:tmp_a){
                sentence_bow[i].add(s.intern().hashCode());
            }
        }
    }
}
