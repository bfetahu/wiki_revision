package entities;

import gnu.trove.set.hash.TIntHashSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by besnik on 4/5/17.
 */
public class WikiSectionSimple implements Serializable{
    public String section_text;

    public List<String> sentences;
    public TIntHashSet urls;
    public Map<Integer, Map<String, String>> section_citations;


    public WikiSectionSimple() {
        sentences = new ArrayList<>();
        urls = new TIntHashSet();
        section_citations = new HashMap<>();
    }
}
