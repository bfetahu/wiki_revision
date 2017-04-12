package entities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by besnik on 4/5/17.
 */
public class WikiSectionSimple implements Serializable{
    public String section_text;
    public List<String> sentences;
    public List<String> urls;

    public WikiSectionSimple() {
        sentences = new ArrayList<>();
        urls = new ArrayList<>();
    }
}
