package utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hube on 7/4/2017.
 */
public class SentenceComparison {


    /**
     * extracts all words contained in sentence1 but not in sentence2
     *
     * @param sentence1
     * @param sentence2
     * @return
     */
    public static List<String> extractChanges(String sentence1, String sentence2){
        List<String> changes = new ArrayList<>();
        String[] tokens1 = createWordList(sentence1);
        String[] tokens2 = createWordList(sentence2);

        for (String word : tokens1){
            boolean hasWord = false;
            for (String word_compare : tokens2){
                if (word.equals(word_compare)){
                    hasWord = true;
                    break;
                }
            }
            if (!hasWord){
                changes.add(word);
            }
        }
        return changes;
    }


    /**
     * creates the word list of a sentence
     *
     * @param sentence
     * @return
     */
    public static String[] createWordList(String sentence){
        String[] words = sentence.toLowerCase().replaceAll("[^a-z0-9 ]", "").split("\\s+");
        return words;
    }


    public static void main(String[] args){
        String prev_sentence = "This is the old test.";
        String current_sentence = "This is the new test.";
        List<String> text_added = new ArrayList<>();
        List<String> text_removed = new ArrayList<>();

        //compare on word level
        for (String word : extractChanges(current_sentence, prev_sentence)){
            text_added.add(word);
        }
        for (String word : extractChanges(prev_sentence, current_sentence)){
            text_removed.add(word);
        }

        System.out.println("text added: " + text_added);
        System.out.println("text removed: " + text_removed);
    }

}
