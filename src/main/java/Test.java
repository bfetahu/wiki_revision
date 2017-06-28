

import utils.FileUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by besnik on 6/23/17.
 */
public class Test {
    public static void main(String[] args) throws IOException {
        BufferedReader reader = FileUtils.getFileReader("/Users/besnik/Desktop/revision_stats.csv.bz2");

        Map<Integer, List<Integer>> sections = new HashMap<>();
        Map<Integer, List<Integer>> citations = new HashMap<>();


        Map<Integer, Set<String>> entities = new HashMap<>();
        String line = null;
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\t");

            int year = Integer.valueOf(data[1]);
            int num_sections = Integer.valueOf(data[2]);
            int num_citations = Integer.valueOf(data[3]);

            if (!sections.containsKey(year)) {
                sections.put(year, new ArrayList<>());
                citations.put(year, new ArrayList<>());
                entities.put(year, new HashSet<>());
            }

            sections.get(year).add(num_sections);
            citations.get(year).add(num_citations);
            entities.get(year).add(data[0]);
        }


        Integer[] years = new Integer[sections.size()];
        sections.keySet().toArray(years);

        for (int year : years) {
            double max_sections = sections.get(year).stream().mapToDouble(x -> x).max().getAsDouble();
            double min_sections = sections.get(year).stream().mapToDouble(x -> x).min().getAsDouble();
            double avg_sections = sections.get(year).stream().mapToDouble(x -> x).average().getAsDouble();

            double max_citations = sections.get(year).stream().mapToDouble(x -> x).max().getAsDouble();
            double min_citations = sections.get(year).stream().mapToDouble(x -> x).min().getAsDouble();
            double avg_citations = sections.get(year).stream().mapToDouble(x -> x).average().getAsDouble();


            int num_entities = entities.get(year).size();


            System.out.printf("%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n", year, num_entities, max_sections, min_sections, avg_sections, max_citations, min_citations, avg_citations);
        }

    }
}
