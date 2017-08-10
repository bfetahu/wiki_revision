package revisions;

import com.google.common.base.Splitter;
import gnu.trove.list.array.TLongArrayList;
import io.FileUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by besnik on 8/8/17.
 */
public class RevPairUtils {
    public static void main(String[] args) throws IOException {
        createRevMaps(args);
    }

    public static void createRevMaps(String args[]) throws IOException {
        //generate all possible revision pairs for an entity
        BufferedReader reader = FileUtils.getFileReader(args[0]);

        TLongArrayList rev_pairs = new TLongArrayList();
        String line;

        String entity = "";
        int counter = 0;
        rev_pairs.add(-1l);
        while ((line = reader.readLine()) != null) {
            counter++;
            if (counter % 1000 == 0) {
                System.out.printf("There are %d lines processed so far.\n", counter);
            }
            Iterator<String> data_iter = Splitter.onPattern("\t").split(line).iterator();
            long rev_id = Long.valueOf(data_iter.next());
            String current_entity = data_iter.next().intern();
            if (current_entity.contains("Talk:") || current_entity.contains("User:") || current_entity.contains("Category:")) {
                continue;
            }

            if (!entity.isEmpty() && !current_entity.equals(entity)) {
                rev_pairs.add(-1l);
            }
            rev_pairs.add(rev_id);
            entity = current_entity;
        }

        String out_file = args[1] + ".hash";
        FileUtils.saveObject(rev_pairs, out_file);
        System.out.println("Finished writing revision pairs for " + rev_pairs.size());
    }

    public static void convertMaps(String map_path, String out) {
        Set<String> files = new HashSet<>();
        FileUtils.getFilesList(map_path, files);

        TLongArrayList all = new TLongArrayList();
        files.parallelStream().forEach(file -> {
            Map<String, List<Long>> map = (Map<String, List<Long>>) FileUtils.readObject(file);

            for (String entity : map.keySet()) {
                all.add(-1l);
                map.get(entity).forEach(all::add);
            }
            System.out.println("Finished processing file " + file);
        });

        FileUtils.saveObject(all, out);
    }

}
