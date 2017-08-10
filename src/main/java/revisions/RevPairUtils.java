package revisions;

import com.google.common.base.Splitter;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import io.FileUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by besnik on 8/8/17.
 */
public class RevPairUtils {
    public static void main(String[] args) throws IOException {
        createCompressedRevMaps(args);
    }

    public static void createRevMaps(String args[]) throws IOException {
        //generate all possible revision pairs for an entity
        BufferedReader reader = FileUtils.getFileReader(args[0]);
        TLongArrayList rev_pairs = new TLongArrayList();
        String line;

        String entity = "";
        int counter = 0;
        int part = 0;
        rev_pairs.add(-1l);
        while ((line = reader.readLine()) != null) {
            counter++;
            if (counter % 10000 == 0) {
                System.out.printf("There are %d lines processed so far.\n", counter);
            }
            Iterator<String> data_iter = Splitter.onPattern("\t").split(line).iterator();
            long rev_id = Long.valueOf(data_iter.next());
            String current_entity = data_iter.next().intern();

            if (current_entity.contains("Talk:") || current_entity.contains("User:") || current_entity.contains("Category:")) {
                continue;
            }


            if (!current_entity.equals(entity) && counter > 100000000) {
                String out_file = args[1] + "_part_" + part + ".hash";
                FileUtils.saveObject(rev_pairs, out_file);
                part++;
                rev_pairs.clear();
                counter = 0;
            }

            if (!entity.isEmpty() && !current_entity.equals(entity)) {
                rev_pairs.add(-1l);
            }
            rev_pairs.add(rev_id);
            entity = current_entity;
        }

        String out_file = args[1] + "_part_" + part + ".hash";
        FileUtils.saveObject(rev_pairs, out_file);
        System.out.println("Finished writing revision pairs for " + rev_pairs.size());
    }

    public static void createCompressedRevMaps(String args[]) throws IOException {
        //generate all possible revision pairs for an entity
        int threshold = Integer.valueOf(args[2]);
        BufferedReader reader = FileUtils.getFileReader(args[0]);
        Map<Integer, TIntArrayList> rev_pairs = new HashMap<>();
        String line;
        int counter = 0;
        int part = 0;
        int line_counter = 0;
        String entity = "";
        while ((line = reader.readLine()) != null) {
            counter++;
            line_counter++;
            if (counter % 1000000 == 0) {
                System.out.printf("There are %d lines processed so far.\n", counter);
            }
            Iterator<String> data_iter = Splitter.onPattern("\t").split(line).iterator();
            long rev_id = Long.valueOf(data_iter.next());
            String current_entity = data_iter.next().intern();

            if (current_entity.contains("Talk:") || current_entity.contains("User:") || current_entity.contains("Category:") || current_entity.contains("User talk:") || current_entity.contains("Category talk:")) {
                continue;
            }

            if (!current_entity.equals(entity) && line_counter > threshold) {
                line_counter = 0;
                String out_file = args[1] + "_part_" + part + ".hash";
                FileUtils.saveObject(rev_pairs, out_file);
                part++;

                rev_pairs.clear();
            }

            int current_hash_code = current_entity.hashCode();
            if (!rev_pairs.containsKey(current_hash_code)) {
                rev_pairs.put(current_hash_code, new TIntArrayList());
            }
            rev_pairs.get(current_hash_code).add(Long.hashCode(rev_id));
        }

        String out_file = args[1] + "_part_" + part + ".hash";
        FileUtils.saveObject(rev_pairs, out_file);
        System.out.println("Finished writing revision pairs for " + rev_pairs.size());
    }
}
