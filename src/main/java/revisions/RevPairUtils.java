package revisions;

import com.google.common.base.Splitter;
import gnu.trove.list.array.TIntArrayList;
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
        splitMaps(args);
    }

    public static void splitMaps(String args[]) throws IOException {
        Map<Integer, TIntArrayList> map = (Map<Integer, TIntArrayList>) FileUtils.readObject(args[0]);
        System.out.println(map.size());

        int mid_size = (int) (map.size() / 2.0);
        Map<Integer, TIntArrayList> map_1 = new HashMap<>();

        int counter = 0;
        for (int key : map.keySet()) {
            map_1.put(key, map.get(key));
            counter++;

            if (counter > mid_size) {
                break;
            }
        }
        FileUtils.saveObject(map_1, args[1]);
        counter = 0;
        map_1.clear();
        for (int key : map.keySet()) {
            if (counter <= mid_size) {
                counter++;
                continue;
            }
            map_1.put(key, map.get(key));
            counter++;
        }
        FileUtils.saveObject(map_1, args[2]);
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
