package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by besnik on 7/14/17.
 */
public class DBUtils {
    public static Map<Map.Entry<String, Integer>, Set<String>> getYagoParentChildTypes(String yago_types_file) throws IOException {
        Map<Map.Entry<String, Integer>, Set<String>> data = new HashMap<>();
        BufferedReader reader = FileUtils.getFileReader(yago_types_file);
        String line;

        //skip the first line
        reader.readLine();

        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\t");
            if (tmp.length != 6) {
                continue;
            }

            String parent_type_label = tmp[4];
            String type_label = tmp[2];
            int level = Integer.valueOf(tmp[tmp.length - 1]);

            Map.Entry<String, Integer> parent_entry = new AbstractMap.SimpleEntry<>(parent_type_label, level - 1);
            if (!data.containsKey(parent_entry)) {
                data.put(parent_entry, new HashSet<>());
            }

            data.get(parent_entry).add(type_label);
        }
        return data;
    }

    /**
     * Load all the entity type ids.
     *
     * @param yago_type_file
     * @return
     * @throws IOException
     */
    public static Map<String, Integer> getEntityTypeIDs(String yago_type_file) throws IOException {
        Map<String, Integer> data = new HashMap<>();
        BufferedReader reader = FileUtils.getFileReader(yago_type_file);
        String line;

        //skip the first line
        reader.readLine();

        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\t");
            if (tmp.length != 6) {
                continue;
            }

            String parent_type_label = tmp[4];
            int parent_type_id = Integer.valueOf(tmp[3]);
            String type_label = tmp[2];
            int child_type_id = Integer.valueOf(tmp[1]);

            if (!data.containsKey(parent_type_label)) {
                data.put(parent_type_label, parent_type_id);
            }
            if (!data.containsKey(type_label)) {
                data.put(type_label, child_type_id);
            }
        }
        return data;
    }

    /**
     * Load the set of entities and the corresponding type associations.
     *
     * @return
     * @throws IOException
     */
    public static Map<String, Set<Integer>> getEntities(String entity_type_file) throws IOException {
        Map<String, Set<Integer>> entities = new HashMap<>();

        BufferedReader reader = FileUtils.getFileReader(entity_type_file);
        String line;

        //skip the first line
        reader.readLine();
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\t");
            if (tmp.length != 4) {
                continue;
            }

            String entity = tmp[1].intern();
            int type_id = Integer.valueOf(tmp[2]);

            if (!entities.containsKey(entity)) {
                entities.put(entity, new HashSet<>());
            }
            entities.get(entity).add(type_id);
        }

        return entities;
    }
}
