package revisions;


import utils.DBUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by besnik on 9/10/15.
 */
public class YagoTypeTaxonomy {
    public int type_id;
    public String type_label;
    public boolean is_root = true;

    public int num_instances;
    public Set<YagoTypeTaxonomy> children;

    public Set<String> entities;
    public int level;

    public YagoTypeTaxonomy(String type_label, int level) {
        this.type_label = type_label;
        children = new HashSet<>();
        entities = new HashSet<>();

        this.level = level;
    }

    /**
     * Return the set of types that belong to a certain level in the YAGO type taxonomy.
     *
     * @param type
     * @param level
     * @param filtered_types
     */
    public static void getChildren(YagoTypeTaxonomy type, int level, Set<YagoTypeTaxonomy> filtered_types) {
        if (type.level == level) {
            filtered_types.addAll(type.children);
            return;
        }

        if (type.children != null && !type.children.isEmpty()) {
            for (YagoTypeTaxonomy child_type : type.children) {
                getChildren(child_type, level, filtered_types);
            }
        }
    }

    /**
     * Parse the YAGO taxonomy from the DB and add them into the type tree.
     */
    public static YagoTypeTaxonomy loadYagoTaxonomyDB(String entity_type_file, String type_taxonomy_file) throws IOException {
        Map<String, YagoTypeTaxonomy> types = new HashMap<>();
        Map<String, Integer> yago_types = DBUtils.getEntityTypeIDs(type_taxonomy_file);

        Map<Integer, Set<String>> entity_types = DBUtils.getEntities(entity_type_file);

        Map<Map.Entry<String, Integer>, Set<String>> parent_child_types = DBUtils.getYagoParentChildTypes(type_taxonomy_file);
        for (Map.Entry<String, Integer> parent_type_entry : parent_child_types.keySet()) {
            if (!yago_types.containsKey(parent_type_entry.getKey())) {
                continue;
            }
            int parent_id = yago_types.get(parent_type_entry.getKey());
            for (String child_type_entry : parent_child_types.get(parent_type_entry)) {
                if (!yago_types.containsKey(child_type_entry)) {
                    continue;
                }
                int child_id = yago_types.get(child_type_entry);

                if (!types.containsKey(parent_type_entry.getKey())) {
                    types.put(parent_type_entry.getKey(), new YagoTypeTaxonomy(parent_type_entry.getKey(), parent_type_entry.getValue()));
                }

                if (!types.containsKey(child_type_entry)) {
                    types.put(child_type_entry, new YagoTypeTaxonomy(child_type_entry, parent_type_entry.getValue() + 1));
                }

                YagoTypeTaxonomy parent = types.get(parent_type_entry.getKey());
                parent.type_id = parent_id;

                YagoTypeTaxonomy child = types.get(child_type_entry);
                child.is_root = false;
                child.type_id = child_id;
                Set<String> sub_entities = entity_types.get(child_id);
                child.num_instances = sub_entities.size();
                child.entities = sub_entities;

                parent.children.add(child);
            }
        }

        //add all the root nodes into a ROOT type in the taxonomy tree.
        return types.get("owl:Thing");
    }

    public String toString() {
        return type_id + "-" + type_label;
    }
}
