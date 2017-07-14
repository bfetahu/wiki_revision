package revisions;


import gnu.trove.map.hash.TIntIntHashMap;
import utils.DBUtils;

import java.sql.*;
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

    public YagoTypeTaxonomy(String type_label) {
        this.type_label = type_label;
        children = new HashSet<>();
    }

    /**
     * Parse the YAGO taxonomy from the DB and add them into the type tree.
     */
    public static YagoTypeTaxonomy loadYagoTaxonomyDB(int threshold) throws SQLException {
        TIntIntHashMap type_freqs = DBUtils.getYagoTypeFrequenciesT2(threshold);
        Map<String, YagoTypeTaxonomy> types = new HashMap<>();
        Map<String, Integer> yago_types = DBUtils.loadYagoTypesInverted();

        Map<String, Set<String>> parent_child_types = DBUtils.getYagoParentChildTypes();
        for (String parent_label : parent_child_types.keySet()) {
            if (!yago_types.containsKey(parent_label)) {
                continue;
            }
            int parent_id = yago_types.get(parent_label);
            for (String child_label : parent_child_types.get(parent_label)) {
                if (!yago_types.containsKey(child_label)) {
                    continue;
                }
                int child_id = yago_types.get(child_label);

                if (!type_freqs.containsKey(parent_id) || !type_freqs.containsKey(child_id)) {
                    continue;
                }

                if (!types.containsKey(parent_label)) {
                    types.put(parent_label, new YagoTypeTaxonomy(parent_label));
                }

                if (!types.containsKey(child_label)) {
                    types.put(child_label, new YagoTypeTaxonomy(child_label));
                }

                YagoTypeTaxonomy parent = types.get(parent_label);
                parent.type_id = parent_id;
                parent.num_instances = type_freqs.containsKey(parent.type_id) ? type_freqs.get(parent.type_id) : 0;

                YagoTypeTaxonomy child = types.get(child_label);
                child.is_root = false;
                child.type_id = child_id;
                child.num_instances = type_freqs.containsKey(child.type_id) ? type_freqs.get(child.type_id) : 0;

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
