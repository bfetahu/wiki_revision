

import revisions.YagoTypeTaxonomy;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by besnik on 6/23/17.
 */
public class Test {
    public static void main(String[] args) throws IOException, SQLException {
//        String entity_types = "/media/fs/data-central/datasets/fetahu/wikipedia_entity_types.csv.gz";
//        String yago_types = "/media/fs/data-central/datasets/fetahu/yago_type_tree_taxonomy.csv.gz";

        String entity_types = "/Users/besnik/Desktop/wikipedia_entity_types.csv.gz";
        String yago_types = "/Users/besnik/Desktop/yago_type_tree_taxonomy.csv.gz";

        YagoTypeTaxonomy tax = YagoTypeTaxonomy.loadYagoTaxonomyDB(entity_types, yago_types);

        //the level of aggregation we are interested at.
        int filter_level = 3;
        Set<YagoTypeTaxonomy> sub_types = new HashSet<>();
        YagoTypeTaxonomy.getChildren(tax, filter_level, sub_types);

        System.out.println(sub_types.size());
    }
}
