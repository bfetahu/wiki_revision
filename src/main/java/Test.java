

import revisions.YagoTypeTaxonomy;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by besnik on 6/23/17.
 */
public class Test {
    public static void main(String[] args) throws IOException, SQLException {
        YagoTypeTaxonomy tax = YagoTypeTaxonomy.loadYagoTaxonomyDB(1000);

        System.out.println(tax.type_label + "\t" + tax.num_instances);
    }
}
