


import io.FileUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Created by besnik on 6/23/17.
 */
public class Test {
    public static void main(String[] args) throws IOException, SQLException {
        Map<String, List<Long>> revisions = (Map<String, List<Long>>) FileUtils.readObject("/Users/besnik/Desktop/rev_pairs_part_0.hash");
        System.out.println(revisions.keySet());
    }
}
