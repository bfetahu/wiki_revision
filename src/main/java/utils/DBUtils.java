package utils;

import gnu.trove.map.hash.TIntIntHashMap;

import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by besnik on 7/14/17.
 */
public class DBUtils {
    public static String mysql_user = "";
    public static String mysql_pwd = "";

    /**
     * Load the frequencies of YAGO types.
     *
     * @return
     * @throws SQLException
     */
    public static TIntIntHashMap getYagoTypeFrequenciesT2(int threshold) throws SQLException {
        Connection conn = getMySQLConnection();
        PreparedStatement prep = conn.prepareStatement("SELECT type_id, entity_count FROM  type_entity_count WHERE entity_count > " + threshold);
        TIntIntHashMap data = new TIntIntHashMap();
        ResultSet rst = prep.executeQuery();
        while (rst.next()) {
            int entity_instances = rst.getInt("entity_count");
            int type_id = rst.getInt("type_id");
            data.put(type_id, entity_instances);
        }
        return data;
    }

    /**
     * Loads the types.
     *
     * @return
     * @throws SQLException
     */
    public static Map<String, Integer> loadYagoTypesInverted() throws SQLException {
        Map<String, Integer> data = new HashMap<>();
        Connection conn = getMySQLConnection();
        PreparedStatement prep = conn.prepareStatement("SELECT DISTINCT type_id, type_name FROM yago_types");
        ResultSet rst = prep.executeQuery();
        while (rst.next()) {
            data.put(rst.getString("type_name"), rst.getInt("type_id"));
        }
        return data;
    }

    public static Map<String, Set<String>> getYagoParentChildTypes() throws SQLException {
        Map<String, Set<String>> data = new HashMap<>();
        Connection conn = getMySQLConnection();
        String sql = "SELECT parent_type_label, type_label FROM yago_type_taxonomy_tree";
        PreparedStatement prep = conn.prepareStatement(sql);

        ResultSet rst = prep.executeQuery();
        while (rst.next()) {
            String parent_type_label = rst.getString("parent_type_label");
            String type_label = rst.getString("type_label");

            if (!data.containsKey(parent_type_label)) {
                data.put(parent_type_label, new HashSet<>());
            }

            data.get(parent_type_label).add(type_label);
        }
        return data;
    }

    private static Connection conn;

    @Override
    public void finalize() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    public static Connection getMySQLConnection() {

        if (conn == null) {
            String dbURL = "jdbc:mysql://db.l3s.uni-hannover.de:3306/wikipedia_population?useUnicode=true&characterEncoding=utf-8";
            try {
                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection(dbURL, mysql_user, mysql_pwd);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return conn;
        }
        return conn;
    }
}
