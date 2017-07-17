package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by besnik on 7/17/17.
 */
public class CategoryHierarchy {
    public String label;
    public int level;


    public Map<String, CategoryHierarchy> parents;
    public Map<String, CategoryHierarchy> children;

    public CategoryHierarchy(String label, int level) {
        children = new HashMap<>();
        parents = new HashMap<>();

        this.label = label;
        this.level = level;
    }


    /**
     * Find a specific category in the category graph.
     *
     * @param label
     * @return
     */
    public CategoryHierarchy findCategory(String label) {
        if (this.label.equals(label)) {
            return this;
        }

        if (!children.isEmpty()) {
            for (String child_label : children.keySet()) {
                CategoryHierarchy rst = children.get(child_label).findCategory(label);

                if (rst != null) {
                    return rst;
                }
            }
        }

        return null;
    }

    /**
     * Construct the category graph.
     *
     * @param category_file
     * @return
     * @throws IOException
     */
    public static CategoryHierarchy readCategoryGraph(String category_file) throws IOException {
        CategoryHierarchy root = new CategoryHierarchy("root", 0);

        BufferedReader reader = FileUtils.getFileReader(category_file);
        String line;

        Map<String, CategoryHierarchy> all_cats = new HashMap<>();

        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\\s+");

            if (!data[1].contains("broader")) {
                continue;
            }

            String parent_label = data[0].replace("<http://dbpedia.org/resource/", "").replace(">", "");
            String child_label = data[2].replace("<http://dbpedia.org/resource/", "").replace(">", "");

            CategoryHierarchy parent = all_cats.get(parent_label);
            if (parent == null) {
                //the category doesnt, exist, add it into the root
                parent = new CategoryHierarchy(parent_label, root.level + 1);
                root.children.put(parent_label, parent);

                parent.parents.put(root.label, root);

                all_cats.put(parent_label, parent);
            }

            CategoryHierarchy child = all_cats.get(child_label);
            if (child == null) {
                //the category didn't exist before
                child = new CategoryHierarchy(child_label, parent.level + 1);
                parent.children.put(child_label, child);

                all_cats.put(child_label, child);
            }
            child.parents.put(parent_label, parent);
        }

        return root;
    }


    /**
     * Write the constructed category taxonomy.
     *
     * @param category
     * @param out_file
     * @param sb
     */
    public static void printCategories(CategoryHierarchy category, String out_file, StringBuffer sb) {
        if (sb.length() > 10000) {
            FileUtils.saveText(sb.toString(), out_file, true);
            sb.delete(0, sb.length());
        }

        if (category.label.equals("root")) {
            sb.append("NA\t-1\t").append(category).append("\n");
        } else {
            Map<String, CategoryHierarchy> parents = category.parents;

            for (String parent_label : parents.keySet()) {
                sb.append(parents.get(parent_label)).append("\t").append(category).append("\n");
            }
        }


        for (String child_label : category.children.keySet()) {
            printCategories(category.children.get(child_label), out_file, sb);
        }

        FileUtils.saveText(sb.toString(), out_file, true);
        sb.delete(0, sb.length());
    }

    /**
     * Remove parent categories whose level is higher than the minimum level.
     *
     * @param category
     */
    public static void fixCategoryGraphHierarchy(CategoryHierarchy category) {
        if (!category.label.equals("root")) {
            //if its not the root category, we check the parents of this category and remove those parents for which

            Map<String, CategoryHierarchy> parents = category.parents;
            int max_level = parents.values().stream().map(x -> x.level).max((x, y) -> x.compareTo(y)).get();
            List<Map.Entry<String, CategoryHierarchy>> filtered_parents = parents.entrySet().stream().filter(x -> x.getValue().level < max_level).collect(Collectors.toList());

            parents.clear();
            filtered_parents.forEach(x -> parents.put(x.getKey(), x.getValue()));

            category.level = max_level + 1;
        }

        for (String child : category.children.keySet()) {
            CategoryHierarchy child_category = category.children.get(child);
            fixCategoryGraphHierarchy(child_category);
        }
    }

    public String toString() {
        return new StringBuffer().append(label).append("\t").append(level).toString();
    }


    public static void main(String[] args) throws IOException {
        String cat_file = "/Users/besnik/Desktop/skos_categories_en.nt";//args[0];
        String out_file = "/Users/besnik/Desktop/category_hierarchy.csv";//args[1];

        CategoryHierarchy cat = CategoryHierarchy.readCategoryGraph(cat_file);
        CategoryHierarchy.fixCategoryGraphHierarchy(cat);

        StringBuffer sb = new StringBuffer();
        printCategories(cat, out_file, sb);

        System.out.println(sb.toString());
    }
}
