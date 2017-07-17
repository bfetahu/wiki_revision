package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by besnik on 7/17/17.
 */
public class CategoryHierarchy {
    public String label;
    public int level;


    public Map<String, CategoryHierarchy> parents = new HashMap<>();
    public Map<String, CategoryHierarchy> children = new HashMap<>();

    public CategoryHierarchy(String label, int level) {
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
     * Return the set of categories that belong to a certain level in the Wikipedia category taxonomy.
     *
     * @param category
     * @param level
     * @param categories
     */
    public static void getChildren(CategoryHierarchy category, int level, Set<CategoryHierarchy> categories) {
        if (category.level == level) {
            categories.add(category);
            return;
        }

        if (category.children != null && !category.children.isEmpty()) {
            for (String child_category : category.children.keySet()) {
                getChildren(category.children.get(child_category), level, categories);
            }
        }
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

            String parent_label = data[2].replace("<http://dbpedia.org/resource/Category:", "").replace(">", "");
            String child_label = data[0].replace("<http://dbpedia.org/resource/Category:", "").replace(">", "");

            CategoryHierarchy parent = all_cats.get(parent_label);
            if (parent == null) {
                //the category doesnt, exist, add it into the root
                parent = new CategoryHierarchy(parent_label, root.level + 1);
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

        for (String category_label : all_cats.keySet()) {
            CategoryHierarchy category = all_cats.get(category_label);
            if (category.parents.isEmpty()) {
                root.children.put(category_label, category);
                category.parents.put(root.label, root);
            }
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
                CategoryHierarchy parent = parents.get(parent_label);
                sb.append(parent).append("\t").append(category).append("\n");
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
     */
    public void fixCategoryGraphHierarchy() {
        if (!label.equals("root")) {
            //if its not the root category, we check the parents of this category and remove those parents for which

            Map<String, CategoryHierarchy> sub_parents = parents;
            int max_level = sub_parents.values().stream().map(x -> x.level).max((x, y) -> x.compareTo(y)).get();
            List<Map.Entry<String, CategoryHierarchy>> filtered_parents = sub_parents.entrySet().stream().filter(x -> x.getValue().level == max_level).collect(Collectors.toList());

            parents.clear();
            filtered_parents.forEach(x -> parents.put(x.getKey(), x.getValue()));

            level = max_level + 1;
        }

        for (String child : children.keySet()) {
            CategoryHierarchy child_category = children.get(child);
            child_category.fixCategoryGraphHierarchy();
        }
    }

    public void reAssignCategoryLevels() {
        if (label.equals("root")) {
            level = 0;
        } else {
            Map.Entry<String, CategoryHierarchy> parent = parents.entrySet().iterator().next();
            int level = parent.getValue().level;
            this.level = level + 1;
        }

        if (!children.isEmpty()) {
            children.keySet().forEach(cat -> children.get(cat).reAssignCategoryLevels());
        }
    }

    public static void reAssignCategoryLevelsDFS(CategoryHierarchy cat) {
        Queue<CategoryHierarchy> q = new LinkedList();
        q.add(cat);


        while (!q.isEmpty()) {
            CategoryHierarchy cat_q = q.remove();
            if (cat_q.label.equals("root")) {
                cat_q.level = 0;
            } else {
                int max_level = 0;
                for (Map.Entry<String, CategoryHierarchy> parent : cat_q.parents.entrySet()) {
                    int level = parent.getValue().level + 1;

                    if (max_level < level) {
                        max_level = level;
                    }
                }
                cat_q.level = max_level;
            }

            q.addAll(cat_q.children.values());
        }
    }

    public String toString() {
        return new StringBuffer().append(label).append("\t").append(level).toString();
    }


    public static void main(String[] args) throws IOException {
        String cat_file = "/Users/besnik/Desktop/skos_categories_en.nt";//args[0];
        String out_file = "/Users/besnik/Desktop/category_hierarchy.csv";//args[1];

        CategoryHierarchy cat = CategoryHierarchy.readCategoryGraph(cat_file);
        cat.fixCategoryGraphHierarchy();
        cat.reAssignCategoryLevels();

        Set<CategoryHierarchy> cat_4 = new HashSet<>();
        CategoryHierarchy.getChildren(cat, 4, cat_4);

        System.out.println(cat_4);

        StringBuffer sb = new StringBuffer();
        printCategories(cat, out_file, sb);

        System.out.println(sb.toString());
    }
}
