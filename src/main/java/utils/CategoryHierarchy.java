package utils;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.lang3.StringUtils;

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
    public boolean visited = false;

    public Map<String, CategoryHierarchy> parents = new HashMap<>();
    public Map<String, CategoryHierarchy> children = new HashMap<>();


    public CategoryHierarchy(String label, int level) {
        this.label = label;
        this.level = level;
    }

    public Set<String> getChildren() {
        Set<String> children = new HashSet<>();

        children.addAll(this.children.keySet());

        for (String child_label : this.children.keySet()) {
            CategoryHierarchy child_category = this.children.get(child_label);
            children.addAll(child_category.getChildren());
        }
        return children;
    }


    /**
     * Return the set of categories that belong to a certain level in the Wikipedia category taxonomy.
     *
     * @param category
     * @param level
     * @param categories
     */
    public static void getChildrenLevel(CategoryHierarchy category, int level, Set<CategoryHierarchy> categories) {
        if (category.level == level) {
            categories.add(category);
            return;
        }

        if (category.children != null && !category.children.isEmpty()) {
            for (String child_category : category.children.keySet()) {
                getChildrenLevel(category.children.get(child_category), level, categories);
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

        Map<String, CategoryHierarchy> all_cats = loadAllCategories(category_file);
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\\s+");
            if (!data[1].contains("<http://www.w3.org/2004/02/skos/core#broader>") || data[2].equals(data[0])) {
                continue;
            }

            String parent_label = data[2].replace("<http://dbpedia.org/resource/Category:", "").replace(">", "");
            String child_label = data[0].replace("<http://dbpedia.org/resource/Category:", "").replace(">", "");

            CategoryHierarchy parent = all_cats.get(parent_label);
            CategoryHierarchy child = all_cats.get(child_label);

            child.level = parent.level + 1;
            parent.children.put(child_label, child);
            child.parents.put(parent_label, parent);
        }

        for (String cat_label : all_cats.keySet()) {
            CategoryHierarchy cat = all_cats.get(cat_label);
            if (!cat.parents.isEmpty()) {
                root.children.put(cat.label, cat);
                cat.parents.put(root.label, root);
            }
        }


        //we first need to break the cycles
        for (String cat_label : all_cats.keySet()) {
            CategoryHierarchy cat = all_cats.get(cat_label);

            cat.parents.keySet().removeAll(cat.children.keySet());
            if (cat.parents.size() == 1) {
                continue;
            }
            if (cat.parents.size() > 1 && cat.parents.containsKey("root")) {
                cat.parents.remove("root");
            } else if (cat.parents.size() == 0) {
                cat.parents.put(root.label, root);
                root.children.put(cat.label, cat);
            }
        }
        fixCategoryGraphHierarchy(root);
        root.reAssignCategoryLevels();
        return root;
    }

    public static Map<String, CategoryHierarchy> loadAllCategories(String file) throws IOException {
        BufferedReader reader = FileUtils.getFileReader(file);
        String line;

        Map<String, CategoryHierarchy> all_cats = new HashMap<>();
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\\s+");
            if (data[1].contains("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")) {
                //the category doesn't, exist, add it into the root
                String cat_label = data[0].replace("<http://dbpedia.org/resource/Category:", "").replace(">", "");
                CategoryHierarchy cat = all_cats.get(cat_label);
                if (cat == null) {
                    cat = new CategoryHierarchy(cat_label, 0);
                    all_cats.put(cat.label, cat);
                }
                continue;
            }

            if (!data[1].contains("<http://www.w3.org/2004/02/skos/core#broader>") || data[2].equals(data[0])) {
                continue;
            }

            String parent_label = data[2].replace("<http://dbpedia.org/resource/Category:", "").replace(">", "");
            String child_label = data[0].replace("<http://dbpedia.org/resource/Category:", "").replace(">", "");

            if (!all_cats.containsKey(parent_label)) {
                CategoryHierarchy cat = all_cats.get(parent_label);
                if (cat == null) {
                    cat = new CategoryHierarchy(parent_label, 0);
                    all_cats.put(cat.label, cat);
                }
            }

            if (!all_cats.containsKey(child_label)) {
                CategoryHierarchy cat = all_cats.get(child_label);
                if (cat == null) {
                    cat = new CategoryHierarchy(child_label, 0);
                    all_cats.put(cat.label, cat);
                }
            }
        }
        return all_cats;
    }

    /**
     * Write the constructed category taxonomy.
     *
     * @param out_file
     * @param sb
     */
    public void printCategories(String out_file, StringBuffer sb) {
        if (sb.length() > 10000) {
            FileUtils.saveText(sb.toString(), out_file, true);
            sb.delete(0, sb.length());
        }

        String tabs = StringUtils.repeat("\t", level);
        sb.append(tabs).append(label).append("\n");

        for (String child_label : children.keySet()) {
            CategoryHierarchy cat = children.get(child_label);
            cat.printCategories(out_file, sb);
        }

        FileUtils.saveText(sb.toString(), out_file, true);
        sb.delete(0, sb.length());
    }

    /**
     * Remove parent categories whose level is higher than the minimum level.
     */
    public static void fixCategoryGraphHierarchy(CategoryHierarchy category) {
        if (!category.label.equals("root") && category.parents.size() > 1) {
            //if its not the root category, we check the parents of this category and remove those parents for which
            Map<String, CategoryHierarchy> sub_parents = category.parents;
            int max_level = sub_parents.values().stream().map(x -> x.level).max((x, y) -> x.compareTo(y)).get();
            List<Map.Entry<String, CategoryHierarchy>> filtered_parents = sub_parents.entrySet().stream().filter(x -> x.getValue().level == max_level).collect(Collectors.toList());

            category.parents.clear();
            filtered_parents.forEach(x -> category.parents.put(x.getKey(), x.getValue()));

            category.level = max_level + 1;
        }
    }

    public void reAssignCategoryLevels() {
        this.visited = true;
        if (label.equals("root")) {
            level = 0;
        } else {
            fixCategoryGraphHierarchy(this);
            Map.Entry<String, CategoryHierarchy> parent = parents.entrySet().iterator().next();

            int level = parent.getValue().level;
            this.level = level + 1;
        }

        if (!children.isEmpty()) {
            Iterator<Map.Entry<String, CategoryHierarchy>> child_keys = children.entrySet().iterator();
            while (child_keys.hasNext()) {
                Map.Entry<String, CategoryHierarchy> cat = child_keys.next();
                if (cat.getValue().visited) {
                    child_keys.remove();
                    continue;
                }
                cat.getValue().reAssignCategoryLevels();
            }
        }
    }

    public String toString() {
        return new StringBuffer().append(label).append("\t").append(level).toString();
    }


    //args[0]=skos_categories, args[1]=article_categories, args[2]=outputFile, args[3]=level
    public static void main(String[] args) throws IOException, CompressorException {
        String[] args1 = {"/Users/besnik/Desktop/skos_categories_en.nt.gz", "/Users/besnik/Desktop/article_categories_en.ttl.bz2", "/Users/besnik/Desktop/output.txt", "2"};
        args = args1;

        //for testing
        String cat2cat_mappings = args[0];
        System.out.println("Read Category Mappings...");
        Map<String, Set<String>> categoriesToArticles = readCategoryMappings(args[1]);

        System.out.println("Read Category Graph...");
        CategoryHierarchy cat = CategoryHierarchy.readCategoryGraph(cat2cat_mappings);

//        StringBuffer sb = new StringBuffer();
//        cat.printCategories("/Users/besnik/Desktop/category_hieararchy.csv", sb);
//        FileUtils.saveText(sb.toString(), "/Users/besnik/Desktop/category_hieararchy.csv", true);

        System.out.println("Retrieve subcategories...");
        //get all categories at a specific level
        Set<CategoryHierarchy> cat_2 = new HashSet<>();
        CategoryHierarchy.getChildrenLevel(cat, Integer.parseInt(args[3]), cat_2);
        CategoryHierarchy.getChildrenLevel(cat, 1, cat_2);

        //iterate to get all sub-categories for all categories at the given level
        Map<String, Set<String>> levelcatToSubcategories = new HashMap<>();
        iterateOverHierarchy(cat_2, levelcatToSubcategories);

        //retrieve all articles for all subcategories
        System.out.println("Retrieve entities...");
        Map<String, Set<String>> levelcategoriesToEntities = new HashMap<>();
        for (String levelcat : levelcatToSubcategories.keySet()) {
            Set<String> entities = new HashSet<>();
            for (String subcat : levelcatToSubcategories.get(levelcat)) {
                if (!categoriesToArticles.containsKey(subcat)) {
                    System.out.println("No entities for category: " + subcat);
                    continue; //in case the subcategory contains no entities
                }
                entities.addAll(categoriesToArticles.get(subcat));
            }
            levelcategoriesToEntities.put(levelcat, entities);
        }

        System.out.println("Output results...");
        writeToOutputFile(levelcategoriesToEntities, args[2]);
        System.out.println("Done");
    }


    public static void writeToOutputFile(Map<String, Set<String>> levelcategoriesToEntities, String outputFile) {
        String tab = "\t";
        StringBuffer sb = new StringBuffer();
        String newline = "\n";
        for (String levelcat : levelcategoriesToEntities.keySet()) {
            sb.append(levelcat).append(tab).append(levelcategoriesToEntities.get(levelcat)).append(newline);
        }
        FileUtils.saveText(sb.toString(), outputFile);
    }


    public static void iterateOverHierarchy(Set<CategoryHierarchy> cats_at_level, Map<String, Set<String>> subcategories) {
        for (CategoryHierarchy cat : cats_at_level) {
            Set<String> subcategories_of_cat = new HashSet<>();
            subcategories.put(cat.label, subcategories_of_cat);
            iterate(cat, subcategories_of_cat);
        }
    }

    public static void iterate(CategoryHierarchy current_cat, Set<String> subcategories) {
        subcategories.add(current_cat.label);
        for (String child : current_cat.children.keySet()) {
            iterate(current_cat.children.get(child), subcategories);
        }
    }


    /**
     * Read the entity-category associations.
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static Map<String, Set<String>> readCategoryMappings(String file) throws IOException {
        Map<String, Set<String>> entity_cats = new HashMap<>();
        BufferedReader reader = FileUtils.getFileReader(file);

        String line;
        while ((line = reader.readLine()) != null) {
            line = line.replace("<http://dbpedia.org/resource/", "").replace(">", "");
            String[] parts = line.split("\\s+");
            String article = parts[0];
            String category = parts[2].replace("Category:", "");

            if (!entity_cats.containsKey(category)) {
                entity_cats.put(category, new HashSet<>());
            }
            entity_cats.get(category).add(article);
        }

        reader.close();
        return entity_cats;
    }
}
