package utils;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
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

            if (data[1].contains("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")) {
                String cat_label = data[0];
                CategoryHierarchy cat = new CategoryHierarchy(cat_label, 0);
                all_cats.put(cat_label, cat);
                continue;
            }

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

        String tabs = StringUtils.repeat("\t", category.level);
        sb.append(tabs).append(category.label).append("\n");

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


    public String toString() {
        return new StringBuffer().append(label).append("\t").append(level).toString();
    }


    //args[0]=skos_categories, args[1]=article_categories, args[2]=outputFile, args[3]=level
    public static void main(String[] args) throws IOException, CompressorException {
        //for testing
        String cat2cat_mappings = args[0];
        Map<String, Set<String>> categoriesToArticles = new HashMap<>();
        System.out.println("Read Category Mappings...");
        readCategoryMappings(args[1], categoriesToArticles);

        System.out.println("Read Category Graph...");
        CategoryHierarchy cat = CategoryHierarchy.readCategoryGraph(cat2cat_mappings);
        cat.reAssignCategoryLevels();
        cat.fixCategoryGraphHierarchy();

        System.out.println("Retrieve subcategories...");
        //get all categories at a specific level
        Set<CategoryHierarchy> cat_2 = new HashSet<>();
        CategoryHierarchy.getChildren(cat, Integer.parseInt(args[3]), cat_2);
        CategoryHierarchy.getChildren(cat, 1, cat_2);
//        System.out.println(cat_2);

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
            Set<String> subcategories_of_cat = new HashSet<String>();
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

    public static void readCategoryMappings(String file, Map<String, Set<String>> categoriesToArticles) throws IOException, CompressorException {
        BufferedReader br_articlesCategoryMappings = getBufferedReaderForCompressedFile(file);

        String line;
        String[] parts;
        String article;
        String category;
        String dbpediaResource = "<http://dbpedia.org/resource/";
        String close_character = ">";
        String emptyString = "";
        Set<String> set;
        String space = " ";
        String category_string = "Category:";

        while ((line = br_articlesCategoryMappings.readLine()) != null) {
            line = line.replace(dbpediaResource, emptyString).replace(close_character, emptyString);
            parts = line.split(space);
            article = parts[0];
            category = parts[2].replace(category_string, emptyString);

            if (categoriesToArticles.containsKey(category)) {
                set = categoriesToArticles.get(category);
                set.add(article);
//                System.out.println("Category: " + category + " contains articles: " + set);
            } else {
                Set<String> newSet = new HashSet<String>();
                newSet.add(article);
                categoriesToArticles.put(category, newSet);
//                System.out.println("Category: " + category + " contains articles: " + newSet);
            }
        }

        br_articlesCategoryMappings.close();
    }


    public static BufferedReader getBufferedReaderForCompressedFile(String fileIn) throws FileNotFoundException, CompressorException {
        FileInputStream fin = new FileInputStream(fileIn);
        BufferedInputStream bis = new BufferedInputStream(fin);
        CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
        BufferedReader br2 = new BufferedReader(new InputStreamReader(input));
        return br2;
    }
}
