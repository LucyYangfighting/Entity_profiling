
import java.io.FileWriter;
import java.sql.*;
import java.util.*;

//Label the entity after creating the final tag set
public class attach_tags {
    private static String sqlitePath = "/base_data/HSP_ALL.sqlite";
    private static void tags(Connection db) throws Exception{

        Map<ArrayList<Integer>, HashSet<Integer>> type_pre_pro_en_Map = new HashMap<ArrayList<Integer>, HashSet<Integer>>();
        Map<Integer, String> mapping_map = new HashMap<Integer, String>();
        Map<ArrayList<Integer>, ArrayList<String>> tags_all_map = new HashMap<ArrayList<Integer>, ArrayList<String>>();
        Map<ArrayList<Integer>, HashSet<Integer>> node_Map = new HashMap<ArrayList<Integer>, HashSet<Integer>>();
        Map<ArrayList<Integer>, HashSet<Integer>> en_Map = new HashMap<ArrayList<Integer>, HashSet<Integer>>();
        Map<ArrayList<String>, HashSet<Integer>> en_num_Map = new HashMap<ArrayList<String>, HashSet<Integer>>();
        double value_min;
        double value_max;
        double value;
        int flg; //Judging whether it is closed("]") or open(")")
        try(PreparedStatement entity_Stmt = db.prepareStatement("SELECT node_id, type_id  FROM nodes_type ; ");
            PreparedStatement relation_Stmt = db.prepareStatement("SELECT entity1_id, predicate_id, entity2_id, entity1_type_id FROM salience; ");
            PreparedStatement property_Stmt = db.prepareStatement("SELECT DISTINCT entity_id, predicate_id, object_id,entity_type_id FROM property_triples ; ");
            PreparedStatement value_Stmt = db.prepareStatement("SELECT id, content FROM mapping");
            PreparedStatement en_true_Stmt = db.prepareStatement("SELECT entity_id,object_id FROM property_triples WHERE entity_type_id=? AND predicate_id=? AND object_type_id!=2 AND object_type_id!=3; ");
            PreparedStatement relation_pro_Stmt = db.prepareStatement("SELECT DISTINCT entity1_type_id, entity1_id,predicate_id,entity2_id, property_id,property_value FROM relation_property_str; ");
            PreparedStatement relation_pro_num_Stmt = db.prepareStatement("SELECT DISTINCT entity1_type, entity1_id,predicate_id,entity2_id, property_id,property_value_range FROM relation_property_num; ");
            PreparedStatement tags_Stmt = db.prepareStatement("SELECT classes,types,predicate, property, object FROM tags_sort ORDER BY ranking; ");

        ) {
            ResultSet property = property_Stmt.executeQuery();
            while(property.next()) {
                HashSet<Integer> type_pre_pro_en_set = new HashSet<Integer>();
                ArrayList<Integer> type_pre_pro = new ArrayList<Integer>();

                Integer entityid = property.getInt(1);
                Integer predicateid = property.getInt(2);
                Integer objectid = property.getInt(3);
                Integer entity_typeid = property.getInt(4);

                type_pre_pro.add(entity_typeid);
                type_pre_pro.add(predicateid);
                type_pre_pro.add(objectid);
                if (!type_pre_pro_en_Map.containsKey(type_pre_pro)) {
                    type_pre_pro_en_set.add(entityid);
                    type_pre_pro_en_Map.put(type_pre_pro, type_pre_pro_en_set);
                } else {
                    type_pre_pro_en_set = type_pre_pro_en_Map.get(type_pre_pro);
                    type_pre_pro_en_set.add(entityid);
                    type_pre_pro_en_Map.put(type_pre_pro, type_pre_pro_en_set);
                }
            }
            ResultSet mapping = value_Stmt.executeQuery();
            while (mapping.next()) {
                Integer id = mapping.getInt(1);
                String content = mapping.getString(2);
                mapping_map.put(id,content);
            }

            ResultSet rel_str = relation_Stmt.executeQuery();
            while(rel_str.next()) {
                HashSet<Integer> node_set = new HashSet<Integer>();
                ArrayList<Integer> node_list = new ArrayList<Integer>();
                Integer en1_id = rel_str.getInt(1);
                Integer pro_id = rel_str.getInt(2);
                Integer en2_id = rel_str.getInt(3);
                Integer en1_type = rel_str.getInt(4);
                node_list.add(en1_type);
                node_list.add(pro_id);
                node_list.add(en2_id);
                if (!node_Map.containsKey(node_list)) {
                    node_set.add(en1_id);
                    node_Map.put(node_list, node_set);
                } else {
                    node_set = node_Map.get(node_list);
                    node_set.add(en1_id);
                    node_Map.put(node_list, node_set);
                }
            }

            ResultSet entity = relation_pro_Stmt.executeQuery();
            while(entity.next()) {
                HashSet<Integer> en_set = new HashSet<Integer>();
                ArrayList<Integer> en_list = new ArrayList<Integer>();

                Integer entity1_type_id = entity.getInt(1);
                Integer entity1_id = entity.getInt(2);
                Integer predicate_id = entity.getInt(3);
                Integer entity2_id = entity.getInt(4);
                Integer property_id = entity.getInt(5);
                Integer property_value = entity.getInt(6);

                en_list.add(entity1_type_id);
                en_list.add(predicate_id);
                en_list.add(property_id);
                en_list.add(property_value);
                if (!en_Map.containsKey(en_list)) {
                    en_set.add(entity1_id);
                    en_Map.put(en_list, en_set);
                } else {
                    en_set = en_Map.get(en_list);
                    en_set.add(entity1_id);
                    en_Map.put(en_list, en_set);
                }
            }
            ResultSet entity_num = relation_pro_num_Stmt.executeQuery();
            while(entity_num.next()) {
                HashSet<Integer> en_set = new HashSet<Integer>();
                ArrayList<String> en_list = new ArrayList<String>();

                Integer entity1_type_id = entity_num.getInt(1);
                Integer entity1_id = entity_num.getInt(2);
                Integer predicate_id = entity_num.getInt(3);
                Integer entity2_id = entity_num.getInt(4);
                Integer property_id = entity_num.getInt(5);
                String property_value_range = entity_num.getString(6);

                en_list.add((entity1_type_id).toString());
                en_list.add((predicate_id).toString());
                en_list.add((property_id).toString());
                en_list.add(property_value_range);
                if (!en_num_Map.containsKey(en_list)) {
                    en_set.add(entity1_id);
                    en_num_Map.put(en_list, en_set);
                } else {
                    en_set = en_num_Map.get(en_list);
                    en_set.add(entity1_id);
                    en_num_Map.put(en_list, en_set);
                }
            }

            //Label the entity
            ResultSet tags = tags_Stmt.executeQuery();
            while (tags.next()) {
                Integer classes = tags.getInt(1);
                Integer entity_type = tags.getInt(2);
                Integer predicate_id = tags.getInt(3);
                Integer property_id = tags.getInt(4);
                String object = tags.getString(5);
                //Label the entity and the type of the property is string
                if (classes==1){
                    HashSet<Integer> en_string_set = new HashSet<Integer>();
                    ArrayList<Integer> type_pre_pro = new ArrayList<Integer>();
                    int property_value = Integer.parseInt(object);
                    type_pre_pro.add(entity_type);
                    type_pre_pro.add(property_id);
                    type_pre_pro.add(property_value);
                    en_string_set = type_pre_pro_en_Map.get(type_pre_pro);

                    String pro = mapping_map.get(property_id);
                    String c[] = pro.split("/");
                    String str_tags = c[c.length-1]+" == "+mapping_map.get(property_value);

                    for (Integer en:en_string_set){
                        ArrayList<Integer> node_list = new ArrayList<Integer>();
                        ArrayList<String> string_tags_list = new ArrayList<String>();
                        node_list.add(entity_type);
                        node_list.add(en);
                        if (!tags_all_map.containsKey(node_list)){
                            string_tags_list.add(str_tags);
                            tags_all_map.put(node_list,string_tags_list);
                        }
                        else{string_tags_list = tags_all_map.get(node_list);
                            string_tags_list.add("\t"+str_tags);
                            tags_all_map.put(node_list, string_tags_list);
                        }
                    }
                }
                else if (classes==2){
                //Label the entity and the type of the property is numerical
                    flg = 0;
                    HashSet<Integer> en_num_set = new HashSet<Integer>();

                    //Processing value range
                    String g[];
                    String c[] = object.split("\\[",2);
                    String d[] = c[1].split(",",2);
                    if (d[1].contains("]")){
                        flg = 1;
                        g = d[1].split("]",2);
                    }
                    else{
                        g= d[1].split("\\)",2);
                    }
                    value_min = Double.parseDouble(d[0]);
                    value_max = Double.parseDouble(g[0]);

                    //Positive entity
                    en_true_Stmt.setInt(1,entity_type);
                    en_true_Stmt.setInt(2,property_id);
                    ResultSet en_true_id = en_true_Stmt.executeQuery();
                    while(en_true_id.next()){
                        //entity_true：Positive entity
                        Integer entity_true = en_true_id.getInt(1);
                        //value
                        Integer obj_id = en_true_id.getInt(2);
                        value = Double.parseDouble(mapping_map.get(obj_id));
                        if ((flg == 0 && value >= value_min && value < value_max) || (flg == 1 && value >= value_min && value <= value_max)){
                            en_num_set.add(entity_true);
                        }
                    }

                    String pro_num = mapping_map.get(property_id);
                    String a[] = pro_num.split("/");
                    String str_tags = a[a.length-1]+" == "+ object;

                    for (Integer en:en_num_set){
                        ArrayList<Integer> node_list = new ArrayList<Integer>();
                        ArrayList<String> string_tags_list = new ArrayList<String>();
                        node_list.add(entity_type);
                        node_list.add(en);
                        if (!tags_all_map.containsKey(node_list)){
                            string_tags_list.add(str_tags);
                            tags_all_map.put(node_list,string_tags_list);
                        }
                        else{string_tags_list = tags_all_map.get(node_list);
                            string_tags_list.add("\t"+str_tags);
                            tags_all_map.put(node_list, string_tags_list);
                        }
                    }
                }
                else if(classes==3){
                    //Label the entity and the type of label is relation-tags
                    HashSet<Integer> en_relation_set = new HashSet<Integer>();
                    ArrayList<Integer> re_list = new ArrayList<Integer>();

                    Integer entity2_id = Integer.parseInt(object);
                    re_list.add(entity_type);
                    re_list.add(predicate_id);
                    re_list.add(entity2_id);
                    en_relation_set = node_Map.get(re_list);

                    String pre = mapping_map.get(predicate_id);
                    String c[] = pre.split("/");
                    String str_tags = c[c.length-1]+" == "+mapping_map.get(entity2_id);

                    for (Integer en:en_relation_set){
                        ArrayList<Integer> node_list = new ArrayList<Integer>();
                        ArrayList<String> string_tags_list = new ArrayList<String>();
                        node_list.add(entity_type);
                        node_list.add(en);
                        if (!tags_all_map.containsKey(node_list)){
                            string_tags_list.add(str_tags);
                            tags_all_map.put(node_list,string_tags_list);
                        }
                        else{string_tags_list = tags_all_map.get(node_list);
                            string_tags_list.add("\t"+str_tags);
                            tags_all_map.put(node_list, string_tags_list);
                        }
                    }

                }
                else if(classes==4){
                    //Label the entity and the type of label is relation_string_tags
                    HashSet<Integer> en_rel_string_set = new HashSet<Integer>();
                    ArrayList<Integer> re_str_list = new ArrayList<Integer>();

                    Integer property_value = Integer.parseInt(object);

                    re_str_list.add(entity_type);
                    re_str_list.add(predicate_id);
                    re_str_list.add(property_id);
                    re_str_list.add(property_value);
                    en_rel_string_set = en_Map.get(re_str_list);

                    String pre = mapping_map.get(predicate_id);
                    String c[] = pre.split("/");
                    String pro = mapping_map.get(property_id);
                    String d[] = pro.split("/");
                    String str_tags = c[c.length-1]+" --- "+d[d.length-1]+" == "+mapping_map.get(property_value);

                    for (Integer en:en_rel_string_set){
                        ArrayList<Integer> node_list = new ArrayList<Integer>();
                        ArrayList<String> string_tags_list = new ArrayList<String>();
                        node_list.add(entity_type);
                        node_list.add(en);
                        if (!tags_all_map.containsKey(node_list)){
                            string_tags_list.add(str_tags);
                            tags_all_map.put(node_list,string_tags_list);
                        }
                        else{string_tags_list = tags_all_map.get(node_list);
                            string_tags_list.add("\t"+str_tags);
                            tags_all_map.put(node_list, string_tags_list);
                        }
                    }


                }
                else if(classes==5){
                    //Label the entity and the type of label is relation_numerical_tags
                    HashSet<Integer> en_rel_num_set = new HashSet<Integer>();
                    ArrayList<String> re_num_list = new ArrayList<String>();

                    re_num_list.add((entity_type).toString());
                    re_num_list.add((predicate_id).toString());
                    re_num_list.add((property_id).toString());
                    re_num_list.add(object);
                    en_rel_num_set = en_num_Map.get(re_num_list);

                    String pre = mapping_map.get(predicate_id);
                    String c[] = pre.split("/");
                    String pro = mapping_map.get(property_id);
                    String d[] = pro.split("/");
                    String str_tags = c[c.length-1]+" --- "+d[d.length-1]+" == "+ object;

                    for (Integer en:en_rel_num_set){
                        ArrayList<Integer> node_list = new ArrayList<Integer>();
                        ArrayList<String> string_tags_list = new ArrayList<String>();
                        node_list.add(entity_type);
                        node_list.add(en);
                        if (!tags_all_map.containsKey(node_list)){
                            string_tags_list.add(str_tags);
                            tags_all_map.put(node_list,string_tags_list);
                        }
                        else{string_tags_list = tags_all_map.get(node_list);
                            string_tags_list.add("\t"+str_tags);
                            tags_all_map.put(node_list, string_tags_list);
                        }
                    }

                }

            }

            String filePath = "base_data/nodes_tags.txt";
            FileWriter fw = new FileWriter(filePath,true);
            ResultSet entity_all = entity_Stmt.executeQuery();
            while(entity_all.next()) {
                ArrayList<Integer> entity_type = new ArrayList<Integer>();
                StringBuilder result = null;
                int p =0;

                Integer entityid = entity_all.getInt(1);
                Integer type = entity_all.getInt(2);

                entity_type.add(type);
                entity_type.add(entityid);

                String node = mapping_map.get(entityid);
                String  node_type = mapping_map.get(type);
                String d[] = node_type.split("/");
                node_type = d[d.length-1];

                result = new StringBuilder((entityid).toString() + "\t" + node_type + "\t" + node);

                if (tags_all_map.containsKey(entity_type)){
                    p=1;
                    ArrayList<String> list =tags_all_map.get(entity_type);
                    for (int i = 0; i < list.size(); i++) {
                        result.append("\t").append(list.get(i));
                    }
                }
                if (p==1){
                    fw.write(result.toString());
                    fw.write("\n");
                }

            }
            fw.close();
        }
        db.commit();

    }

    public static void main(String[] args) throws Exception{
        Connection db = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);
        db.setAutoCommit(false);
        tags(db);
    }
}
