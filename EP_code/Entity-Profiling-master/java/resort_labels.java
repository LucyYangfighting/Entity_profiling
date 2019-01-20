
import java.sql.*;
import java.util.*;
/**Label reordering*/

//Put tag set information into memory
public class resort_labels {
    private static String sqlitePath = "/base_data/HSP_ALL.sqlite";
    private static void restore(Connection db) throws Exception{
        int count = 0;

        ArrayList<Double> simi = new ArrayList<Double>();
        ArrayList<Double> simi_backup = new ArrayList<Double>();
        ArrayList<Integer> simi_label = new ArrayList<Integer>();
        Map<Integer, ArrayList<String>> tags_map = new HashMap<Integer, ArrayList<String>>();
        try(
            PreparedStatement string_tags_Stmt = db.prepareStatement("SELECT entity_type, property_id, property_value,similarity FROM string_tags; ");
            PreparedStatement num_tags_Stmt = db.prepareStatement("SELECT entity_type, property_id, property_value_range,similarity FROM numerical_tags; ");
            PreparedStatement relations_tags_Stmt = db.prepareStatement("SELECT entity1_type,predicate_id, entity2_id,similarity FROM relation_tags; ");
            PreparedStatement relations_string_tags_Stmt = db.prepareStatement("SELECT entity1_type,predicate_id, property_id, property_value,similarity FROM relation_string_tags; ");
            PreparedStatement relations_num_tags_Stmt = db.prepareStatement("SELECT entity1_type,predicate_id, property_id, property_value_range,similarity FROM relation_numerical_tags; ");

        ) {//Store tag type is string_tags
            ResultSet string_tags = string_tags_Stmt.executeQuery();
            while (string_tags.next()) {
                ArrayList<String> list = new ArrayList<String>();
                Integer entity_type = string_tags.getInt(1);
                Integer property_id = string_tags.getInt(2);
                Integer property_value = string_tags.getInt(3);
                Double similarity = string_tags.getDouble(4);
                list.add("1");
                list.add((entity_type).toString());
                list.add((property_id).toString());
                list.add((property_value).toString());
                tags_map.put(count,list);
                count++;
                simi.add(similarity);
            }
            //Store tag type is numerical_tags
            ResultSet num_tags = num_tags_Stmt.executeQuery();
            while (num_tags.next()) {
                ArrayList<String> list = new ArrayList<String>();
                Integer entity_type = num_tags.getInt(1);
                Integer property_id = num_tags.getInt(2);
                String property_value_range = num_tags.getString(3);
                Double similarity = num_tags.getDouble(4);
                list.add("2");
                list.add((entity_type).toString());
                list.add((property_id).toString());
                list.add(property_value_range);
                tags_map.put(count,list);
                count++;
                simi.add(similarity);
            }
            //Store tag type is relation_tags
            ResultSet relations_tags = relations_tags_Stmt.executeQuery();
            while (relations_tags.next()) {
                ArrayList<String> list = new ArrayList<String>();
                Integer entity_type = relations_tags.getInt(1);
                Integer predicate_id = relations_tags.getInt(2);
                Integer entity2_id = relations_tags.getInt(3);
                Double similarity = relations_tags.getDouble(4);
                list.add("3");
                list.add((entity_type).toString());
                list.add((predicate_id).toString());
                list.add((entity2_id).toString());
                tags_map.put(count,list);
                count++;
                simi.add(similarity);
            }
            //Store tag type is relation_string_tags
            ResultSet relations_string_tags = relations_string_tags_Stmt.executeQuery();
            while (relations_string_tags.next()) {
                ArrayList<String> list = new ArrayList<String>();
                Integer entity_type = relations_string_tags.getInt(1);
                Integer predicate_id = relations_string_tags.getInt(2);
                Integer property_id = relations_string_tags.getInt(3);
                Integer property_value = relations_string_tags.getInt(4);
                Double similarity = relations_string_tags.getDouble(5);
                list.add("4");
                list.add((entity_type).toString());
                list.add((predicate_id).toString());
                list.add((property_id).toString());
                list.add((property_value).toString());
                tags_map.put(count,list);
                count++;
                simi.add(similarity);
            }
            //Store tag type is relation_numerical_tags
            ResultSet relations_num_tags = relations_num_tags_Stmt.executeQuery();
            while (relations_num_tags.next()) {
                ArrayList<String> list = new ArrayList<String>();
                Integer entity_type = relations_num_tags.getInt(1);
                Integer predicate_id = relations_num_tags.getInt(2);
                Integer property_id = relations_num_tags.getInt(3);
                String property_value_range = relations_num_tags.getString(4);
                Double similarity = relations_num_tags.getDouble(5);
                list.add("5");
                list.add((entity_type).toString());
                list.add((predicate_id).toString());
                list.add((property_id).toString());
                list.add(property_value_range);
                tags_map.put(count,list);
                count++;
                simi.add(similarity);
            }
            //Descending
            simi_backup.addAll(simi);
            simi.sort(Collections.reverseOrder());
            HashSet<Double> set = new HashSet<Double>();
            ArrayList<Double> newList = new ArrayList<Double>();
            for (Double sim:simi) {
                if (set.add(sim))
                    newList.add(sim);
            }
            simi.clear();
            simi.addAll(newList);

            for(int i=0; i<simi.size(); i++){
                double res = simi.get(i);
                for(int j=0; j<simi_backup.size();j++){
                    if (simi_backup.get(j)==res){
                        simi_label.add(j);
                    }
                }
            }
            //Tag set reordering
            sort_label(db,tags_map,simi_label,simi_backup);
        }
        db.commit();
    }
    //Tag set reordering
    private static void sort_label(Connection db, Map<Integer, ArrayList<String>> tags_map,ArrayList<Integer> simi_label,ArrayList<Double> simi_backup) throws Exception{
        Map<ArrayList<String>,Double> map = new HashMap<ArrayList<String>,Double>();
        HashSet<Integer> node_set = new HashSet<Integer>();
        Map<ArrayList<Integer>, HashSet<Integer>> string_Map = new HashMap<ArrayList<Integer>, HashSet<Integer>>();
        Map<Integer, String> mapping_map = new HashMap<Integer, String>();
        Map<Integer, Integer> nodes_all_map = new HashMap<Integer, Integer>();
        Map<ArrayList<Integer>, HashSet<Integer>> node_Map = new HashMap<ArrayList<Integer>, HashSet<Integer>>();
        Map<ArrayList<Integer>, HashSet<Integer>> en_Map = new HashMap<ArrayList<Integer>, HashSet<Integer>>();
        Map<ArrayList<String>, HashSet<Integer>> en_num_Map = new HashMap<ArrayList<String>, HashSet<Integer>>();
        ArrayList<Double> result_list = new ArrayList<Double>();

        double similarily = 0.0;
        int node_number = 1; //The total number of types of entities
        double  p = 0.5;
        try (Statement stmt = db.createStatement()) {
           
            stmt.execute("CREATE TABLE tags_sort (classes INT ,types INT,predicate INT ,property INT ,object CHAR ,score DOUBLE,ranking INT);");
        }
        db.commit();
        try(PreparedStatement node_Stmt = db.prepareStatement("SELECT DISTINCT node_id FROM nodes_type ; ");
            PreparedStatement property_Stmt = db.prepareStatement("SELECT DISTINCT entity_id, predicate_id, object_id,entity_type_id FROM property_triples ; ");
            PreparedStatement entity_Stmt = db.prepareStatement("SELECT count(distinct node_id) FROM nodes_type ; ");
            PreparedStatement value_Stmt = db.prepareStatement("SELECT id, content FROM mapping");
            PreparedStatement relation_Stmt = db.prepareStatement("SELECT entity1_id, predicate_id, entity2_id, entity1_type_id FROM salience; ");
            PreparedStatement relation_pro_Stmt = db.prepareStatement("SELECT DISTINCT entity1_type_id, entity1_id,predicate_id,entity2_id, property_id,property_value FROM relation_property_str; ");
            PreparedStatement relation_pro_num_Stmt = db.prepareStatement("SELECT DISTINCT entity1_type, entity1_id,predicate_id,entity2_id, property_id,property_value_range FROM relation_property_num; ");
            PreparedStatement insert_Stmt = db.prepareStatement("INSERT INTO tags_sort (classes,types,predicate,property,object,score,ranking) VALUES (?,?,?,?,?,?,?);");
            PreparedStatement en_true_Stmt = db.prepareStatement("SELECT entity_id,object_id FROM property_triples WHERE entity_type_id=? AND predicate_id=? AND object_type_id!=2 AND object_type_id!=3; ");
        ){
            ResultSet node_all = node_Stmt.executeQuery();
            while(node_all.next()) {
                Integer entityid = node_all.getInt(1);
                nodes_all_map.put(entityid,0);
            }
            ResultSet node = entity_Stmt.executeQuery();
            if (node.next()){
                node_number = node.getInt(1);
            }
            ResultSet mapping = value_Stmt.executeQuery();
            while (mapping.next()) {
                Integer id = mapping.getInt(1);
                String content = mapping.getString(2);
                mapping_map.put(id,content);
            }

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
                if (!string_Map.containsKey(type_pre_pro)) {
                    type_pre_pro_en_set.add(entityid);
                    string_Map.put(type_pre_pro, type_pre_pro_en_set);
                } else {
                    type_pre_pro_en_set = string_Map.get(type_pre_pro);
                    type_pre_pro_en_set.add(entityid);
                    string_Map.put(type_pre_pro, type_pre_pro_en_set);
                }
            }
            ResultSet rel_str = relation_Stmt.executeQuery();
            while(rel_str.next()) {
                HashSet<Integer> rel_node_set = new HashSet<Integer>();
                ArrayList<Integer> node_list = new ArrayList<Integer>();
                Integer en1_id = rel_str.getInt(1);
                Integer pro_id = rel_str.getInt(2);
                Integer en2_id = rel_str.getInt(3);
                Integer en1_type = rel_str.getInt(4);
                node_list.add(en1_type);
                node_list.add(pro_id);
                node_list.add(en2_id);
                if (!node_Map.containsKey(node_list)) {
                    rel_node_set.add(en1_id);
                    node_Map.put(node_list, rel_node_set);
                } else {
                    rel_node_set = node_Map.get(node_list);
                    rel_node_set.add(en1_id);
                    node_Map.put(node_list, rel_node_set);
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


            for (int i = 0; i < simi_label.size(); i++) {
                ArrayList<String> list = new ArrayList<String>();
                ArrayList<Integer> str_list = new ArrayList<Integer>();
                ArrayList<String> res_list = new ArrayList<String>();
                ArrayList<String> rel_num_list = new ArrayList<String>();
                int label = simi_label.get(i);
                list = tags_map.get(label);
                Integer flag = Integer.parseInt(list.get(0));
                res_list.add((flag).toString());
                res_list.add(list.get(1));
                res_list.add(list.get(2));
                res_list.add(list.get(3));

                rel_num_list.add(list.get(1));
                rel_num_list.add(list.get(2));
                rel_num_list.add(list.get(3));

                similarily = simi_backup.get(label);

                str_list.add(Integer.parseInt(list.get(1)));
                str_list.add(Integer.parseInt(list.get(2)));

                if (flag==1){
                    HashSet<Integer> en_set = new HashSet<Integer>();
                    HashSet<Integer> en_set_1 = new HashSet<Integer>();
                    str_list.add(Integer.parseInt(list.get(3)));
                    double reward,penalty;
                    int num_re_node = 0;
                    en_set = string_Map.get(str_list);
                    for (Integer en_node:en_set){
                        nodes_all_map.put(en_node,nodes_all_map.get(en_node)+1);
                        num_re_node = num_re_node + nodes_all_map.get(en_node);
                    }
                    en_set_1.clear();
                    en_set_1.addAll(node_set);
                    en_set_1.retainAll(en_set);
                    if (node_set.size()==0){
                        penalty = 0.0;
                    }
                    else{
                        penalty = (num_re_node*1.0)/(node_number*(i));
                    }
                    node_set.addAll(en_set);
                    reward = node_set.size()*1.0/node_number;
                    double result = similarily + p*reward - (1-p)*penalty;
                    map.put(res_list,result);
                    result_list.add(result);
                }
                else if (flag==2){
                    HashSet<Integer> en_set = new HashSet<Integer>();
                    HashSet<Integer> en_set_1 = new HashSet<Integer>();
                    int flg =0;
                    double value_min,value_max;
                    String range = list.get(3);
                    //Processing value range
                    String g[];
                    String c[] = range.split("\\[",2);
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
                    en_true_Stmt.setInt(1,Integer.parseInt(list.get(1)));
                    en_true_Stmt.setInt(2,Integer.parseInt(list.get(2)));
                    ResultSet en_true_id = en_true_Stmt.executeQuery();
                    while(en_true_id.next()){
                        //entity_true：//Positive entity
                        Integer entity_true = en_true_id.getInt(1);
                        //value
                        Integer obj_id = en_true_id.getInt(2);
                        double value = Double.parseDouble(mapping_map.get(obj_id));
                        if ((flg == 0 && value >= value_min && value < value_max) || (flg == 1 && value >= value_min && value <= value_max)){
                            en_set.add(entity_true);
                        }
                    }
                    double reward,penalty;
                    int num_re_node = 0;
                    for (Integer en_node:en_set){
                        nodes_all_map.put(en_node,nodes_all_map.get(en_node)+1);
                        num_re_node = num_re_node + nodes_all_map.get(en_node);
                    }
                    en_set_1.clear();
                    en_set_1.addAll(node_set);
                    en_set_1.retainAll(en_set);
                    if (node_set.size()==0){
                        penalty = 0.0;
                    }
                    else{
                        penalty = (num_re_node*1.0)/(node_number*(i));
                    }
                    node_set.addAll(en_set);
                    reward = node_set.size()*1.0/node_number;
                    double result = similarily + p*reward - (1-p)*penalty;
                    map.put(res_list,result);
                    result_list.add(result);
                }
                else if (flag==3){
                    HashSet<Integer> en_set = new HashSet<Integer>();
                    HashSet<Integer> en_set_1 = new HashSet<Integer>();
                    str_list.add(Integer.parseInt(list.get(3)));
                    double reward,penalty;
                    en_set = node_Map.get(str_list);
                    int num_re_node = 0;
                    for (Integer en_node:en_set){
                        nodes_all_map.put(en_node,nodes_all_map.get(en_node)+1);
                        num_re_node = num_re_node + nodes_all_map.get(en_node);
                    }
                    en_set_1.clear();
                    en_set_1.addAll(node_set);
                    en_set_1.retainAll(en_set);
                    if (node_set.size()==0){
                        penalty = 0.0;
                    }
                    else{
                        penalty = (num_re_node*1.0)/(node_number*(i));
                    }
                    node_set.addAll(en_set);
                    reward = node_set.size()*1.0/node_number;
                    double result = similarily + p*reward - (1-p)*penalty;
                    map.put(res_list,result);
                    result_list.add(result);
                }
                else if (flag==4){
                    HashSet<Integer> en_set = new HashSet<Integer>();
                    HashSet<Integer> en_set_1 = new HashSet<Integer>();
                    double reward,penalty;
                    str_list.add(Integer.parseInt(list.get(3)));
                    str_list.add(Integer.parseInt(list.get(4)));
                    en_set = en_Map.get(str_list);
                    int num_re_node = 0;
                    for (Integer en_node:en_set){
                        nodes_all_map.put(en_node,nodes_all_map.get(en_node)+1);
                        num_re_node = num_re_node + nodes_all_map.get(en_node);
                    }
                    en_set_1.clear();
                    en_set_1.addAll(node_set);
                    en_set_1.retainAll(en_set);
                    if (node_set.size()==0){
                        penalty = 0.0;
                    }
                    else{
                        penalty = (num_re_node*1.0)/(node_number*(i));
                    }
                    node_set.addAll(en_set);
                    reward = node_set.size()*1.0/node_number;
                    double result = similarily + p*reward - (1-p)*penalty;
                    res_list.add(list.get(4));
                    map.put(res_list,result);
                    result_list.add(result);
                }
                else if (flag==5){
                    HashSet<Integer> en_set = new HashSet<Integer>();
                    HashSet<Integer> en_set_1 = new HashSet<Integer>();
                    double reward,penalty;
                    rel_num_list.add(list.get(4));

                    res_list.add(list.get(4));
                    en_set = en_num_Map.get(rel_num_list);
                    int num_re_node = 0;
                    for (Integer en_node:en_set){
                        nodes_all_map.put(en_node,nodes_all_map.get(en_node)+1);
                        num_re_node = num_re_node + nodes_all_map.get(en_node);
                    }
                    en_set_1.clear();
                    en_set_1.addAll(node_set);
                    en_set_1.retainAll(en_set);
                    if (node_set.size()==0){
                        penalty = 0.0;
                    }
                    else{
                        penalty = (num_re_node*1.0)/(node_number*(i));
                    }
                    node_set.addAll(en_set);
                    reward = node_set.size()*1.0/node_number;
                    double result = similarily + p*reward - (1-p)*penalty;
                    map.put(res_list,result);
                    result_list.add(result);
                }
            }
            result_list.sort(Collections.reverseOrder());
            HashSet<Double> set = new HashSet<Double>();
            ArrayList<Double> newList = new ArrayList<Double>();
            for (Double sim:result_list) {
                if (set.add(sim))
                    newList.add(sim);
            }
            result_list.clear();
            result_list.addAll(newList);
            int score = 1,type=-1,pre=-1,pro=-1;
            String object=null;

            for(int i=0; i<result_list.size(); i++){
                double res = result_list.get(i);
                for (Map.Entry<ArrayList<String>,Double> entry : map.entrySet()) {
                    if (entry.getValue()==res){
                        ArrayList<String> list = entry.getKey();
                        int classes = Integer.parseInt(list.get(0));
                        if (classes==1){
                            type = Integer.parseInt(list.get(1));
                            pre = -1;
                            pro = Integer.parseInt(list.get(2));
                            object = list.get(3);
                        }
                        else if (classes==2){
                            type = Integer.parseInt(list.get(1));
                            pre = -1;
                            pro = Integer.parseInt(list.get(2));
                            object = list.get(3);
                        }
                        else if (classes==3){
                            type = Integer.parseInt(list.get(1));
                            pre = Integer.parseInt(list.get(2));
                            pro = -1;
                            object = list.get(3);
                        }
                        else if (classes==4){
                            type = Integer.parseInt(list.get(1));
                            pre = Integer.parseInt(list.get(2));
                            pro = Integer.parseInt(list.get(3));
                            object = list.get(4);
                        }
                        else if (classes==5){
                            type = Integer.parseInt(list.get(1));
                            pre = Integer.parseInt(list.get(2));
                            pro = Integer.parseInt(list.get(3));
                            object = list.get(4);
                        }
                        insert_Stmt.setInt(1,classes);
                        insert_Stmt.setInt(2,type);
                        insert_Stmt.setInt(3,pre);
                        insert_Stmt.setInt(4,pro);
                        insert_Stmt.setString(5, object);
                        insert_Stmt.setDouble(6, res);
                        insert_Stmt.setInt(7, score);
                        insert_Stmt.execute();
                        score++;
                    }
                }
            }
        }
        db.commit();
    }

    public static void main(String[] args) throws Exception{
        Connection db = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);
        db.setAutoCommit(false);
        restore(db);

    }

}
