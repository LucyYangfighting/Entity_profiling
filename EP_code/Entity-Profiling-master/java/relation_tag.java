import java.sql.*;
import java.util.*;
/**Use the pagerank algorithm to find the center of each entity,
Use the center of the entity to find the relationship label*/

public class relation_tag {
    private static String sqlitePath = "/base_data/HSP_ALL.sqlite";
    //The salience of the entity under each type, and only the salience value is greater than 0.70 and the center degree is ranked in the top 100 as the candidate relationship label.
    private static void make_salience(Connection db) throws Exception{
        
        try (Statement stmt = db.createStatement()) {
            stmt.execute("CREATE TABLE salience (entity1_id INTEGER NOT NULL, predicate_id  INTEGER NOT NULL, " +
                    "entity2_id INTEGER NOT NULL, entity1_type_id  INTEGER NOT NULL, entity2_type_id INTEGER NOT NULL,entity2_salience DOUBLE NOT NULL )");
        }
        try (Statement indexingStmt = db.createStatement()) {
            indexingStmt.execute("CREATE INDEX salience_IND ON salience(entity2_salience)");
        }
        db.commit();
        Map<Integer, Double> vectorMap = new HashMap<Integer, Double>();
        Map<Integer, Double> node_cen_map = new HashMap<Integer, Double>();
        HashSet<Double> node_cen_set = new HashSet<Double>();
        try(PreparedStatement obj_Stmt = db.prepareStatement("SELECT DISTINCT entity2_id,entity2_type_id FROM relations_triples;");
            PreparedStatement rel_Stmt = db.prepareStatement("SELECT entity1_id,predicate_id,entity2_id,entity1_type_id,entity2_type_id FROM relations_triples;");
            PreparedStatement cen_stmt = db.prepareStatement("SELECT ID, centrality FROM entity_centrality");
            PreparedStatement insert_Stmt = db.prepareStatement("INSERT INTO salience (entity1_id,predicate_id,entity2_id,entity1_type_id,entity2_type_id,entity2_salience) VALUES (?,?,?,?,?,?)")
        ){
            //node--centrality
            ResultSet node_centrality = cen_stmt.executeQuery();
            while (node_centrality.next()) {
                Integer id = node_centrality.getInt(1);
                Double content = node_centrality.getDouble(2);
                node_cen_set.add(content);
                node_cen_map.put(id,content);
            }
            //select top 100
            ArrayList<Double> en_list = new ArrayList<Double>(node_cen_set);
            en_list.sort(Collections.reverseOrder());

            //vectorMap:The largest centrality in the storage entity2 type
            ResultSet obj = obj_Stmt.executeQuery();
            while(obj.next()) {
                Integer en2_id = obj.getInt(1);
                Integer en2_type_id = obj.getInt(2);
                double center = node_cen_map.get(en2_id);
                if (!vectorMap.containsKey(en2_type_id)){
                    vectorMap.put(en2_type_id,center);
                }
                else{
                    if (center > vectorMap.get(en2_type_id)){
                        vectorMap.put(en2_type_id,center);
                    }
                }
            }
            ResultSet relation_1 = rel_Stmt.executeQuery();
            while(relation_1.next()) {
                Integer en1_id = relation_1.getInt(1);
                Integer pro_id = relation_1.getInt(2);
                Integer en2_id = relation_1.getInt(3);
                Integer en1_type_id = relation_1.getInt(4);
                Integer en2_type_id = relation_1.getInt(5);
                double center = node_cen_map.get(en2_id);
                //sal=Center degree of entity2/The largest centrality value in an entity of the same type
                double sal_type = center / vectorMap.get(en2_type_id);
                //sal_type>=0.70 as a candidate tag
                if ((sal_type) >= 0.70&&(center>=en_list.get(99))){
                    insert_Stmt.setInt(1,en1_id);
                    insert_Stmt.setInt(2,pro_id);
                    insert_Stmt.setInt(3,en2_id);
                    insert_Stmt.setInt(4,en1_type_id);
                    insert_Stmt.setInt(5,en2_type_id);
                    insert_Stmt.setDouble(6,sal_type);
                    insert_Stmt.execute();
                }
            }
        }
        db.commit();
    }
    //build the <predicate，the string property of the entity2>relation_string_tags
    private static void make_relation_property_str(Connection db) throws Exception{
        try (Statement stmt = db.createStatement()) {
            
            stmt.execute("CREATE TABLE relation_property_str (entity1_type_id INT NOT NULL, entity1_id INT ,predicate_id INT,entity2_id INT ,property_id INT,property_value INT);");
        }
        try (Statement indexingStmt = db.createStatement()) {
            indexingStmt.execute("CREATE INDEX rela_pro_str_pre ON relation_property_str(predicate_id)");
        }
        db.commit();
        Map<ArrayList<Integer>, HashSet<Integer>> type_pre_pro_en_Map = new HashMap<ArrayList<Integer>, HashSet<Integer>>();
        Map<ArrayList<Integer>, ArrayList<Integer>> relation_en1_Map = new HashMap<ArrayList<Integer>, ArrayList<Integer>>();
        Map<ArrayList<Integer>, ArrayList<Integer>> relation_en2_Map = new HashMap<ArrayList<Integer>, ArrayList<Integer>>();
        Map<ArrayList<Integer>, Integer> result_Map = new HashMap<ArrayList<Integer>,Integer>();
        try (
                PreparedStatement entity_Stmt = db.prepareStatement("SELECT DISTINCT entity_type_id, entity_id, predicate_id, object_id FROM property_triples WHERE object_type_id=2 OR object_type_id=3; ");
                PreparedStatement string_tags_stmt = db.prepareStatement("SELECT entity_type, property_id, property_value FROM string_tags ");
                PreparedStatement relation_stmt = db.prepareStatement("SELECT DISTINCT entity1_id, predicate_id,entity2_id,entity1_type_id,entity2_type_id FROM relations_triples");
                PreparedStatement insert_Stmt = db.prepareStatement("INSERT INTO relation_property_str " +
                        "(entity1_type_id,entity1_id,predicate_id,entity2_id, property_id,property_value) VALUES (?,?,?,?,?,?);")
        ) {
            //Store database information in memory (speed up)
            //[entity_type_id,predicate_id,object_id]--[entity_id]
            ResultSet entity = entity_Stmt.executeQuery();
            while(entity.next()) {
                HashSet<Integer> type_pre_pro_en_set = new HashSet<Integer>();
                ArrayList<Integer> type_pre_pro = new ArrayList<Integer>();
                Integer entity_typeid = entity.getInt(1);
                Integer entityid = entity.getInt(2);
                Integer predicateid = entity.getInt(3);
                Integer objectid = entity.getInt(4);

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
            //[relations]
            ResultSet relation = relation_stmt.executeQuery();
            while(relation.next()) {
                ArrayList<Integer> en1_key = new ArrayList<Integer>();
                ArrayList<Integer> en1_value = new ArrayList<Integer>();

                Integer entity1_id = relation.getInt(1);
                Integer predicate_id = relation.getInt(2);
                Integer entity2_id = relation.getInt(3);
                Integer entity1_type_id = relation.getInt(4);
                Integer entity2_type_id = relation.getInt(5);

                en1_key.add(entity1_id);
                en1_key.add(entity1_type_id);
                if (!relation_en1_Map.containsKey(en1_key)) {
                    en1_value.add(predicate_id);
                    en1_value.add(entity2_id);
                    en1_value.add(entity2_type_id);
                    relation_en1_Map.put(en1_key, en1_value);
                } else {
                    en1_value = relation_en1_Map.get(en1_key);
                    en1_value.add(predicate_id);
                    en1_value.add(entity2_id);
                    en1_value.add(entity2_type_id);
                    relation_en1_Map.put(en1_key, en1_value);
                }

                ArrayList<Integer> en2_key = new ArrayList<Integer>();
                ArrayList<Integer> en2_value = new ArrayList<Integer>();
                en2_key.add(entity2_id);
                en2_key.add(entity2_type_id);
                if (!relation_en2_Map.containsKey(en2_key)) {
                    en2_value.add(predicate_id);
                    en2_value.add(entity1_id);
                    en2_value.add(entity1_type_id);
                    relation_en2_Map.put(en2_key, en2_value);
                } else {
                    en2_value = relation_en2_Map.get(en2_key);
                    en2_value.add(predicate_id);
                    en2_value.add(entity1_id);
                    en2_value.add(entity1_type_id);
                    relation_en2_Map.put(en2_key, en2_value);
                }

            }

            ResultSet string_tags = string_tags_stmt.executeQuery();
            while (string_tags.next()) {
                ArrayList<Integer> type_pre_pro_en_1 = new ArrayList<Integer>();
                HashSet<Integer> type_pre_pro_en_set_1 = new HashSet<Integer>();

                Integer  entity_type = string_tags.getInt(1);
                Integer property_id = string_tags.getInt(2);
                Integer property_value = string_tags.getInt(3);
                type_pre_pro_en_1.add(entity_type);
                type_pre_pro_en_1.add(property_id);
                type_pre_pro_en_1.add(property_value);
                type_pre_pro_en_set_1 = type_pre_pro_en_Map.get(type_pre_pro_en_1);
                for (Integer en:type_pre_pro_en_set_1){
                    //Use entity as the subject to get the corresponding object
                    result_Map.clear();
                    ArrayList<Integer> en_1_key = new ArrayList<Integer>();
                    ArrayList<Integer> en_1_value = new ArrayList<Integer>();
                    en_1_key.add(en);
                    en_1_key.add(entity_type);
                    if (relation_en1_Map.containsKey(en_1_key)){
                        en_1_value = relation_en1_Map.get(en_1_key);
                        for (int i = 0; i < en_1_value.size()-2; i+=3) {
                            ArrayList<Integer> result_list = new ArrayList<Integer>();
                            Integer pre_id = en_1_value.get(i);
                            Integer en2_id = en_1_value.get(i+1);
                            Integer en2_type_id = en_1_value.get(i+2);
                            result_list.add(en2_id);
                            result_list.add(pre_id);
                            result_list.add(en2_type_id);
                            if (!result_Map.containsKey(result_list)) {
                                result_Map.put(result_list, 1);
                                insert_Stmt.setInt(1, en2_type_id);
                                insert_Stmt.setInt(2, en2_id);
                                insert_Stmt.setInt(3, pre_id);
                                insert_Stmt.setInt(4, en);
                                insert_Stmt.setInt(5, property_id);
                                insert_Stmt.setInt(6, property_value);
                                insert_Stmt.execute();
                            }
                        }
                    }

                    //Use entity as the object to get the corresponding subject
                    ArrayList<Integer> en_2_key = new ArrayList<Integer>();
                    ArrayList<Integer> en_2_value = new ArrayList<Integer>();
                    en_2_key.add(en);
                    en_2_key.add(entity_type);
                    if (relation_en2_Map.containsKey(en_2_key)){
                        en_2_value = relation_en2_Map.get(en_2_key);
                        for (int i = 0; i < en_2_value.size()-2; i+=3) {
                            ArrayList<Integer> result_list_1 = new ArrayList<Integer>();
                            Integer pre_id_1 = en_2_value.get(i);
                            Integer en1_id = en_2_value.get(i+1);
                            Integer en1_type_id = en_2_value.get(i+2);
                            result_list_1.add(en1_id);
                            result_list_1.add(pre_id_1);
                            result_list_1.add(en1_type_id);
                            if (!result_Map.containsKey(result_list_1)) {
                                result_Map.put(result_list_1, 1);
                                insert_Stmt.setInt(1, en1_type_id);
                                insert_Stmt.setInt(2, en1_id);
                                insert_Stmt.setInt(3, pre_id_1);
                                insert_Stmt.setInt(4, en);
                                insert_Stmt.setInt(5, property_id);
                                insert_Stmt.setInt(6, property_value);
                                insert_Stmt.execute();
                            }
                        }
                    }

                }
            }
        }
        db.commit();
    }
    //build the <predicate，the numerical property of the entity2>relation_numercial_tags
    private static void make_relation_property_num(Connection db) throws Exception{
        try (Statement stmt = db.createStatement()) {
            
            stmt.execute("CREATE TABLE relation_property_num (entity1_type INT NOT NULL,entity1_id INT ,predicate_id INT,entity2_id INT ,property_id INT,property_value_range CHAR );");
        }
        db.commit();
        Map<Integer, Double> mappingMap = new HashMap<Integer, Double>();
        Map<ArrayList<String>, Integer> result_Map = new HashMap<ArrayList<String>,Integer>();
        Map<ArrayList<Integer>, ArrayList<Integer>> relation_en1_Map = new HashMap<ArrayList<Integer>, ArrayList<Integer>>();
        Map<ArrayList<Integer>, ArrayList<Integer>> relation_en2_Map = new HashMap<ArrayList<Integer>, ArrayList<Integer>>();
        double value_min,value_max,value;
        int flg; //Judging whether it is closed("]") or open(")")
        try (   PreparedStatement num_tags_stmt = db.prepareStatement("SELECT entity_type, property_id, property_value_range FROM numerical_tags");
                PreparedStatement pro_stmt = db.prepareStatement("SELECT DISTINCT entity_id,object_id FROM property_triples WHERE entity_type_id=? AND predicate_id=? AND object_type_id!=2 AND object_type_id!=3;");
                PreparedStatement relation_stmt = db.prepareStatement("SELECT DISTINCT entity1_id, predicate_id,entity2_id,entity1_type_id,entity2_type_id FROM relations_triples");
                PreparedStatement value_Stmt = db.prepareStatement("SELECT id, content FROM mapping WHERE (string_type_id=4 OR string_type_id=5 OR string_type_id=6)");
                PreparedStatement Stmt = db.prepareStatement("INSERT INTO relation_property_num " +
                        "(entity1_type,entity1_id,predicate_id,entity2_id,property_id,property_value_range) VALUES (?,?,?,?,?,?);");
        ) {
            ResultSet mapping = value_Stmt.executeQuery();
            while (mapping.next()) {
                Integer id = mapping.getInt(1);
                Double content = mapping.getDouble(2);
                mappingMap.put(id,content);
            }
            //[relations]
            ResultSet relation = relation_stmt.executeQuery();
            while(relation.next()) {
                ArrayList<Integer> en1_key = new ArrayList<Integer>();
                ArrayList<Integer> en1_value = new ArrayList<Integer>();

                Integer entity1_id = relation.getInt(1);
                Integer predicate_id = relation.getInt(2);
                Integer entity2_id = relation.getInt(3);
                Integer entity1_type_id = relation.getInt(4);
                Integer entity2_type_id = relation.getInt(5);

                en1_key.add(entity1_id);
                en1_key.add(entity1_type_id);
                if (!relation_en1_Map.containsKey(en1_key)) {
                    en1_value.add(predicate_id);
                    en1_value.add(entity2_id);
                    en1_value.add(entity2_type_id);
                    relation_en1_Map.put(en1_key, en1_value);
                } else {
                    en1_value = relation_en1_Map.get(en1_key);
                    en1_value.add(predicate_id);
                    en1_value.add(entity2_id);
                    en1_value.add(entity2_type_id);
                    relation_en1_Map.put(en1_key, en1_value);
                }

                ArrayList<Integer> en2_key = new ArrayList<Integer>();
                ArrayList<Integer> en2_value = new ArrayList<Integer>();
                en2_key.add(entity2_id);
                en2_key.add(entity2_type_id);
                if (!relation_en2_Map.containsKey(en2_key)) {
                    en2_value.add(predicate_id);
                    en2_value.add(entity1_id);
                    en2_value.add(entity1_type_id);
                    relation_en2_Map.put(en2_key, en2_value);
                } else {
                    en2_value = relation_en2_Map.get(en2_key);
                    en2_value.add(predicate_id);
                    en2_value.add(entity1_id);
                    en2_value.add(entity1_type_id);
                    relation_en2_Map.put(en2_key, en2_value);
                }

            }

            ResultSet num_tags = num_tags_stmt.executeQuery();
            while (num_tags.next()) {
                HashSet<Integer> type_pre_pro_en_set_1 = new HashSet<Integer>();
                Integer entity_type = num_tags.getInt(1);
                Integer property_id = num_tags.getInt(2);
                String value_range = num_tags.getString(3);
                flg = 0;
                //Processing value range
                String g[];
                String c[] = value_range.split("\\[",2);
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

                pro_stmt.setInt(1,entity_type);
                pro_stmt.setInt(2,property_id);
                ResultSet pro = pro_stmt.executeQuery();
                while(pro.next()) {
                    Integer en_id = pro.getInt(1);
                    Integer obj_id = pro.getInt(2);
                    value = mappingMap.get(obj_id);
                    if ((flg == 0 && value >= value_min && value < value_max) || (flg == 1 && value >= value_min && value <= value_max)){
                        type_pre_pro_en_set_1.add(en_id);
                    }
                }
                for (Integer en:type_pre_pro_en_set_1){
                    //Use entity as the subject to get the corresponding object
                    ArrayList<Integer> en_1_key = new ArrayList<Integer>();
                    ArrayList<Integer> en_1_value = new ArrayList<Integer>();
                    en_1_key.add(en);
                    en_1_key.add(entity_type);
                    if (relation_en1_Map.containsKey(en_1_key)){
                        en_1_value = relation_en1_Map.get(en_1_key);
                        for (int i = 0; i < en_1_value.size()-2; i+=3) {
                            ArrayList<String> result_list = new ArrayList<String>();
                            Integer pre_id = en_1_value.get(i);
                            Integer en2_id = en_1_value.get(i + 1);
                            Integer en2_type_id = en_1_value.get(i + 2);
                            result_list.add((en2_type_id).toString());
                            result_list.add((en2_id).toString());
                            result_list.add((pre_id).toString());
                            result_list.add((en).toString());
                            result_list.add((property_id).toString());
                            result_list.add(value_range);
                            if (!result_Map.containsKey(result_list)){
                                result_Map.put(result_list,1);
                                Stmt.setInt(1, en2_type_id);
                                Stmt.setInt(2, en2_id);
                                Stmt.setInt(3, pre_id);
                                Stmt.setInt(4, en);
                                Stmt.setInt(5, property_id);
                                Stmt.setString(6, value_range);
                                Stmt.execute();
                            }
                        }
                    }
                    //Use entity as the object to get the corresponding subject
                    ArrayList<Integer> en_2_key = new ArrayList<Integer>();
                    ArrayList<Integer> en_2_value = new ArrayList<Integer>();
                    en_2_key.add(en);
                    en_2_key.add(entity_type);
                    if (relation_en2_Map.containsKey(en_2_key)) {
                        en_2_value = relation_en2_Map.get(en_2_key);
                        for (int i = 0; i < en_2_value.size() - 2; i += 3) {
                            ArrayList<String> result_list_1 = new ArrayList<String>();
                            Integer pre_id_1 = en_2_value.get(i);
                            Integer en1_id = en_2_value.get(i + 1);
                            Integer en1_type_id = en_2_value.get(i + 2);
                            result_list_1.add((en1_type_id).toString());
                            result_list_1.add((en1_id).toString());
                            result_list_1.add((pre_id_1).toString());
                            result_list_1.add((en).toString());
                            result_list_1.add((property_id).toString());
                            result_list_1.add(value_range);
                            if (!result_Map.containsKey(result_list_1)){
                                result_Map.put(result_list_1,1);
                                Stmt.setInt(1, en1_type_id);
                                Stmt.setInt(2, en1_id);
                                Stmt.setInt(3, pre_id_1);
                                Stmt.setInt(4, en);
                                Stmt.setInt(5, property_id);
                                Stmt.setString(6, value_range);
                                Stmt.execute();
                            }
                        }
                    }
                }
            }
        }
        db.commit();
    }

    public static void main(String[] args) throws Exception{
        Connection db = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);
        db.setAutoCommit(false);
        make_salience(db);
        make_relation_property_str(db);
        make_relation_property_num(db);
    }
}
