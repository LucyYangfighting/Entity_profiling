
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/*Create a base table:property_triples_table,property_mid_support,relations_triples*/


public class creat_base_table {

    private static String sqlitePath = "/base_data/HSP_ALL.sqlite";
    //statistics Attribute triple:Subject and object must have type
    private static void build_property_triples_table(Connection db) throws Exception{
        try(Statement stmt = db.createStatement()){
            //build the property_triples table: Attribute triple
            stmt.execute("CREATE TABLE property_triples (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,entity_id INTEGER NOT NULL, predicate_id  INTEGER NOT NULL, object_id INTEGER NOT NULL, entity_type_id  INTEGER NOT NULL, object_type_id INTEGER NOT NULL)");
        }

        try(
                //the onject is not uri
                PreparedStatement property_Triples_Stmt = db.prepareStatement("SELECT subject_id, predicate_id, object_id FROM triples_all WHERE  (SELECT string_type_id FROM mapping WHERE id=triples_all.object_id )!=1;");
                PreparedStatement subject_type_Stmt = db.prepareStatement("SELECT type_id FROM nodes_type WHERE  node_id=?;");
                PreparedStatement object_type_Stmt = db.prepareStatement("SELECT string_type_id FROM mapping WHERE  id=?;");
                PreparedStatement stmt = db.prepareStatement("INSERT INTO property_triples (entity_id,predicate_id,object_id,entity_type_id,object_type_id) VALUES (?,?,?,?,?)"))
        {
            ResultSet triples = property_Triples_Stmt.executeQuery();
            while(triples.next()){
                Integer subjectID = triples.getInt(1);
                Integer predicateID = triples.getInt(2);
                Integer objectID = triples.getInt(3);

                //the type of the subject
                int sub_type_id;
                subject_type_Stmt.setInt(1, subjectID);
                ResultSet sub_type = subject_type_Stmt.executeQuery();
                if (!sub_type.next()){
                    //the subject has no type
                    continue;
                }
                else{
                    sub_type_id = sub_type.getInt(1);
                }

                //the type of the object
                object_type_Stmt.setInt(1, objectID);
                ResultSet object_type = object_type_Stmt.executeQuery();
                int obj_type_id = object_type.getInt(1);

                stmt.setInt(1,subjectID);
                stmt.setInt(2,predicateID);
                stmt.setInt(3,objectID);
                stmt.setInt(4,sub_type_id);
                stmt.setInt(5,obj_type_id);
                stmt.execute();
            }
        }
        db.commit();

        try (
                Statement indexingStmt = db.createStatement();
        ) {
            indexingStmt.execute("CREATE INDEX subject_property_index ON property_triples(entity_id)");
            indexingStmt.execute("CREATE INDEX object_property_index ON property_triples(object_id)");
            indexingStmt.execute("CREATE INDEX entity_type_index ON property_triples(entity_type_id)");
            indexingStmt.execute("CREATE INDEX pre_property_index ON property_triples(predicate_id)");
        }
        db.commit();
    }
    //Summary table of attribute information,Convenient for later inquiry
    private static void support_property_table(Connection db) throws Exception {
        Map<Integer, HashSet<Integer>> type_entity_Map = new HashMap<Integer, HashSet<Integer>>();
        Map<Integer, HashSet<Integer>> type_pre_Map = new HashMap<Integer, HashSet<Integer>>();
        Map<ArrayList<Integer>, HashSet<Integer>> type_pre_entity_Map = new HashMap<ArrayList<Integer>, HashSet<Integer>>();
        Map<ArrayList<Integer>, HashSet<Integer>> type_pre_pro_Map = new HashMap<ArrayList<Integer>, HashSet<Integer>>();
        Map<ArrayList<Integer>, HashSet<Integer>> type_pre_pro_en_Map = new HashMap<ArrayList<Integer>, HashSet<Integer>>();

        try(Statement stmt = db.createStatement()){
            //build property_mid_support table: 
            /**
             * type_id
             * entity_num
             * property_num
             * property_id
             * property_en_num
             * property_value_num
             * property_value
             * num_property_value
             * support_property
             * support_property_value*/
            stmt.execute("CREATE TABLE property_mid_support (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT," +
                    "type_id INTEGER NOT NULL, entity_num INTEGER NOT NULL, property_num INTEGER NOT NULL, " +
                    "property_id  INTEGER NOT NULL, property_en_num INTEGER NOT NULL, property_value_num INTEGER NOT NULL," +
                    "property_value  INTEGER NOT NULL, num_property_value INTEGER NOT NULL,support_property INTEGER NOT NULL," +
                    "support_property_value INTEGER NOT NULL)");
        }
        db.commit();
        try (PreparedStatement Stmt = db.prepareStatement("SELECT DISTINCT entity_id, predicate_id, object_id,entity_type_id FROM property_triples;");
                
                PreparedStatement insert_Stmt = db.prepareStatement("INSERT INTO property_mid_support (type_id,entity_num,property_num,property_id,property_en_num,property_value_num,property_value,num_property_value,support_property,support_property_value) VALUES (?,?,?,?,?,?,?,?,?,?)")) {
            ResultSet pro_triples = Stmt.executeQuery();
            while (pro_triples.next()) {
                // the all information of property_triples table
                Integer entity_typeID = pro_triples.getInt(4);
                Integer entityID = pro_triples.getInt(1);
                Integer predicateID = pro_triples.getInt(2);
                Integer objectID = pro_triples.getInt(3);

                HashSet<Integer> type_entity_set = new HashSet<Integer>();
                HashSet<Integer> type_pre_set = new HashSet<Integer>();
                HashSet<Integer> type_pre_en_set = new HashSet<Integer>();

                HashSet<Integer> type_pre_pro_set = new HashSet<Integer>();
                HashSet<Integer> type_pre_pro_en_set = new HashSet<Integer>();
                ArrayList<Integer> type_pre = new ArrayList<Integer>();
                ArrayList<Integer> type_pre_pro = new ArrayList<Integer>();
                //Store database information in memory (speed up)
                //Map store[type--subject]
                if (!type_entity_Map.containsKey(entity_typeID)) {
                    type_entity_set.add(entityID);
                    type_entity_Map.put(entity_typeID, type_entity_set);
                } else {
                    type_entity_set = type_entity_Map.get(entity_typeID);
                    type_entity_set.add(entityID);
                    type_entity_Map.put(entity_typeID, type_entity_set);
                }
                //Map store[type--predicate]
                if (!type_pre_Map.containsKey(entity_typeID)) {
                    type_pre_set.add(predicateID);
                    type_pre_Map.put(entity_typeID, type_pre_set);
                } else {
                    type_pre_set = type_pre_Map.get(entity_typeID);
                    type_pre_set.add(predicateID);
                    type_pre_Map.put(entity_typeID, type_pre_set);
                }
                //Map store[type_predicate--subject]
                type_pre.add(entity_typeID);
                type_pre.add(predicateID);
                if (!type_pre_entity_Map.containsKey(type_pre)) {
                    type_pre_en_set.add(entityID);
                    type_pre_entity_Map.put(type_pre, type_pre_en_set);
                } else {
                    type_pre_en_set = type_pre_entity_Map.get(type_pre);
                    type_pre_en_set.add(entityID);
                    type_pre_entity_Map.put(type_pre, type_pre_en_set);
                }
                //Map store[type_predicate--object]
                if (!type_pre_pro_Map.containsKey(type_pre)) {
                    type_pre_pro_set.add(objectID);
                    type_pre_pro_Map.put(type_pre, type_pre_pro_set);
                } else {
                    type_pre_pro_set = type_pre_pro_Map.get(type_pre);
                    type_pre_pro_set.add(objectID);
                    type_pre_pro_Map.put(type_pre, type_pre_pro_set);
                }
                //Map store[type_predicate_object--subject]
                type_pre_pro.add(entity_typeID);
                type_pre_pro.add(predicateID);
                type_pre_pro.add(objectID);
                if (!type_pre_pro_en_Map.containsKey(type_pre_pro)) {
                    type_pre_pro_en_set.add(entityID);
                    type_pre_pro_en_Map.put(type_pre_pro, type_pre_pro_en_set);
                } else {
                    type_pre_pro_en_set = type_pre_pro_en_Map.get(type_pre_pro);
                    type_pre_pro_en_set.add(entityID);
                    type_pre_pro_en_Map.put(type_pre_pro, type_pre_pro_en_set);
                }
            }

            int type, en_num, pro_num, pro_id, pro_en_num, pro_obj_num, pro_value_id, num_pro_value;

            for (Map.Entry<Integer, HashSet<Integer>> entry : type_entity_Map.entrySet()) {
                HashSet<Integer> type_pre_set_1 = new HashSet<Integer>();
                type = entry.getKey(); //statistic type
                //Count the number of entities about this type
                en_num = entry.getValue().size();
                //Count the number of attributes about this type
                type_pre_set_1 = type_pre_Map.get(type);
                pro_num = type_pre_set_1.size();
                for (Integer pro : type_pre_set_1) {
                    HashSet<Integer> type_pre_pro_set_1 = new HashSet<Integer>();
                    ArrayList<Integer> type_pre_1 = new ArrayList<Integer>();
                    //Count all attributes about this type
                    pro_id = pro;
                    //Count the number of entities for each attribute about this type
                    type_pre_1.add(type);
                    type_pre_1.add(pro_id);
                    pro_en_num = type_pre_entity_Map.get(type_pre_1).size();
                    //Count how many different values each attribute has about this type
                    type_pre_pro_set_1 = type_pre_pro_Map.get(type_pre_1);
                    pro_obj_num = type_pre_pro_set_1.size();
                    for (Integer obj : type_pre_pro_set_1) {
                        ArrayList<Integer> type_pre_pro_1 = new ArrayList<Integer>();
                        //Count the values of each attribute about this type.
                        pro_value_id = obj;
                        //count the number of different values for each attribute about this type
                        type_pre_pro_1.add(type);
                        type_pre_pro_1.add(pro_id);
                        type_pre_pro_1.add(pro_value_id);
                        num_pro_value = type_pre_pro_en_Map.get(type_pre_pro_1).size();

                        float support_pro = ((float) (pro_en_num) / (float) (en_num));
                        float support_pro_value = ((float) (num_pro_value) / (float) (en_num));
                        //store the result in the property_mide_support table
                        insert_Stmt.setInt(1,type);
                        insert_Stmt.setInt(2,en_num);
                        insert_Stmt.setInt(3,pro_num);
                        insert_Stmt.setInt(4,pro_id);
                        insert_Stmt.setInt(5,pro_en_num);
                        insert_Stmt.setInt(6,pro_obj_num);
                        insert_Stmt.setInt(7,pro_value_id);
                        insert_Stmt.setInt(8,num_pro_value);
                        insert_Stmt.setFloat(9,support_pro);
                        insert_Stmt.setFloat(10,support_pro_value);
                        insert_Stmt.execute();
                    }
                }
            }
        }
        db.commit();
        try (Statement indexingStmt = db.createStatement()) {
            indexingStmt.execute("CREATE INDEX type_id_sup_index ON property_mid_support(type_id)");
            indexingStmt.execute("CREATE INDEX property_id_sup_index ON property_mid_support(property_id)");
            indexingStmt.execute("CREATE INDEX property_value_sup_index ON property_mid_support(property_value)");
            indexingStmt.execute("CREATE INDEX support_property_value_index ON property_mid_support(support_property_value)");
        }
        db.commit();
    }
    //statistics relation triple:Subject and object must have type
    private static void build_relation_triples_table(Connection db) throws Exception{
        try(Statement stmt = db.createStatement()){
            //build relation_triples table: relation triple
            stmt.execute("CREATE TABLE relations_triples (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,entity1_id INTEGER NOT NULL, predicate_id  INTEGER NOT NULL, entity2_id INTEGER NOT NULL, entity1_type_id  INTEGER NOT NULL, entity2_type_id INTEGER NOT NULL)");
        }

        try(
                //select triple in triples_all table: subject,predicate and object are uri，predicate is not rdf/owl/rdfs，predicate is not "type"
                PreparedStatement relation_Triples_Stmt = db.prepareStatement("SELECT subject_id, predicate_id, object_id FROM triples_all " +
                        "WHERE (predicate_id NOT IN (SELECT id FROM mapping WHERE content like '%rdf%' OR content like '%owl%' " +
                        "OR content = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'))");
                PreparedStatement type_Stmt = db.prepareStatement("SELECT type_id FROM nodes_type WHERE  node_id=?;");
                PreparedStatement insert_Stmt = db.prepareStatement("INSERT INTO relations_triples (entity1_id,predicate_id,entity2_id,entity1_type_id,entity2_type_id) VALUES (?,?,?,?,?)"))
        {
            ResultSet triple = relation_Triples_Stmt.executeQuery();
            while(triple.next()){
                Integer subjectID = triple.getInt(1);
                Integer predicateID = triple.getInt(2);
                Integer objectID = triple.getInt(3);
                
                //the type of subject
                int sub_type_id;
                type_Stmt.setInt(1, subjectID);
                ResultSet sub_type = type_Stmt.executeQuery();
                if (!sub_type.next()){
                    continue;
                }
                else{
                    sub_type_id = sub_type.getInt(1);
                }

                //the type of object
                int obj_type_id;
                type_Stmt.setInt(1, objectID);
                ResultSet object_type = type_Stmt.executeQuery();
                if (!object_type.next()){
                    continue;
                }
                else{
                    obj_type_id = object_type.getInt(1);
                }

                insert_Stmt.setInt(1,subjectID);
                insert_Stmt.setInt(2,predicateID);
                insert_Stmt.setInt(3,objectID);
                insert_Stmt.setInt(4,sub_type_id);
                insert_Stmt.setInt(5,obj_type_id);
                insert_Stmt.execute();
            }
        }
        db.commit();

        try (
                Statement indexingStmt = db.createStatement();
        ) {
            indexingStmt.execute("CREATE INDEX entity1_relations_index ON relations_triples(entity1_id)");
            indexingStmt.execute("CREATE INDEX entity1_type_relations_index ON relations_triples(entity1_type_id)");
            indexingStmt.execute("CREATE INDEX entity2_relation_index ON relations_triples(entity2_id)");
            indexingStmt.execute("CREATE INDEX predicate_relation_index ON relations_triples(predicate_id)");

        }
        db.commit();
    }

    public static void main(String[] args) throws Exception {
        Connection db = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);
        db.setAutoCommit(false);
        build_property_triples_table(db);
        support_property_table(db);
        build_relation_triples_table(db);

    }
}

