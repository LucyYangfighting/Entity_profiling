import java.sql.*;
import java.util.*;
/**Convert abstract tags from the SQLite into database real information*/
public class make_tag_value {
    private static String sqlitePath = "/base_data/HSP_ALL.sqlite";
    private static void string(Connection db) throws Exception{
        Map<Integer, String> mapping_map = new HashMap<Integer, String>();
        try (Statement stmt = db.createStatement()) {
            //Label the entity and the type of the property is string_tags
            stmt.execute("CREATE TABLE tag_string (entity_type CHAR,property CHAR,property_value CHAR,similarity DOUBLE );");
        }
        try (PreparedStatement str_stmt = db.prepareStatement("SELECT DISTINCT entity_type,property_id,property_value,similarity FROM string_tags; ");
             PreparedStatement value_Stmt = db.prepareStatement("SELECT id, content FROM mapping");
             PreparedStatement insert_Stmt = db.prepareStatement("INSERT INTO tag_string (entity_type,property,property_value,similarity) VALUES (?,?,?,?);"))
        {
            ResultSet mapping = value_Stmt.executeQuery();
            while (mapping.next()) {
                Integer id = mapping.getInt(1);
                String content = mapping.getString(2);
                mapping_map.put(id,content);
            }
            ResultSet str_pro = str_stmt.executeQuery();
            while (str_pro.next()) {
                Integer entity_type = str_pro.getInt(1);
                Integer property_id = str_pro.getInt(2);
                Integer property_value = str_pro.getInt(3);
                Double support = str_pro.getDouble(4);

                String type = mapping_map.get(entity_type);
                String pro = mapping_map.get(property_id);
                String pro_value = mapping_map.get(property_value);
                insert_Stmt.setString(1, type);
                insert_Stmt.setString(2, pro);
                insert_Stmt.setString(3, pro_value);
                insert_Stmt.setDouble(4, support);
                insert_Stmt.execute();
            }
        }
        db.commit();

    }
    private static void numerical(Connection db) throws Exception{
        Map<Integer, String> mapping_map = new HashMap<Integer, String>();
        try (Statement stmt = db.createStatement()) {
            //build the numerical_tags table: numerical attribute tag set
            stmt.execute("CREATE TABLE tag_numerical (entity_type CHAR,property CHAR,property_value_range CHAR,similarity DOUBLE );");
        }
        try (PreparedStatement str_stmt = db.prepareStatement("SELECT DISTINCT entity_type,property_id,property_value_range,similarity FROM numerical_tags; ");
             PreparedStatement value_Stmt = db.prepareStatement("SELECT id, content FROM mapping");
             PreparedStatement insert_Stmt = db.prepareStatement("INSERT INTO tag_numerical (entity_type,property,property_value_range,similarity) VALUES (?,?,?,?);"))
        {
            ResultSet mapping = value_Stmt.executeQuery();
            while (mapping.next()) {
                Integer id = mapping.getInt(1);
                String content = mapping.getString(2);
                mapping_map.put(id,content);
            }
            ResultSet str_pro = str_stmt.executeQuery();
            while (str_pro.next()) {
                Integer entity_type = str_pro.getInt(1);
                Integer property_id = str_pro.getInt(2);
                String property_value_range = str_pro.getString(3);
                Double support = str_pro.getDouble(4);

                String type = mapping_map.get(entity_type);
                String pro = mapping_map.get(property_id);
                insert_Stmt.setString(1, type);
                insert_Stmt.setString(2, pro);
                insert_Stmt.setString(3, property_value_range);
                insert_Stmt.setDouble(4, support);
                insert_Stmt.execute();
            }
        }
        db.commit();

    }
    private static void relation(Connection db) throws Exception{
        Map<Integer, String> mapping_map = new HashMap<Integer, String>();
        try (Statement stmt = db.createStatement()) {
            //Label the entity and the type of label is relation-tags
            stmt.execute("CREATE TABLE tag_relation (entity_type CHAR,predicate_id CHAR,object CHAR,similarity DOUBLE );");
        }
        try (PreparedStatement str_stmt = db.prepareStatement("SELECT DISTINCT entity1_type,predicate_id,entity2_id,similarity FROM relation_tags ; ");
             PreparedStatement value_Stmt = db.prepareStatement("SELECT id, content FROM mapping");
             PreparedStatement insert_Stmt = db.prepareStatement("INSERT INTO tag_relation (entity_type,predicate_id,object,similarity) VALUES (?,?,?,?);"))
        {
            ResultSet mapping = value_Stmt.executeQuery();
            while (mapping.next()) {
                Integer id = mapping.getInt(1);
                String content = mapping.getString(2);
                mapping_map.put(id,content);
            }
            ResultSet str_pro = str_stmt.executeQuery();
            while (str_pro.next()) {
                Integer entity_type = str_pro.getInt(1);
                Integer predicate_id = str_pro.getInt(2);
                Integer entity2_id = str_pro.getInt(3);
                Double salience = str_pro.getDouble(4);

                String type = mapping_map.get(entity_type);
                String pro = mapping_map.get(predicate_id);
                String object = mapping_map.get(entity2_id);
                insert_Stmt.setString(1, type);
                insert_Stmt.setString(2, pro);
                insert_Stmt.setString(3, object);
                insert_Stmt.setDouble(4, salience);
                insert_Stmt.execute();
            }
        }
        db.commit();
    }
    private static void relation_property_string(Connection db) throws Exception{
        Map<Integer, String> mapping_map = new HashMap<Integer, String>();
        try (Statement stmt = db.createStatement()) {
            //Label the entity and the type of label is relation_string_tags
            stmt.execute("CREATE TABLE tag_relation_property_string (entity_type CHAR ,predicate CHAR,property CHAR,property_value CHAR,similarity DOUBLE );");
        }
        try (PreparedStatement str_stmt = db.prepareStatement("SELECT DISTINCT entity1_type,predicate_id,property_id,property_value,similarity FROM relation_string_tags; ");
             PreparedStatement value_Stmt = db.prepareStatement("SELECT id, content FROM mapping");
             PreparedStatement insert_Stmt = db.prepareStatement("INSERT INTO tag_relation_property_string (entity_type,predicate,property,property_value,similarity) VALUES (?,?,?,?,?);"))
        {
            ResultSet mapping = value_Stmt.executeQuery();
            while (mapping.next()) {
                Integer id = mapping.getInt(1);
                String content = mapping.getString(2);
                mapping_map.put(id,content);
            }
            ResultSet str_pro = str_stmt.executeQuery();
            while (str_pro.next()) {
                Integer entity_type = str_pro.getInt(1);
                Integer predicate_id = str_pro.getInt(2);
                Integer property_id = str_pro.getInt(3);
                Integer property_value = str_pro.getInt(4);
                Double similarity = str_pro.getDouble(5);

                String type = mapping_map.get(entity_type);
                String pre = mapping_map.get(predicate_id);
                String pro = mapping_map.get(property_id);
                String pro_value = mapping_map.get(property_value);
                insert_Stmt.setString(1, type);
                insert_Stmt.setString(2, pre);
                insert_Stmt.setString(3, pro);
                insert_Stmt.setString(4, pro_value);
                insert_Stmt.setDouble(5,similarity);
                insert_Stmt.execute();
            }
        }
        db.commit();
    }
    private static void relation_property_numerical(Connection db) throws Exception{
        Map<Integer, String> mapping_map = new HashMap<Integer, String>();
        try (Statement stmt = db.createStatement()) {
            //Label the entity and the type of label is relation_numerical_tags
            stmt.execute("CREATE TABLE tag_relation_property_numerical (entity_type CHAR ,predicate CHAR,property CHAR,property_value_range CHAR,similarity DOUBLE );");
        }
        try (PreparedStatement str_stmt = db.prepareStatement("SELECT DISTINCT entity1_type,predicate_id,property_id,property_value_range,similarity FROM relation_numerical_tags; ");
             PreparedStatement value_Stmt = db.prepareStatement("SELECT id, content FROM mapping");
             PreparedStatement insert_Stmt = db.prepareStatement("INSERT INTO tag_relation_property_numerical (entity_type,predicate,property,property_value_range,similarity) VALUES (?,?,?,?,?);"))
        {
            ResultSet mapping = value_Stmt.executeQuery();
            while (mapping.next()) {
                Integer id = mapping.getInt(1);
                String content = mapping.getString(2);
                mapping_map.put(id,content);
            }
            ResultSet str_pro = str_stmt.executeQuery();
            while (str_pro.next()) {
                Integer entity_type = str_pro.getInt(1);
                Integer predicate_id = str_pro.getInt(2);
                Integer property_id = str_pro.getInt(3);
                String property_value_range = str_pro.getString(4);
                Double similarity = str_pro.getDouble(5);

                String type = mapping_map.get(entity_type);
                String pre = mapping_map.get(predicate_id);
                String pro = mapping_map.get(property_id);
                insert_Stmt.setString(1, type);
                insert_Stmt.setString(2, pre);
                insert_Stmt.setString(3, pro);
                insert_Stmt.setString(4, property_value_range);
                insert_Stmt.setDouble(5,similarity);
                insert_Stmt.execute();
            }
        }
        db.commit();

    }
    private static void tags_sort(Connection db) throws Exception{
        Map<Integer, String> mapping_map = new HashMap<Integer, String>();

        try (Statement stmt = db.createStatement()) {
            //Reorder tag set
            stmt.execute("CREATE TABLE resort_labels (classes INT ,types CHAR ,predicate CHAR ,property CHAR ,object CHAR ,score DOUBLE,ranking INT,distinction DOUBLE );");
        }
        try (PreparedStatement str_stmt = db.prepareStatement("SELECT  classes,types,predicate,property,object,score,ranking FROM tags_sort; ");
             PreparedStatement value_Stmt = db.prepareStatement("SELECT id, content FROM mapping");
             PreparedStatement string_tags_Stmt = db.prepareStatement("SELECT similarity FROM string_tags WHERE entity_type=? AND property_id=? AND property_value=?; ");
             PreparedStatement num_tags_Stmt = db.prepareStatement("SELECT similarity FROM numerical_tags WHERE entity_type=? AND property_id=? AND property_value_range=?; ");
             PreparedStatement relations_tags_Stmt = db.prepareStatement("SELECT similarity FROM relation_tags WHERE entity1_type=? AND predicate_id=? AND entity2_id=?; ");
             PreparedStatement relations_string_tags_Stmt = db.prepareStatement("SELECT similarity FROM relation_string_tags WHERE entity1_type=? AND predicate_id=? AND property_id=? AND  property_value=?; ");
             PreparedStatement relations_num_tags_Stmt = db.prepareStatement("SELECT similarity FROM relation_numerical_tags WHERE entity1_type=? AND predicate_id=? AND  property_id=? AND property_value_range=?; ");

             PreparedStatement insert_Stmt = db.prepareStatement("INSERT INTO resort_labels (classes,types,predicate,property,object,score,ranking,distinction) VALUES (?,?,?,?,?,?,?,?);"))
        {
            ResultSet mapping = value_Stmt.executeQuery();
            while (mapping.next()) {
                Integer id = mapping.getInt(1);
                String content = mapping.getString(2);
                mapping_map.put(id,content);
            }
            ResultSet str_pro = str_stmt.executeQuery();
            while (str_pro.next()) {
                String  type = null,pre=null,pro=null,object_1=null;
                Double distinction = 0.0;

                Integer classes = str_pro.getInt(1);
                Integer entity_type = str_pro.getInt(2);
                Integer predicate_id = str_pro.getInt(3);
                Integer property_id = str_pro.getInt(4);
                String object = str_pro.getString(5);
                Double score = str_pro.getDouble(6);
                Integer rank = str_pro.getInt(7);

                type = mapping_map.get(entity_type);

                //string_tags
                if (classes==1){
                    pro = mapping_map.get(property_id);
                    string_tags_Stmt.setInt(1,entity_type);
                    string_tags_Stmt.setInt(2,property_id);
                    string_tags_Stmt.setInt(3,Integer.parseInt(object));
                    ResultSet string_sim = string_tags_Stmt.executeQuery();
                    if (string_sim.next())
                        distinction = string_sim.getDouble(1);
                    object_1 = mapping_map.get(Integer.parseInt(object));
                }
                //numerical_tags
                else if (classes==2){
                    num_tags_Stmt.setInt(1,entity_type);
                    num_tags_Stmt.setInt(2,property_id);
                    num_tags_Stmt.setString(3,object);
                    ResultSet num_sim = num_tags_Stmt.executeQuery();
                    if (num_sim.next())
                        distinction = num_sim.getDouble(1);
                    pro = mapping_map.get(property_id);
                    object_1 = object;
                }
                //relation_tags
                else if (classes==3){
                    relations_tags_Stmt.setInt(1,entity_type);
                    relations_tags_Stmt.setInt(2,predicate_id);
                    relations_tags_Stmt.setInt(3,Integer.parseInt(object));
                    ResultSet relation_sim = relations_tags_Stmt.executeQuery();
                    if (relation_sim.next())
                        distinction = relation_sim.getDouble(1);
                    pre = mapping_map.get(predicate_id);
                    object_1 = mapping_map.get(Integer.parseInt(object));
                }
                //relation_string_tags
                else if (classes==4){
                    relations_string_tags_Stmt.setInt(1,entity_type);
                    relations_string_tags_Stmt.setInt(2,predicate_id);
                    relations_string_tags_Stmt.setInt(3,property_id);
                    relations_string_tags_Stmt.setInt(4,Integer.parseInt(object));
                    ResultSet relation_string_sim = relations_string_tags_Stmt.executeQuery();
                    if (relation_string_sim.next())
                        distinction = relation_string_sim.getDouble(1);
                    pre = mapping_map.get(predicate_id);
                    pro = mapping_map.get(property_id);
                    object_1 = mapping_map.get(Integer.parseInt(object));
                }
                //relation_numerical_tags
                else if (classes==5){
                    relations_num_tags_Stmt.setInt(1,entity_type);
                    relations_num_tags_Stmt.setInt(2,predicate_id);
                    relations_num_tags_Stmt.setInt(3,property_id);
                    relations_num_tags_Stmt.setString(4,object);
                    ResultSet relation_num_sim = relations_num_tags_Stmt.executeQuery();
                    if (relation_num_sim.next())
                        distinction = relation_num_sim.getDouble(1);
                    pre = mapping_map.get(predicate_id);
                    pro = mapping_map.get(property_id);
                    object_1 = object;
                }
                insert_Stmt.setInt(1,classes);
                insert_Stmt.setString(2, type);
                insert_Stmt.setString(3, pre);
                insert_Stmt.setString(4, pro);
                insert_Stmt.setString(5, object_1);
                insert_Stmt.setDouble(6,score);
                insert_Stmt.setInt(7,rank);
                insert_Stmt.setDouble(8,distinction);
                insert_Stmt.execute();
            }
        }
        db.commit();
    }
    //Reordering resulting in the final tag set
    private static void final_tags(Connection db) throws Exception{
        try (Statement stmt = db.createStatement()) {
            stmt.execute("CREATE TABLE final_labels (classes INT ,types CHAR ,predicate CHAR ,property CHAR ,object CHAR ,score DOUBLE,ranking INT,distinction DOUBLE );");
        }
        try (PreparedStatement value_Stmt = db.prepareStatement("SELECT  classes,types,predicate,property,object,score,ranking,distinction FROM resort_labels ORDER BY ranking; ");
             PreparedStatement insert_Stmt = db.prepareStatement("INSERT INTO final_labels (classes,types,predicate,property,object,score,ranking,distinction) VALUES (?,?,?,?,?,?,?,?);"))
        {
            ResultSet str_pro = value_Stmt.executeQuery();
            while (str_pro.next()) {

                Integer classes = str_pro.getInt(1);
                String entity_type = str_pro.getString(2);
                String predicate_id = str_pro.getString(3);
                String property_id = str_pro.getString(4);
                String object = str_pro.getString(5);
                Double score = str_pro.getDouble(6);
                Integer rank = str_pro.getInt(7);
                Double distinction = str_pro.getDouble(8);
                if (rank<=500){
                    insert_Stmt.setInt(1,classes);
                    insert_Stmt.setString(2, entity_type);
                    insert_Stmt.setString(3, predicate_id);
                    insert_Stmt.setString(4, property_id);
                    insert_Stmt.setString(5, object);
                    insert_Stmt.setDouble(6,score);
                    insert_Stmt.setInt(7,rank);
                    insert_Stmt.setDouble(8,distinction);
                    insert_Stmt.execute();
                }
            }
        }
        db.commit();
    }

    public static void main(String[] args)throws Exception {
        Connection db = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);
        db.setAutoCommit(false);
        string(db);
        numerical(db);
        relation(db);
        relation_property_string(db);
        relation_property_numerical(db);
        tags_sort(db);
        final_tags(db);
    }
}
