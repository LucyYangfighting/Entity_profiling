import java.sql.*;

/**make the tag set after filtering and HSP model */
public class make_label {

    private static String sqlitePath = "/base_data/HSP_ALL.sqlite";
    //Create a string attribute tag set
    private static void make_string_tags(Connection db) throws Exception {
        try (Statement stmt = db.createStatement()) {
            //build the string_tags table: string attribute tag set
            stmt.execute("CREATE TABLE string_tags (entity_type INT NOT NULL ,property_id INT,property_value INT,similarity DOUBLE );");
        }
        try (Statement indexingStmt = db.createStatement()) {
            indexingStmt.execute("CREATE INDEX pro_str_e_type ON string_tags(entity_type)");
        }
        db.commit();
        int count = 0;
        try (PreparedStatement str_stmt = db.prepareStatement("SELECT entity_type,property_id,property_value,similarity FROM str_similarity ORDER BY similarity DESC; ");
             PreparedStatement insert_Stmt = db.prepareStatement("INSERT INTO string_tags " +
                     "(entity_type,property_id,property_value,similarity) VALUES (?,?,?,?);")) {
            ResultSet str_pro = str_stmt.executeQuery();
            while (str_pro.next()) {
                count++;
                Integer entity_type = str_pro.getInt(1);
                Integer property_id = str_pro.getInt(2);
                Integer property_value = str_pro.getInt(3);
                double  similarity = str_pro.getDouble(4);
                //top 20 and similarity>=0.5 (Adjust parameters according to their needs)
                if ((count <= 160)&&(similarity >= 0.5)) {
                    insert_Stmt.setInt(1, entity_type);
                    insert_Stmt.setInt(2, property_id);
                    insert_Stmt.setInt(3, property_value);
                    insert_Stmt.setDouble(4, similarity);
                    insert_Stmt.execute();
                }
            }

        }
        db.commit();
    }
    //Create the numerical attribute tag set
    private static void make_numerical_tags(Connection db) throws Exception {
        try (Statement stmt = db.createStatement()) {
            //build the numerical_tags table: numerical attribute tag set
            stmt.execute("CREATE TABLE numerical_tags (entity_type INT NOT NULL ,property_id INT,property_value_range CHAR,similarity DOUBLE );");
        }
        try (Statement indexingStmt = db.createStatement()) {
            indexingStmt.execute("CREATE INDEX pro_int_e_type ON numerical_tags(entity_type)");
        }
        db.commit();
        int count = 0;
        try (PreparedStatement int_stmt = db.prepareStatement("SELECT entity_type,property_id,property_value_range,similarity FROM numerical_similarity ORDER BY similarity DESC; ");
             PreparedStatement insert_Stmt = db.prepareStatement("INSERT INTO numerical_tags " +
                     "(entity_type,property_id,property_value_range,similarity) VALUES (?,?,?,?);")
        ) {
            ResultSet int_pro = int_stmt.executeQuery();
            while (int_pro.next()) {
                count++;
                Integer entity_type = int_pro.getInt(1);
                Integer property_id = int_pro.getInt(2);
                String property_value_range = int_pro.getString(3);
                double sim = int_pro.getDouble(4);
                //top 20 and similarity>=0.5(Adjust parameters according to their needs)
                if ((count <= 160)&&(sim>=0.015)) {
                    insert_Stmt.setInt(1, entity_type);
                    insert_Stmt.setInt(2, property_id);
                    insert_Stmt.setString(3, property_value_range);
                    insert_Stmt.setDouble(4, sim);
                    insert_Stmt.execute();
                }
            }
        }
        db.commit();
    }
    //build the<predicate，entity2> relation_tags
    private static void make_relation_tags(Connection db) throws Exception{
        try (Statement stmt = db.createStatement()) {

            stmt.execute("CREATE TABLE relation_tags (entity1_type INTEGER NOT NULL,predicate_id  INTEGER NOT NULL," +
                    "entity2_id INTEGER NOT NULL,similarity DOUBLE );");
        }
        try (Statement indexingStmt = db.createStatement()) {
            indexingStmt.execute("CREATE INDEX realtion_e_type ON relation_tags(entity1_type)");
        }
        db.commit();
        int count = 0;
        try (PreparedStatement relation_stmt = db.prepareStatement("SELECT entity_1_type,predicate_id,entity2_id,similarity FROM relation_similarity ORDER BY similarity DESC; ");
             PreparedStatement insert_Stmt = db.prepareStatement("INSERT INTO relation_tags " +
                     "(entity1_type,predicate_id,entity2_id,similarity) VALUES (?,?,?,?);")
        ) {
            ResultSet relation = relation_stmt.executeQuery();
            while (relation.next()) {
                count++;
                Integer entity_1_type = relation.getInt(1);
                Integer predicate_id = relation.getInt(2);
                Integer entity2_id = relation.getInt(3);
                double  sim_dif = relation.getDouble(4);
                if ((count <= 160)&&(sim_dif >= 0.01)) {
                    insert_Stmt.setInt(1, entity_1_type);
                    insert_Stmt.setInt(2, predicate_id);
                    insert_Stmt.setInt(3, entity2_id);
                    insert_Stmt.setDouble(4, sim_dif);
                    insert_Stmt.execute();
                }
            }
        }
        db.commit();
    }
    //build the <predicate，the string property of the entity2>relation_string_tags
    private static void make_relation_string_tags(Connection db) throws Exception{
        try (Statement stmt = db.createStatement()) {
           
            stmt.execute("CREATE TABLE relation_string_tags (entity1_type INTEGER NOT NULL,predicate_id  INTEGER NOT NULL," +
                    "property_id INTEGER NOT NULL,property_value INT,similarity DOUBLE );");
        }
        try (Statement indexingStmt = db.createStatement()) {
            indexingStmt.execute("CREATE INDEX r_pro_str_type ON relation_string_tags(entity1_type)");
        }
        db.commit();
        int count = 0;
        try (
                PreparedStatement insert_Stmt = db.prepareStatement("INSERT INTO relation_string_tags " +
                        "(entity1_type,predicate_id,property_id,property_value,similarity) VALUES (?,?,?,?,?);");
                PreparedStatement relation_pro_str_stmt = db.prepareStatement("SELECT entity1_type,predicate_id,property_id,property_value,similarity FROM relation_string_similarity ORDER BY similarity DESC; ")

        ) {
            ResultSet relation_pro_str = relation_pro_str_stmt.executeQuery();
            while (relation_pro_str.next()) {
                count++;
                Integer e_type = relation_pro_str.getInt(1);
                Integer pre = relation_pro_str.getInt(2);
                Integer pro = relation_pro_str.getInt(3);
                Integer pro_value = relation_pro_str.getInt(4);
                double  sim_dif = relation_pro_str.getDouble(5);
                if ((count <= 160)&&(sim_dif >= 0.525)) {
                    insert_Stmt.setInt(1, e_type);
                    insert_Stmt.setInt(2, pre);
                    insert_Stmt.setInt(3, pro);
                    insert_Stmt.setInt(4, pro_value);
                    insert_Stmt.setDouble(5, sim_dif);
                    insert_Stmt.execute();
                }
            }
        }
        db.commit();
    }
    //build the <predicate，the numerical property of the entity2>relation_numerical_tags
    private static void make_relation_numerical_tags(Connection db) throws Exception{
        try (Statement stmt = db.createStatement()) {
        
            stmt.execute("CREATE TABLE relation_numerical_tags (entity1_type INTEGER NOT NULL,predicate_id  INTEGER NOT NULL," +
                    "property_id INTEGER NOT NULL,property_value_range CHAR,similarity DOUBLE );");
        }
        try (Statement indexingStmt = db.createStatement()) {
            indexingStmt.execute("CREATE INDEX r_pro_int_type ON relation_numerical_tags(entity1_type)");
        }
        db.commit();
        int count = 0;
        try (
                PreparedStatement insert_Stmt = db.prepareStatement("INSERT INTO relation_numerical_tags " +
                        "(entity1_type,predicate_id,property_id,property_value_range,similarity) VALUES (?,?,?,?,?);");
                PreparedStatement relation_pro_int_stmt = db.prepareStatement("SELECT entity1_type,predicate_id,property_id,property_value_range,similarity FROM relation_numerical_similarity ORDER BY similarity DESC; ");

        ) {
            ResultSet relation_pro_int = relation_pro_int_stmt.executeQuery();
            while (relation_pro_int.next()) {
                count++;
                Integer e_type = relation_pro_int.getInt(1);
                Integer pre = relation_pro_int.getInt(2);
                Integer pro = relation_pro_int.getInt(3);
                String pro_value_range = relation_pro_int.getString(4);
                double  sim_dif = relation_pro_int.getDouble(5);
                //f:s_s_sim>s_w_sim>s_c_sim
                if ((count <= 160)&&(sim_dif >= 0.37)) {
                    insert_Stmt.setInt(1, e_type);
                    insert_Stmt.setInt(2, pre);
                    insert_Stmt.setInt(3, pro);
                    insert_Stmt.setString(4, pro_value_range);
                    insert_Stmt.setDouble(5, sim_dif);
                    insert_Stmt.execute();
                }
            }
        }
        db.commit();
    }
    //Create the final tag set (after reordering, select the top 80%)
    private static void make_final_tags(Connection db) throws Exception{
        try (Statement stmt = db.createStatement()) {
          
            stmt.execute("CREATE TABLE final_tags (classes INT ,types INT,predicate INT ,property INT ,object CHAR ,score DOUBLE,ranking INT);");
        }
        db.commit();
        try (   PreparedStatement tags_stmt = db.prepareStatement("SELECT  classes,types,predicate,property,object,score,ranking FROM tags_sort ORDER BY ranking; ");
                PreparedStatement insert_Stmt = db.prepareStatement("INSERT INTO final_tags " +
                        "(classes,types,predicate,property,object,score,ranking ) VALUES (?,?,?,?,?,?,?);");

        ) {
            ResultSet tags = tags_stmt.executeQuery();
            while (tags.next()) {
                Integer classes = tags.getInt(1);
                Integer entity_type = tags.getInt(2);
                Integer predicate_id = tags.getInt(3);
                Integer property_id = tags.getInt(4);
                String object = tags.getString(5);
                Double score = tags.getDouble(6);
                Integer rank = tags.getInt(7);

                if (rank<=500) {
                    insert_Stmt.setInt(1, classes);
                    insert_Stmt.setInt(2, entity_type);
                    insert_Stmt.setInt(3, predicate_id);
                    insert_Stmt.setInt(4, property_id);
                    insert_Stmt.setString(5, object);
                    insert_Stmt.setDouble(6, score);
                    insert_Stmt.setInt(7, rank);
                    insert_Stmt.execute();
                }
            }
        }
        db.commit();
    }
    public static void main(String[] args)throws Exception {
        Connection db = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);
        db.setAutoCommit(false);
        make_string_tags(db);
        make_numerical_tags(db);
        make_relation_tags(db);
        make_relation_string_tags(db);
        make_relation_numerical_tags(db);
        make_final_tags(db);
    }
}
