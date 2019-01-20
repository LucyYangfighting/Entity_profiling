import java.sql.*;

/**Filter attribute values of support(According to your respective needs)*/
public class filter_support {
    private static String sqlitePath = "/base_data/HSP_ALL.sqlite";
    //Filter attribute values of type string
    private static void filter_String_support(Connection db) throws Exception{
        try(Statement stmt = db.createStatement()){
            
            stmt.execute("CREATE TABLE filter_pro_string_sup (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT," +
                    "type_id INTEGER NOT NULL,  " +
                    "property_id  INTEGER NOT NULL, " +
                    "property_value  INTEGER NOT NULL, " +
                    "support_property_value DOUBLE NOT NULL)");
        }
        try(
                PreparedStatement pro_filter_string_Stmt = db.prepareStatement("SELECT type_id,property_id,property_value,support_property_value FROM property_string_support WHERE (support_property_value<0.90 AND support_property_value>0)");
                PreparedStatement insert_stmt = db.prepareStatement("INSERT INTO filter_pro_string_sup (type_id,property_id,property_value,support_property_value) VALUES (?,?,?,?)")
          )
        {
            ResultSet pro_filter_string = pro_filter_string_Stmt.executeQuery();
            while(pro_filter_string.next()){
                int a =pro_filter_string.getInt(1);
                insert_stmt.setInt(1,a);
                insert_stmt.setInt(2,pro_filter_string.getInt(2));
                insert_stmt.setInt(3,pro_filter_string.getInt(3));
                insert_stmt.setDouble(4,pro_filter_string.getDouble(4));
                insert_stmt.execute();
            }

        }
        db.commit();

    }
    //Filter attribute values of type numerical
    private static void filter_int_double_support(Connection db) throws Exception{
        try(Statement creat_stmt = db.createStatement()){
            creat_stmt.execute("CREATE TABLE filter_pro_numerical_sup (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT," +
                    "type_id INTEGER NOT NULL,  " +
                    "property_id  INTEGER NOT NULL, " +
                    "property_value_range  CHAR NOT NULL, " +
                    "pro_value_range_support DOUBLE NOT NULL)");
        }
        db.commit();
        try(PreparedStatement pro_int_stmt = db.prepareStatement("SELECT type_id,property_id,property_value_range,pro_value_range_support FROM property_numerical_support WHERE (pro_value_range_support<0.90 AND pro_value_range_support>0)");
        PreparedStatement insert = db.prepareStatement("INSERT INTO filter_pro_numerical_sup(type_id,property_id,property_value_range,pro_value_range_support) VALUES (?,?,?,?)")){
            ResultSet pro_int = pro_int_stmt.executeQuery();
            while(pro_int.next()){
                insert.setInt(1,pro_int.getInt(1));
                insert.setInt(2,pro_int.getInt(2));
                insert.setString(3,pro_int.getString(3));
                insert.setDouble(4,pro_int.getDouble(4));
                insert.execute();
            }
        }
        db.commit();
    }

    public static void main(String[] args)throws Exception {
        Connection db = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);
        db.setAutoCommit(false);
        filter_String_support(db);
        filter_int_double_support(db);

    }
}
