import java.sql.*;
/*statistical  support value of attribute (the type of the attribute is string).*/
public class make_string_support {
    private static String sqlitePath = "/base_data/HSP_ALL.sqlite";
    
    private static void String_support(Connection db) throws Exception{
        try(Statement stmt = db.createStatement()){
          
            stmt.execute("CREATE TABLE property_string_support (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT," +
                    "type_id INTEGER NOT NULL,  " +
                    "property_id  INTEGER NOT NULL, " +
                    "property_value  INTEGER NOT NULL, " +
                    "support_property_value DOUBLE NOT NULL)");
        }
        try(
                PreparedStatement property_string_Stmt = db.prepareStatement("SELECT type_id,property_id,property_value,support_property_value " +
                        "FROM property_mid_support WHERE (SELECT string_type_id FROM mapping WHERE mapping.id=property_mid_support.property_value)=2 OR (SELECT string_type_id FROM mapping WHERE mapping.id=property_mid_support.property_value)=3;");
                PreparedStatement stmt = db.prepareStatement("INSERT INTO property_string_support (type_id,property_id,property_value,support_property_value) VALUES (?,?,?,?)"))
        {
            ResultSet property_string = property_string_Stmt.executeQuery();
            while(property_string.next()){
                stmt.setInt(1,property_string.getInt(1));
                stmt.setInt(2,property_string.getInt(2));
                stmt.setInt(3,property_string.getInt(3));
                stmt.setDouble(4,property_string.getDouble(4));
                stmt.execute();
            }

        }
        db.commit();

    }
    public static void main(String[] args)throws Exception {
        Connection db = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);
        db.setAutoCommit(false);
        String_support(db);
    }
}
