package small_goal;
 /*Continuous numerical discretization：
  1.Equal width division
  2.Local density division
statistical  support value of attribute (the type of the attribute is numerical)
  */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;


public class Numerical_discretization {
    private static String sqlitePath = "/base_data/HSP_ALL.sqlite";
    //Written into the database table
    private static void write_table(Connection db,int type_id,int property_id, String pro_value_range, double property_value_sup)throws Exception {
        try(PreparedStatement pro_insert_stmt = db.prepareStatement("INSERT INTO property_numerical_support (type_id,property_id,property_value_range,pro_value_range_support) VALUES (?,?,?,?)")){
            pro_insert_stmt.setInt(1, type_id);
            pro_insert_stmt.setInt(2, property_id);
            pro_insert_stmt.setString(3, pro_value_range);
            pro_insert_stmt.setDouble(4, property_value_sup);
            pro_insert_stmt.execute();
        }
        db.commit();
    }
    private static void range(Connection db,int type_id,int entity_num, int property_id, int list_low_size,List<Double> list_low,Map<Double, Integer> value_map)throws Exception{
        int num_property_value, value;
        double is_value, pro_value_range_sup;
        String pro_value_area;
        for (int i = 0; i < list_low_size -1; i++) {
            value = 0;
            if (i == list_low_size - 2){
                pro_value_area = "[" + list_low.get(i) + "," + list_low.get(i+1)+ "]";
                for (Map.Entry<Double, Integer> entry : value_map.entrySet()) {
                    is_value = entry.getKey();
                    num_property_value = entry.getValue();
                    if ((is_value>=list_low.get(i))&&(is_value<=list_low.get(i+1)))
                        value += num_property_value;
                }
            }
            else{
                pro_value_area = "[" + list_low.get(i) + "," + list_low.get(i+1)+ ")";
                for (Map.Entry<Double, Integer> entry : value_map.entrySet()) {
                    is_value = entry.getKey();
                    num_property_value = entry.getValue();
                    if ((is_value >= list_low.get(i))&&(is_value < list_low.get(i+1)))
                        value += num_property_value;
                }
            }
            pro_value_range_sup = (value * 1.0) / entity_num;
            write_table(db,type_id,property_id,pro_value_area,pro_value_range_sup);
        }
    }
    //Equal width division
    private static void Equal_width_division(Connection db,int type_id,int entity_num, int property_id, int property_value_num, Map<Double, Integer> value_map,List<Double> list)throws Exception{
        //System.out.print(list);
        int area = 10, num_property_value;
        double value_min, value_max, is_value, space, pro_value_range_sup;
        double [] arr_range = new double[area+1];
        int [] arr_count = new int[area];
        String pro_value_area;
        //if property_value_num<10,Divided into property_value_num ranges
        if (property_value_num < area){
            for (Map.Entry<Double, Integer> entry : value_map.entrySet()) {
                is_value = entry.getKey();
                num_property_value = entry.getValue();
                pro_value_area = "[" + is_value + "," + is_value + "]";
                pro_value_range_sup = (num_property_value * 1.0) / entity_num;
                write_table(db,type_id,property_id,pro_value_area,pro_value_range_sup);
            }
        }//if property_value_num>=10,Divided into 10 ranges
        else if (property_value_num >= area) {
            //Minimum and maximum value of the attribute value
            value_min = Collections.min(list);
            value_max = Collections.max(list);
            //space:Length of each range
            space = (value_max - value_min) / area;
            //Attribute range
            arr_range[0] = value_min;
            for (int i=1; i < area; i++){
                arr_range[i] = arr_range[i-1] + space;
            }
            arr_range[area] = value_max;
            //the number of the attribute range
            for (int i = 0; i < area; i++)
                arr_count[i]=0;
            for (Map.Entry<Double, Integer> entry : value_map.entrySet()) {
                is_value = entry.getKey();
                num_property_value = entry.getValue();
                for (int j = 0; j < area; j++) {
                    if ((is_value >= arr_range[j]) && (is_value < arr_range[j + 1]))
                        arr_count[j] += num_property_value;
                }
                if (is_value == value_max){
                    arr_count[area-1] += num_property_value;
                }
            }
            //Calculation the value of the support and Written into the database table
            for (int i = 0; i < area; i++){
                if (i == area-1)
                    pro_value_area = "[" + arr_range[i] + "," + arr_range[i+1]+ "]";
                else
                    pro_value_area = "[" + arr_range[i] + "," + arr_range[i+1]+ ")";
                pro_value_range_sup = (arr_count[i] * 1.0) / entity_num;
                write_table(db,type_id,property_id,pro_value_area,pro_value_range_sup);
            }
        }
    }
    //Local density division
    private static void Local_density_division(Connection db,int type_id,int entity_num, int property_id, int property_value_num,Map<Double, Integer> value_map,List<Double> list)throws Exception{
        List<Double> list_low = new ArrayList<Double>();//Storage is the value of the bottom of the property
        List<Double> list_low_range = new ArrayList<Double>();
        int list_low_size, list_low_range_size, range, area = 10;
        double value_is;
        //Ascending order
        Collections.sort(list);
        //Looking for a critical point
        list_low.clear();
        list_low_range.clear();
        list_low.add(list.get(0));
        for (int i = 1; i <  property_value_num - 1; i++){
            value_is = list.get(i);
            int value_is_num = value_map.get(value_is);
            if ((value_map.get(list.get(i-1)) > value_is_num)&&(value_is_num <= value_map.get(list.get(i+1))))
                    list_low.add(value_is);
        }
        list_low.add(list.get(property_value_num - 1));
        //Screening critical point
        list_low_size = list_low.size();
        if (list_low_size == 2)
            Equal_width_division(db,type_id, entity_num, property_id, property_value_num,value_map,list);
        else if ((2 < list_low_size) && (list_low_size <= 10)){
            range(db,type_id,entity_num, property_id, list_low_size, list_low, value_map);
        }
        else{
            //The case where the number of critical points is large (divided into 10)
            range = list_low_size/area;
            for (int i = 0; i < list_low_size; i = i+range) {
                list_low_range.add(list_low.get(i));
            }
            list_low_range.add(list_low.get(list_low_size - 1));
            list_low_range_size = list_low_range.size();
            range(db,type_id,entity_num, property_id, list_low_range_size, list_low_range, value_map);
        }
    }

    //Judgment of interval division method. Parameter default：Density_Threshold_min=0.3，Density_Threshold_max=1
    private static void num_division_main(Connection db, double Density_Threshold_min, double Density_Threshold_max) throws Exception{
        
        try (Statement stmt = db.createStatement()) {
            stmt.execute("CREATE TABLE property_numerical_support (type_id INTEGER NOT NULL," +
                    "property_id INTEGER NOT NULL, property_value_range CHAR NOT NULL," +
                    "pro_value_range_support DOUBLE NOT NULL)");
        }
        db.commit();
        //Defining variables
        Map<Double, Integer> value_map = new HashMap<Double, Integer>(); 
        List<Double> list = new ArrayList<Double>(); //<属性值>

        try (PreparedStatement property_numerical_Stmt = db.prepareStatement("SELECT DISTINCT type_id,entity_num, property_id,property_en_num,property_value_num FROM property_mid_support WHERE (SELECT string_type_id FROM mapping WHERE mapping.id=property_mid_support.property_value)!=2 AND (SELECT string_type_id FROM mapping WHERE mapping.id=property_mid_support.property_value)!=3;");
             //Find a type, the value of all attributes of an attribute, and the number of entities with attribute values
             PreparedStatement property_value_Stmt = db.prepareStatement("SELECT DISTINCT property_value,num_property_value FROM property_mid_support WHERE type_id=? AND property_id=?;");
             //Find the specific value of a specific attribute value
             PreparedStatement value_real_stmt = db.prepareStatement("SELECT content FROM mapping WHERE id=?;")){
            ResultSet property_numerical = property_numerical_Stmt.executeQuery();
            while(property_numerical.next()) {
                value_map.clear();
                list.clear();
                Integer type_id = property_numerical.getInt(1);
                //The number of entities about this type
                Integer entity_num = property_numerical.getInt(2);
                Integer property_id = property_numerical.getInt(3);
                //Count the number of entities with this attribute，as property_entity_num
                Integer property_entity_num = property_numerical.getInt(4);
                //Count how many different values of this attribute value，as property_value_num
                Integer property_value_num = property_numerical.getInt(5);

                property_value_Stmt.setInt(1, type_id);
                property_value_Stmt.setInt(2, property_id);
                ResultSet property_value = property_value_Stmt.executeQuery();
                while (property_value.next()) {
                    Integer property_value_id = property_value.getInt(1);
                    Integer num_property_value = property_value.getInt(2);
                    //The  value of a  attribute 
                    value_real_stmt.setInt(1, property_value_id);
                    ResultSet value_real = value_real_stmt.executeQuery();
                    double pro_value = value_real.getDouble(1);
                
                    value_map.put(pro_value, num_property_value);
                    list.add(pro_value);
                }

                //Degree of dispersion of attribute values：P=property_value_num/property_entity_num<=1
                double P =  (property_value_num*1.0)/property_entity_num;
                //1.Density_Threshold_min<P<=Density_Threshold_max: Equal width division
                //2.P<=Density_Threshold_min:Local density division
                if ((Density_Threshold_min < P)&&(P <= Density_Threshold_max))
                    Equal_width_division(db,type_id, entity_num, property_id, property_value_num,value_map,list);
                else if (P <= Density_Threshold_min)
                    Local_density_division(db,type_id, entity_num, property_id, property_value_num,value_map,list);
            }
        }
        db.commit();
    }
    public static void main(String[] args)throws Exception{
        Connection db = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);
        db.setAutoCommit(false);
        System.out.println("MAIN");
        num_division_main(db,0.5,1.0);
    }
}