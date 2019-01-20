import org.apache.commons.lang3.StringUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;

import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**Parsing raw data(eg:.rdf/.nt/.ttl), the experimental data set is drugbank.nt
 * building five tables：
 *              (1)mapping table: Map strings and integers
 *              (2)types_string table: the type of string_id
 *              (3)triples_all table: Store all triples
 *              (4)nodes_type table: Store all  nodes and the type of the node
 *              (5)types_node table: stroe the type of the all entity*/
class IDGenerator implements Iterable<Map.Entry<String, Integer>> {
    private final HashMap<String, Integer> valuesByID = new HashMap<String, Integer>();
    private int currentID = 1;

    Integer idOf(String value){
        Integer id = valuesByID.get(value);
        if(id == null){
            id = currentID;
            currentID++;
            valuesByID.put(value, id);
        }
        return id;
    }

    Integer get(String value){
        return valuesByID.get(value);
    }

    public Iterator<Map.Entry<String, Integer>> iterator() {
        return valuesByID.entrySet().iterator();
    }
}

public class RDFDatabase {

    private static String sqlitePath = "/base_data/HSP_ALL.sqlite";
    private static String rdfPath = "/base_data/drugbank.nt";
    private static  int num = 0;

    private static void build_table(Connection db) throws Exception{
        try(Statement stmt = db.createStatement()){
            //mapping table: Map strings and integers
            stmt.execute("CREATE TABLE mapping (id INTEGER NOT NULL PRIMARY KEY, content TEXT NOT NULL, string_type_id INTEGER NOT NULL)");
            //the type of string_id
            stmt.execute("CREATE TABLE types_string (id INTEGER NOT NULL PRIMARY KEY, type_string TEXT NOT NULL )");
            //Store all triples
            stmt.execute("CREATE TABLE triples_all(id INTEGER NOT NULL PRIMARY KEY, subject_id INTEGER NOT NULL, predicate_id INTEGER NOT NULL, object_id INTEGER NOT NULL)");
            //Store all  nodes and the type of the node
            stmt.execute("CREATE TABLE nodes_type (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, node_id INTEGER NOT NULL, type_id INTEGER NOT NULL)");
            //stroe the type of the all entity
            stmt.execute("CREATE TABLE types_node (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, types_id INTEGER NOT NULL)");

        }

        final IDGenerator contentIDGenerator = new IDGenerator();
        final IDGenerator typeIDGenerator = new IDGenerator();

        RDFParser.create().source(rdfPath).lang(Lang.TURTLE).build().parse(new StreamRDF() {
            public void start() {

            }

            private void saveContent(Node node) {

                String content = node.toString();

                String typeContent;//Refers to the classification of uri, literal and blank, not the different types of nodes
                if(node.isURI()){
                    typeContent = "uri";
                }
                else if(node.isLiteral()){
                    typeContent = node.getLiteral().getDatatype().getURI();
                }else if(node.isBlank()){
                    typeContent = "blank";
                }else{
                    return;
                }
                contentIDGenerator.idOf(typeIDGenerator.idOf(typeContent).toString() + "|" + content);
            }

            public void triple(Triple triple) {
                saveContent(triple.getSubject());
                saveContent(triple.getPredicate());
                saveContent(triple.getObject());
            }

            public void quad(Quad quad) {
                System.out.println("quad");
            }

            public void base(String s) {

            }

            public void prefix(String s, String s1) {

            }

            public void finish() {

            }
        });

        Integer typeURIID = typeIDGenerator.idOf("uri");
        Integer rdfTypeID = contentIDGenerator.idOf(typeURIID + "|http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

        try(PreparedStatement stmt = db.prepareStatement("INSERT INTO mapping (id, content, string_type_id) VALUES (?,?,?)")) {
            for(Map.Entry<String, Integer> entry: contentIDGenerator){
                String[] components = StringUtils.split(entry.getKey(), "|", 2); // 2：Returns the maximum length of the array
                String content = components[1];
                Integer typeID = Integer.parseInt(components[0]);
                stmt.setInt(1,entry.getValue());
                stmt.setString(2,content);
                stmt.setInt(3,typeID);
                stmt.execute();
            }
        }
        System.out.println(num);
        try(PreparedStatement stmt = db.prepareStatement("INSERT INTO types_string (id,type_string) VALUES (?,?)")) {
            for(Map.Entry<String,Integer> typeEntry:typeIDGenerator){
                stmt.setInt(1,typeEntry.getValue());
                stmt.setString(2,typeEntry.getKey());
                stmt.execute();
            }
        }
        db.commit();

        try(
                final PreparedStatement stmt = db.prepareStatement("INSERT INTO triples_all (subject_id, predicate_id, object_id) VALUES (?,?,?)")
        ){
            RDFParser.create().source(rdfPath).lang(Lang.TURTLE).build().parse(new StreamRDF(){

                public void start() {

                }

                private Integer idOf(Node node){
                    String content = node.toString();
                    String typeContent;
                    if(node.isURI()){
                        typeContent = "uri";
                    }else if(node.isLiteral()){
                        typeContent = node.getLiteral().getDatatype().getURI();
                    }else if(node.isBlank()){
                        typeContent = "blank";
                    }else{
                        return null;
                    }
                    return contentIDGenerator.get(typeIDGenerator.get(typeContent).toString() + "|" + content);
                }

                public void triple(Triple triple) {
                    Integer subjectID = idOf(triple.getSubject());
                    Integer predicateID = idOf(triple.getPredicate());
                    Integer objectID = idOf(triple.getObject());
                    try {
                        stmt.setInt(1, subjectID);
                        stmt.setInt(2, predicateID);
                        stmt.setInt(3, objectID);
                        stmt.execute();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }

                public void quad(Quad quad) {

                }

                public void base(String s) {

                }

                public void prefix(String s, String s1) {

                }

                public void finish() {

                }
            });
        }
        db.commit();

        HashMap<Integer, HashSet<Integer>> typesByNodeID = new HashMap<>();

        try (
                PreparedStatement allTypeStmt = db.prepareStatement("SELECT subject_id, object_id FROM triples_all WHERE predicate_id=?");
        ) {
            allTypeStmt.setInt(1, rdfTypeID);
            ResultSet typeTriples = allTypeStmt.executeQuery();
            while (typeTriples.next()) {
                Integer subjectID = typeTriples.getInt(1);
                HashSet<Integer> types = typesByNodeID.get(subjectID);
                if (types == null) {
                    types = new HashSet<>();
                    typesByNodeID.put(subjectID, types);
                }
                types.add(typeTriples.getInt(2));
            }
        }
        // nodes_type:Store all  nodes and the type of the node
        try(
                PreparedStatement insertNodeLabelStmt = db.prepareStatement("INSERT INTO nodes_type (node_id, type_id) VALUES (?, ?)")
        ) {
            for (Map.Entry<Integer, HashSet<Integer>> nodeEntry: typesByNodeID.entrySet()) {
                Integer nodeID = nodeEntry.getKey();
                for (Integer nodeType: nodeEntry.getValue()) {
                    insertNodeLabelStmt.setInt(1, nodeID);
                    insertNodeLabelStmt.setInt(2, nodeType);
                    insertNodeLabelStmt.execute();
                }
            }
        }
        db.commit();

        try (
                //nodes_type：Count all non-repeating types
                PreparedStatement allTypeStmt = db.prepareStatement("SELECT DISTINCT type_id FROM nodes_type");
                //types_node table: stroe the type of the all entity
                PreparedStatement insertTypeStmt = db.prepareStatement("INSERT INTO types_node (types_id) VALUES (?)")
        ){
            ResultSet types = allTypeStmt.executeQuery();
            while(types.next()){
                Integer typesID = types.getInt(1);
                insertTypeStmt.setInt(1,typesID);
                insertTypeStmt.execute();
            }
        }
        db.commit();

        try (
                Statement indexingStmt = db.createStatement();
        ) {
            indexingStmt.execute("CREATE INDEX subject_id_index ON triples_all(subject_id)");
            indexingStmt.execute("CREATE INDEX object_id_index ON triples_all(object_id)");
            indexingStmt.execute("CREATE INDEX node_id_index ON nodes_type(node_id)");
        }
        db.commit();
    }
    // Remove "", eg:"12"-->12, 12.30"-->12.30
    private static void change_int_double(Connection db) throws Exception{

        try(
                PreparedStatement read_int_Stmt = db.prepareStatement("SELECT id,content FROM mapping WHERE string_type_id=3");
                PreparedStatement read_double_Stmt = db.prepareStatement("SELECT id,content FROM mapping WHERE string_type_id=4");
                PreparedStatement change_int_Stmt = db.prepareStatement("UPDATE mapping SET content=? WHERE id=?");
                PreparedStatement change_double_Stmt = db.prepareStatement("UPDATE mapping SET content=? WHERE id=?");)
        {
            //Remove "", eg:"12"-->12
            ResultSet read_int = read_int_Stmt.executeQuery();
            while(read_int.next()){
                int id = read_int.getInt(1);
                String old_content = read_int.getString(2);
                String [] arrstr = old_content.split( "\"" );
                int new_content =Integer.parseInt(arrstr[1]);
               // System.out.println(new_content);
                change_int_Stmt.setInt(1,new_content);
                change_int_Stmt.setInt(2,id);
                change_int_Stmt.execute();
            }
            //Remove "", eg:"12.30"-->12.30
            ResultSet read_double = read_double_Stmt.executeQuery();
            while (read_double.next()){
                int id = read_double.getInt(1);
                String old_content = read_double.getString(2);
                String [] arrstr = old_content.split("\"");
                double new_content = Double.parseDouble(arrstr[1]);
                change_double_Stmt.setDouble(1,new_content);
                change_double_Stmt.setInt(2,id);
                change_double_Stmt.execute();
            }


        }
        db.commit();
    }

    public static void main(String[] args) throws Exception {
        Connection db = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);
        db.setAutoCommit(false);
        build_table(db);
        change_int_double(db);

    }
}