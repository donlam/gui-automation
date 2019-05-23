import java.sql.*;

/*
/**
 * Created by dlam
 * Built-in DB great tool we can leverage for doing some data analysis, especially for SI testing.
 * This may slow things down a bit as we will be storing data directly on disk, but will allow us to scale our test data
 * collection and generation to much bigger sets where we would be constraint by memory resource before
 */
public class DerbyHelper {

    private static String CONNECT_URL = "jdbc:derby:"+System.getProperty("user.dir")+"/tmp/QADerby;create=true";
    static Connection conn;

    //get a derby connection -- create a new QADerby database if one doesnt' already exist
    public static void getDerbyConnection() throws SQLException{
        conn = DriverManager.getConnection(CONNECT_URL);
    }

    public static void normalDBUsage(int id, String userName) throws SQLException {


        Statement statement = conn.createStatement();

        String insert = "insert into users values("+id+",'"+userName+"')";

        statement.executeUpdate(insert);
        ResultSet rs = statement.executeQuery("SELECT * FROM users");
        while(rs.next()){
            System.out.printf("%d\t%s\n",rs.getInt("id"), rs.getString("name"));
        }
    }


    public void setupTable() throws SQLException {
        Statement statement = conn.createStatement();
        try {
            statement.executeUpdate("Create table users (id int primary key, name varchar(30))");
        } catch (SQLException e) {
            if (e.getSQLState().contains("X0Y32")) {
                System.out.println("Table already exists");
            }
        }
    }


    static volatile int counter = 15;
    public static void main (String args []) throws Exception{

        DerbyHelper db = new DerbyHelper();
        db.getDerbyConnection();
        db.setupTable();
        for(int i=300; i<400; i++){
            db.normalDBUsage(i,"ABBY");
        }
}




}
