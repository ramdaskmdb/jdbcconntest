package com.databricks.norfolk;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class RKMQuery {


    // Connect to your database.
    // Replace server name, username, and password with your credentials
    public static void main(String[] args) {
        if(args.length < 7)
        {
            System.out.println("Usage is RKMQuery <host> <port> <httppath> <database> <userid> <password> <query>" );
        }

        try 
        {
           //Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();
           Class.forName("com.databricks.client.jdbc.Driver");
        }
        catch(ClassNotFoundException ex) {
           System.out.println("Error: unable to load driver class!");
           System.exit(1);
	}


        String host = args[0];
        String port = args[1];
        String db = args[3];
        String httpPath = args[2];
        String userid = args[4];
        String password = args[5];
        String selectSql = args[6];

        String connectionUrl =
                //"jdbc:oracle:thin:@" + host + ":" + port + "/" + db;
		"jdbc:databricks://" + host + ":" + port + "/" + db + ";transportMode=http;ssl=1;AuthMech=3;httpPath=" + httpPath;
        System.out.println("Connection:" + connectionUrl);
        System.out.println("with:" + userid);

        ResultSet resultSet = null;

        try{ 
		Connection connection = DriverManager.getConnection(connectionUrl, userid, password);
                Statement statement = connection.createStatement();
            System.out.println("Executing:" + selectSql);
            // Create and execute a SELECT SQL statement.
            resultSet = statement.executeQuery(selectSql);
            ResultSetMetaData rsmd = resultSet.getMetaData();
            System.out.println("Completed execution of SQL. Getting responses");
            while (resultSet.next()) {
                for(int i = 0; i<rsmd.getColumnCount(); i++)
                {
                    System.out.print(resultSet.getString(i+1) + ",");
                }
                System.out.println("____");
            }
	    statement.close();
	    connection.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
