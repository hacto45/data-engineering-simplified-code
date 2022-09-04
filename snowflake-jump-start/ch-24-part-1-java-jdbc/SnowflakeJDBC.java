//change package as per your requirement.
//it will work without package also.
//package com.util;

//java.sql library for all connection objects
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

//java util library
import java.util.Properties;

//class definition
public class SnowflakeJDBC {

    //default constructor
    public SnowflakeJDBC() {
    }

    //entry main method
    public static void main(String[] args) {

        //properties object
        Properties properties = new Properties();

        //setting properties
        properties.put("user", "<my-snowflake-user-id>");
        properties.put("password", "<my-password>");
        properties.put("account", "vq1234.ap-southeast-2"); //account-id followed by cloud region.
        properties.put("warehouse", "COMPUTE_WH");
        properties.put("db", "TEST_DB");
        properties.put("schema", "TEST_SCHEMA");
        properties.put("role", "SYSADMIN");

        //change this below URL as per your snowflake instance
        String jdbcUrl = "jdbc:snowflake://vq1234.ap-southeast-2.snowflakecomputing.com/";

        //change this select statement, but make sure the logic below is hard coded for now.
        String selectSQL = "SELECT * FROM  TEST_DB.TEST_SCHEMA.Employees";

        //try-catch block
        try {
            Connection connection = DriverManager.getConnection(jdbcUrl, properties);
            System.out.println("\tConnection established, connection id : " + connection);
            Statement stmt = connection.createStatement();
            System.out.println("\tGot the statement object, object-id : " + stmt);
            ResultSet rs = stmt.executeQuery(selectSQL);
            System.out.println("\tGot the result set object, object-id : " + rs);
            System.out.println("\t----------------------------------------");

            while(rs.next()) {
                //following rs.getXXX should also change as per your select query
                System.out.println(" \tEmployee ID: " + rs.getInt("ID"));
                System.out.println(" \tEmployee Age: " + rs.getInt("AGE"));
                System.out.println(" \tEmployee First: " + rs.getString("FIRST"));
                System.out.println(" \tEmployee Last: " + rs.getString("LAST"));
            }
        } catch (SQLException exp) {
            exp.printStackTrace();
        }

        System.out.println("\t----------------------------------------");
        System.out.println("\tProgram executed successfully");
    }
}
