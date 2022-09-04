
//package com.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class SnowflakeJDBCDDLDML {
    public SnowflakeJDBCDDLDML() {
    }

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("user", "<my-user-id>");
        properties.put("password", "<my-password>");
        properties.put("account", "vq1234.ap-southeast-2");
        properties.put("warehouse", "COMPUTE_WH");
        properties.put("db", "TEST_DB");
        properties.put("schema", "TEST_SCHEMA");
        properties.put("role", "SYSADMIN");
        
        //jdbc URL
        String jdbcUrl = "jdbc:snowflake://vq1234.ap-southeast-2.snowflakecomputing.com/";

        //ddl statement
        String sqlQuery = "create or replace table jdbc_demo02 (id number, name text ) ";

        System.out.println("\tStarting the Snowflake Java JDBC Connection Program");

        try {
            Connection connection = DriverManager.getConnection(jdbcUrl, properties);
            Statement stmt = connection.createStatement();
            int positiveInt = stmt.executeUpdate(sqlQuery);
            System.out.println("\tConnection established, connection id : " + connection);
            System.out.println("\tGot the statement object, object-id : " + stmt);
            System.out.println("\tDDL statement executed : " + positiveInt);

            //number of records to be inserted
            int recordInsert = 10;

            for(int i = 0; i < recordInsert; ++i) {
                String dmlQuery = "insert into jdbc_demo02 values (" + i + ", 'Name-" + i + "')";
                System.out.println("The query is:" + dmlQuery);
                int insertCnt = stmt.executeUpdate(dmlQuery);
                System.out.println("\t(" + i + ") Row inserted: " + insertCnt);
            }
        } catch (SQLException var11) {
            var11.printStackTrace();
        }

        System.out.println("\t----------------------------------------");
        System.out.println("\tProgram executed successfully");
    }
}
