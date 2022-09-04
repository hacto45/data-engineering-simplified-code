//add package as per your package structure
//package com.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

public class IngestRDBS2Snowflake {
    public IngestRDBS2Snowflake() {
    }

    public static void main(String[] args) {
        //step-1: Get all the table names
        ArrayList<String> tableNames = getTableNames(getSourceDBConnection());

        //step-2: based on table names, build all the ddls
        ArrayList<String> tableDDLs = createDDLForSnowflake(getSourceDBConnection(), tableNames);

        //step-3: based on table names, fetch column names and types and build prepared statements
        ArrayList<String> prepStmt = createPreparedStmtForSnowflake(getSourceDBConnection(), tableNames);

        //step-4 based on table names, fetch all data and build data in memory
        HashMap<String, ArrayList<ArrayList<String>>> insertDataMap = getInsertData(getSourceDBConnection(), tableNames);

        //step-5 now run the ddl in snowflake environment
        createTablesInSnowflake(getSnowflakeConnection(), tableDDLs);

        //step-6 load data using insert batch statement
        insertDataInSnowflake(getSnowflakeConnection(), tableNames, prepStmt, insertDataMap);
        
    }

    public static Connection getSourceDBConnection() {
        String pgUser = "<postgres-user-id>";
        String pgPwd = "<postgres-pwd";
        String jdbcUrl = "jdbc:postgresql://host-name:5432/db-name";
        Connection sourceDBConnection = null;
        String var4 = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'";

        try {
            Class.forName("org.postgresql.Driver");
            sourceDBConnection = DriverManager.getConnection(jdbcUrl, pgUser, pgPwd);
        } catch (Exception var6) {
            var6.printStackTrace();
            System.exit(1);
        }

        return sourceDBConnection;
    }

    public static ArrayList<String> getTableNames(Connection srcDBConnection) {
        System.out.println("\n\t\t-----------------------------------");
        System.out.println("\t\tStep 1: Getting Table Names From Postgres");
        System.out.println("\t\t-----------------------------------");
        ArrayList<String> tblNames = new ArrayList();
        String sqlGetTable = "SELECT table_name FROM information_schema.tables  WHERE table_schema='public' and  table_name not in ('pg_stat_statements')";

        try {
            Statement stmt = srcDBConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sqlGetTable);

            while(rs.next()) {
                String tblName = rs.getString("table_name");
                System.out.println("\t\t\tTable Name: " + tblName);
                tblNames.add(tblName);
            }

            stmt.close();
            rs.close();
            srcDBConnection.close();
        } catch (Exception var6) {
            var6.printStackTrace();
            System.exit(1);
        }

        System.out.println("\n\t\tStep 1: Completed");
        return tblNames;
    }

    public static ArrayList<String> createDDLForSnowflake(Connection srcDBConnection, ArrayList<String> tblNames) {
        System.out.println("\n\t\t-----------------------------------");
        System.out.println("\t\tStep 2: Getting Table DDLs Statement From Postgres");
        System.out.println("\t\t-----------------------------------");
        ArrayList ddlSqls = new ArrayList();

        try {
            Iterator var3 = tblNames.iterator();

            while(var3.hasNext()) {
                String table = (String)var3.next();
                String finalTableDDL = "create or replace table " + table + " (";
                String ddlSql = "SELECT * FROM information_schema.columns  WHERE table_schema = 'public' AND  table_name   = '" + table + "'  order by ordinal_position";
                Statement stmt = srcDBConnection.createStatement();

                ResultSet rsDDL;
                String columnName;
                String columnType;
                for(rsDDL = stmt.executeQuery(ddlSql); rsDDL.next(); finalTableDDL = finalTableDDL + columnName + " " + columnType + ",") {
                    columnName = rsDDL.getString("column_name");
                    columnType = rsDDL.getString("data_type");
                }

                String var10000 = finalTableDDL.substring(0, finalTableDDL.length() - 1);
                finalTableDDL = var10000 + " );";
                System.out.println("\t\t\tDDL for table " + table + " : " + finalTableDDL);
                ddlSqls.add(finalTableDDL);
                stmt.close();
                rsDDL.close();
            }

            srcDBConnection.close();
        } catch (Exception var11) {
            var11.printStackTrace();
            System.exit(1);
        }

        System.out.println("\n\t\tStep 2: Completed");
        return ddlSqls;
    }

    public static ArrayList<String> createPreparedStmtForSnowflake(Connection srcDBConnection, ArrayList<String> tblNames) {
        System.out.println("\n\t\t-----------------------------------");
        System.out.println("\t\tStep 3: Getting Prepared Statement From Postgres");
        System.out.println("\t\t-----------------------------------");
        ArrayList prepStmtSqls = new ArrayList();

        try {
            Iterator var3 = tblNames.iterator();

            while(var3.hasNext()) {
                String table = (String)var3.next();
                String finalPrepStmt = "insert into " + table + " ( ";
                String finalPrepStmtValues = " values ( ";
                String ddlSql = "SELECT column_name FROM information_schema.columns  WHERE table_schema = 'public' AND  table_name   = '" + table + "'  order by ordinal_position";
                Statement stmt = srcDBConnection.createStatement();

                ResultSet rsDDL;
                for(rsDDL = stmt.executeQuery(ddlSql); rsDDL.next(); finalPrepStmtValues = finalPrepStmtValues + "? ,") {
                    String columnName = rsDDL.getString("column_name");
                    finalPrepStmt = finalPrepStmt + columnName + " ,";
                }

                String var10000 = finalPrepStmt.substring(0, finalPrepStmt.length() - 1);
                finalPrepStmt = var10000 + " )";
                var10000 = finalPrepStmtValues.substring(0, finalPrepStmtValues.length() - 1);
                finalPrepStmtValues = var10000 + " )";
                prepStmtSqls.add(finalPrepStmt + finalPrepStmtValues);
                System.out.println("\t\t\tPrepared Stmt for table " + table + " : " + finalPrepStmt + finalPrepStmtValues);
                stmt.close();
                rsDDL.close();
            }

            srcDBConnection.close();
        } catch (Exception var11) {
            var11.printStackTrace();
            System.exit(1);
        }

        System.out.println("\n\t\tStep 3: Completed");
        return prepStmtSqls;
    }

    public static HashMap<String, ArrayList<ArrayList<String>>> getInsertData(Connection srcDBConnection, ArrayList<String> tblNames) {
        System.out.println("\n\t\t-----------------------------------");
        System.out.println("\t\tStep 4: Getting Data From Postgres Tables as Java Object");
        System.out.println("\t\t-----------------------------------");
        HashMap tableDataMap = new HashMap();

        try {
            Iterator var3 = tblNames.iterator();

            while(var3.hasNext()) {
                String table = (String)var3.next();
                ArrayList<ArrayList<String>> tblData = new ArrayList();
                String selectSQL = "SELECT * FROM " + table;
                Statement stmt = srcDBConnection.createStatement();
                ResultSet rsDDL = stmt.executeQuery(selectSQL);

                while(rsDDL.next()) {
                    ArrayList<String> rowData = new ArrayList();
                    rowData.add(rsDDL.getString(1));
                    rowData.add(rsDDL.getString(2));
                    tblData.add(rowData);
                }

                tableDataMap.put(table, tblData);
                System.out.println("\t\t\tRow counts for table " + table + " : " + tblData.size());
                stmt.close();
                rsDDL.close();
            }

            srcDBConnection.close();
        } catch (Exception var10) {
            var10.printStackTrace();
            System.exit(1);
        }

        System.out.println("\n\t\tStep 4: Completed");
        return tableDataMap;
    }

    public static Connection getSnowflakeConnection() {
        Connection targetConnection = null;
        Properties properties = new Properties();
        properties.put("user", "<snowflake-user-id>");
        properties.put("password", "<snowflake-pwd>");
        properties.put("warehouse", "COMPUTE_WH");
        properties.put("db", "DATA_WAREHOUSE_QA");
        properties.put("schema", "STAGE_SCHEMA");
        properties.put("role", "SYSADMIN");
        String jdbcUrl = "jdbc:snowflake://vq1234.ap-southeast-2.snowflakecomputing.com/";

        try {
            targetConnection = DriverManager.getConnection(jdbcUrl, properties);
        } catch (SQLException var4) {
            var4.printStackTrace();
            System.exit(1);
        }

        return targetConnection;
    }

    public static void createTablesInSnowflake(Connection targetDBConnection, ArrayList<String> tableDDLs) {
        System.out.println("\n\t\t-----------------------------------");
        System.out.println("\t\tStep 5:All DDLs executed in snowflake");
        System.out.println("\t\t-----------------------------------");

        try {
            Statement stmt = targetDBConnection.createStatement();
            Iterator var3 = tableDDLs.iterator();

            while(var3.hasNext()) {
                String ddlStmt = (String)var3.next();
                System.out.println("\t\t\tDDL Statement is : \n\t\t\t" + ddlStmt);
                stmt.executeUpdate(ddlStmt);
            }

            stmt.close();
            targetDBConnection.close();
        } catch (Exception var5) {
            var5.printStackTrace();
            System.exit(1);
        }

        System.out.println("\n\t\tStep 5: Completed");
    }

    public static void insertDataInSnowflake(Connection targetDBConnection, ArrayList<String> tableNames, ArrayList<String> prepStmt, HashMap<String, ArrayList<ArrayList<String>>> insertDataMap) {
        try {
            System.out.println("\n\t\t-----------------------------------");
            System.out.println("\t\tStep 6: Snowflake Batch Insert Operation Method");
            System.out.println("\t\t-----------------------------------");
            targetDBConnection.setAutoCommit(false);
            int index = 0;

            for(Iterator var5 = prepStmt.iterator(); var5.hasNext(); ++index) {
                String insertStmt = (String)var5.next();
                PreparedStatement pstmt = targetDBConnection.prepareStatement(insertStmt);
                String tblName = (String)tableNames.get(index);
                System.out.println("\t\t\tFor Table: " + tblName + " \n\t\t\tStmt = " + insertStmt);
                ArrayList<ArrayList<String>> dataSet = (ArrayList)insertDataMap.get(tblName);
                Iterator var10 = dataSet.iterator();

                while(var10.hasNext()) {
                    ArrayList<String> row = (ArrayList)var10.next();
                    pstmt.setString(1, (String)row.get(0));
                    pstmt.setString(2, (String)row.get(1));
                    pstmt.addBatch();
                }

                int[] count = pstmt.executeBatch();
                targetDBConnection.commit();
                System.out.println("\t\t\tBatch Commit Is Done..\n");
                pstmt.close();
            }

            targetDBConnection.close();
        } catch (Exception var12) {
            var12.printStackTrace();
            System.exit(1);
        }

        System.out.println("\n\t\tStep 6: Completed");
        System.out.println("\t\t-----------------------------------");
    }
}
