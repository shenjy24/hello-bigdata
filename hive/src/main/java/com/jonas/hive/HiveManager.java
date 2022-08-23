package com.jonas.hive;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveManager {

    private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String URL = "jdbc:hive2://localhost:10000/hive";
    private static final String USER = "jia";
    private static final String PASSWORD = "123456";

    static {
        try {
            Class.forName(DRIVER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createDatabase() throws SQLException {
        String sql = "create database test_db";
        try (Connection connection = getConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }
    }

    public static List<String> showDatabases() throws SQLException {
        List<String> databases = new ArrayList<>();
        String sql = "show databases";
        try (Connection connection = getConnection()) {
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    databases.add(resultSet.getString(1));
                }
            }
        }
        return databases;
    }

    public static void createTable() throws SQLException {
        String sql = "create table user_tb(id int, name string) row format delimited fields terminated by ','";
        try (Connection connection = getConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }
    }

    public static List<String> showTables() throws SQLException {
        List<String> tables = new ArrayList<>();
        String sql = "show tables";
        try (Connection connection = getConnection()) {
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    tables.add(resultSet.getString(1));
                }
            }
        }
        return tables;
    }

    public static Map<String, String> descTable(String table) throws SQLException {
        Map<String, String> info = new HashMap<>();
        String sql = "desc " + table;
        try (Connection connection = getConnection()) {
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    info.put(resultSet.getString(1), resultSet.getString(2));
                }
            }
        }
        return info;
    }

    public static void loadData(String file) throws SQLException {
        String sql = String.format("load data local inpath '%s' overwrite into table user_tb", file);
        try (Connection connection = getConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }
    }

    public static Map<String, String> select() throws SQLException {
        Map<String, String> res = new HashMap<>();
        String sql = "select * from user_tb";
        try (Connection connection = getConnection()) {
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    res.put(resultSet.getString("id"), resultSet.getString("name"));
                }
            }
        }
        return res;
    }

    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }
}
