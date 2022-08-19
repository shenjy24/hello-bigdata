package com.jonas;

import lombok.SneakyThrows;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class AppTest {

    @Test
    @SneakyThrows
    public void testCreateDatabase() {
        HiveManager.createDatabase();
    }

    @Test
    @SneakyThrows
    public void showDatabases() {
        List<String> databases = HiveManager.showDatabases();
        System.out.println(databases);
    }

    @Test
    @SneakyThrows
    public void createTable() {
        HiveManager.createTable();
    }

    @Test
    @SneakyThrows
    public void showTables() {
        List<String> tables = HiveManager.showTables();
        System.out.println(tables);
    }

    @Test
    @SneakyThrows
    public void descTable() {
        Map<String, String> info = HiveManager.descTable("user_tb");
        System.out.println(info);
    }

    @Test
    @SneakyThrows
    public void loadData() {
        String file = "D:\\java\\jackal-bigdata\\hive\\src\\main\\resources\\user.txt";
        HiveManager.loadData(file);
    }

    @Test
    @SneakyThrows
    public void select() {
        Map<String, String> info = HiveManager.select();
        System.out.println(info);
    }
}
