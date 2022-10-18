package com.jonas.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

/**
 * HBaseTest
 *
 * @author shenjy
 * @version 1.0
 * @date 2022-09-08
 */
public class HBaseTest {
    private final String TEACHER_TABLE = "Teacher";

    @Test
    public void testCreateTable() {
        HBaseManager.deleteTable(TEACHER_TABLE);
        boolean result = HBaseManager.createTable(TEACHER_TABLE, Arrays.asList("baseInfo", "schoolInfo"));
        Assert.assertTrue(result);
    }

    @Test
    public void testGetScanner() {
        ResultScanner resultScanner = HBaseManager.scanTable(TEACHER_TABLE);
        Iterator<Result> resultIterator = resultScanner.iterator();
        while (resultIterator.hasNext()) {
            Result result = resultIterator.next();
            System.out.println(result);
        }
    }

    @Test
    public void testPutRow() {
        boolean result = HBaseManager.putRow(TEACHER_TABLE, "rowKey1", "baseInfo", "name", "Jonas");
        Assert.assertTrue(result);
    }

    @Test
    public void testGetRow() {
        Result result = HBaseManager.getRow(TEACHER_TABLE, "rowKey1");
        System.out.println(result);
    }

    @Test
    public void testGetCell() {
        String cellValue = HBaseManager.getCell(TEACHER_TABLE, "rowKey1", "baseInfo", "name");
        System.out.println(cellValue);
    }
}
