package com.jonas.hadoop;

import com.jonas.hadoop.hdfs.HDFSManager;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class HDFSTest {

    @Test
    public void testMkdir() {
        HDFSManager.mkdir("/user/jia");
    }

    @Test
    public void testCreateFile() {
        HDFSManager.createFile("/user/jia/test", "hello hadoop!\n你好，hadoop！");
    }

    @Test
    public void testReadFile() {
        String content = HDFSManager.readFile("/user/jia/test");
        System.out.println(content);
    }

    @Test
    public void testRename() {
        boolean result1 = HDFSManager.rename("/user/jia", "/user/jonas");
        boolean result2 = HDFSManager.rename("/user/jonas", "/user/jia");
        Assert.assertTrue(result1 & result2);
    }

    @Test
    public void testDelete() {
        boolean result = HDFSManager.delete("/user/test");
        Assert.assertTrue(result);
    }

    @Test
    public void testUpload() {
        boolean result = HDFSManager.upload("C:\\Users\\Desktop\\hdfs", "/user/jia");
        Assert.assertTrue(result);
    }

    @Test
    public void testListFile() {
        List<FileStatus> fileStatuses = HDFSManager.listFileStatus("/user/jia");
        System.out.println(fileStatuses);
    }

    @Test
    public void testListLocatedFileStatus() {
        List<LocatedFileStatus> fileStatuses = HDFSManager.listLocatedFileStatus("/user/jia", true);
        System.out.println(fileStatuses);
    }

    @Test
    public void testListFileBlockLocation() {
        List<BlockLocation> blockLocations = HDFSManager.listFileBlockLocation("/user/jia/test");
        System.out.println(blockLocations);
    }
}
