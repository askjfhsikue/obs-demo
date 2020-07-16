package com.boss.trainee.file.utils;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

/**
 * @author: Jianbinbing
 * @Date: 2020/7/15 19:55
 */
@SpringBootTest
public class FileOperationUtilTest {
    @Autowired
    private FileOperationUtil fileOperationUtil;


    @Test
    public void upload() {
        String filePath = "F:/huaweiyun/test/test.txt";

        fileOperationUtil.uploadFile(filePath);
    }

    @Test
    public void upPart() throws InterruptedException {
        String filePath = "F:/huaweiyun/test/指导手册.pdf";
        fileOperationUtil.upPart(filePath);
    }

    @Test
    public void upBreak() {
        String filePath = "F:/huaweiyun/test/指导手册.pdf";
        fileOperationUtil.uploadBreakPoint(filePath);
    }

    @Test
    public void getFile() throws IOException {
        String objectName = "f9a49eac24b34a04a63d281e75575a25_指导手册.pdf";
        String filePath = "F:/huaweiyun/test/res.pdf";
        fileOperationUtil.downFile(objectName, filePath);
    }

    @Test
    public void getPoint() {
        String objectName = "f9a49eac24b34a04a63d281e75575a25_指导手册.pdf";
        String filePath = "F:/huaweiyun/test/res2.pdf";
        fileOperationUtil.downBreakPoint(objectName, filePath);
    }

    @Test
    public void getPartFile() throws InterruptedException {
        String objectName = "f9a49eac24b34a04a63d281e75575a25_指导手册.pdf";
        String filePath = "F:/huaweiyun/test/res3.pdf";
        fileOperationUtil.downPart(objectName, filePath);
    }

}
