package com.boss.trainee.file.utils;

import com.obs.services.ObsClient;
import com.obs.services.model.CompleteMultipartUploadRequest;
import com.obs.services.model.CompleteMultipartUploadResult;
import com.obs.services.model.DownloadFileRequest;
import com.obs.services.model.DownloadFileResult;
import com.obs.services.model.GetObjectRequest;
import com.obs.services.model.InitiateMultipartUploadRequest;
import com.obs.services.model.InitiateMultipartUploadResult;
import com.obs.services.model.ObjectMetadata;
import com.obs.services.model.ObsObject;
import com.obs.services.model.PartEtag;
import com.obs.services.model.UploadFileRequest;
import com.obs.services.model.UploadPartRequest;
import com.obs.services.model.UploadPartResult;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author: Jianbinbing
 * @Date: 2020/7/15 16:56
 */
@Component
@Slf4j
public class FileOperationUtil {

    @Value("${obs.ak}")
    private String ak;

    @Value("${obs.sk}")
    private String sk;

    @Value("${obs.endPoint}")
    private String endPoint;

    @Value("${obs.bucketName}")
    private String bucketName;

    private final static long FILE_PART_SIZE = 1 * 1024 * 1024L;

    /**
     * 获取obs实例
     *
     * @return ObsClient
     */
    private ObsClient getObsClient() {
        return new ObsClient(ak, sk, endPoint);
    }

    /**
     * 获取新的文件名
     *
     * @param filePath
     * @return
     */
    private String getObjectName(String filePath) {
        int index = filePath.lastIndexOf("/");
        String originalFileName = filePath.substring(index + 1);
        String objectName = UUID.randomUUID().toString().replace("-", "") + "_" + originalFileName;
        return objectName;
    }

    /**
     * 整体上传文件
     *
     * @param filePath
     * @return
     */
    public boolean uploadFile(String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            return false;
        }
        String objectName = getObjectName(filePath);
        ObsClient obsClient = getObsClient();
        obsClient.putObject(bucketName, objectName, file);

        return true;
    }

    /**
     * 获取分段上传的全局唯一标识
     *
     * @param objectName
     * @return
     */
    private String getUploadId(String objectName) {
        ObsClient obsClient = getObsClient();
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.addUserMetadata("property", "property-value");
        metadata.setContentType("text/plain");
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName, objectName);
        request.setMetadata(metadata);
        //通知OBS初始化一个分段上传任务，并返回一个唯一标识
        InitiateMultipartUploadResult result = obsClient.initiateMultipartUpload(request);
        return result.getUploadId();
    }

    /**
     * 分段上传文件
     *
     * @param filePath
     * @return
     */
    public boolean uploadPart(String filePath) {
        String objectName = getObjectName(filePath);
        //开启分段上传任务，并获取唯一标识
        String uploadId = getUploadId(objectName);
        ObsClient obsClient = getObsClient();
        File file = new File(filePath);
        long fileSize = file.length();
        //计算文件段数
        long count = fileSize % FILE_PART_SIZE == 0 ? fileSize / FILE_PART_SIZE : fileSize / FILE_PART_SIZE + 1;
        List<PartEtag> partEtags = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            //开始位置
            long offset = i * FILE_PART_SIZE;
            //当前文件大小
            long currPartSize = (i + 1 == count) ? fileSize - offset : FILE_PART_SIZE;
            // 分段号
            int partNumber = i + 1;
            UploadPartRequest request = new UploadPartRequest(bucketName, objectName);
            //设置分段id
            request.setUploadId(uploadId);
            //设置分段号
            request.setPartNumber(partNumber);
            //设置文件分段大小
            request.setPartSize(currPartSize);
            request.setFile(file);
            //设置文件起始位置
            request.setOffset(offset);
            //分段上传
            UploadPartResult result = obsClient.uploadPart(request);
            PartEtag partEtag = new PartEtag(result.getEtag(), result.getPartNumber());
            partEtags.add(partEtag);
        }
        //合并分段的文件
        CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest(bucketName, objectName, uploadId, partEtags);
        obsClient.completeMultipartUpload(request);

        return true;
    }

    /**
     * 自定义多线程并发上传
     *
     * @param filePath
     * @return
     * @throws InterruptedException
     */
    public boolean upPart(String filePath) throws InterruptedException {
        String objectName = getObjectName(filePath);
        //开启分段上传任务，并获取唯一标识
        String uploadId = getUploadId(objectName);
        ObsClient obsClient = getObsClient();
        File file = new File(filePath);
        long fileSize = file.length();
        //计算文件段数
        long count = fileSize % FILE_PART_SIZE == 0 ? fileSize / FILE_PART_SIZE : fileSize / FILE_PART_SIZE + 1;
        List<PartEtag> partEtags = new ArrayList<>();
        // 初始化线程池
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < count; i++) {
            //开始位置
            long offset = i * FILE_PART_SIZE;
            //当前文件大小
            long currPartSize = (i + 1 == count) ? fileSize - offset : FILE_PART_SIZE;
            // 分段号
            int partNumber = i + 1;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    log.info(partNumber + "线程被调用");
                    UploadPartRequest request = new UploadPartRequest(bucketName, objectName);
                    //设置分段id
                    request.setUploadId(uploadId);
                    //设置分段号
                    request.setPartNumber(partNumber);
                    //设置文件分段大小
                    request.setPartSize(currPartSize);
                    request.setFile(file);
                    //设置文件起始位置
                    request.setOffset(offset);
                    //分段上传
                    UploadPartResult result = obsClient.uploadPart(request);
                    PartEtag partEtag = new PartEtag(result.getEtag(), result.getPartNumber());
                    partEtags.add(partEtag);
                }
            });

        }
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }
        //合并分段的文件
        CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest(bucketName, objectName, uploadId, partEtags);
        obsClient.completeMultipartUpload(request);

        return true;
    }


    /**
     * 断点续传上传
     *
     * @param filePath
     * @return
     */
    public boolean uploadBreakPoint(String filePath) {
        ObsClient obsClient = getObsClient();
        String objectName = getObjectName(filePath);
        UploadFileRequest request = new UploadFileRequest(bucketName, objectName);
        request.setUploadFile(filePath);
        //设置最大并发数
        request.setTaskNum(3);
        // 设置分段
        request.setPartSize(FILE_PART_SIZE);
        // 开启断点续传模式
        request.setEnableCheckpoint(true);
        CompleteMultipartUploadResult result = obsClient.uploadFile(request);

        return true;
    }


    /**
     * 获取文件对象-下载
     *
     * @param objectName
     * @return
     */
    public void downFile(String objectName, String filePath) throws IOException {
        ObsClient obsClient = getObsClient();
        ObsObject obsObject = obsClient.getObject(bucketName, objectName);
        if (obsObject != null) {
            InputStream input = obsObject.getObjectContent();
            byte[] b = new byte[1024];
            FileOutputStream fos = new FileOutputStream(new File(filePath));
            int len;
            while ((len = input.read(b)) != -1) {
                fos.write(b, 0, len);
            }
            fos.close();
            input.close();
        }
    }

    /**
     * 自定义多线程下载
     *
     * @param objectName
     * @param localPath
     * @throws InterruptedException
     */
    public void downPart(String objectName, String localPath) throws InterruptedException {
        ObsClient obsClient = getObsClient();
        //获取文件大小
        ObjectMetadata metadata = obsClient.getObjectMetadata(bucketName, objectName);
        long fileSize = metadata.getContentLength();
        ObsObject obsObject = obsClient.getObject(bucketName, objectName);
        // 初始化线程池
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        //获取文件流
        InputStream input = obsObject.getObjectContent();
        //计算文件段数
        long count = fileSize % FILE_PART_SIZE == 0 ? fileSize / FILE_PART_SIZE : fileSize / FILE_PART_SIZE + 1;
        for (int i = 0; i < count; i++) {
            Long startPos = i * FILE_PART_SIZE;
            Long endPos = (i == count) ? fileSize - 1 : (i + 1) * FILE_PART_SIZE - 1;
            //启动线程池的线程
            executorService.execute(new Runnable() {
                @SneakyThrows
                @Override
                public void run() {
                    log.info("线程被调用");
                    //调用SDK的范围下载
                    GetObjectRequest request = new GetObjectRequest(bucketName, objectName);
                    request.setRangeStart(startPos);
                    request.setRangeEnd(endPos);
                    //使用RandomAccessFile指定位置写入
                    RandomAccessFile randomAccessFile = new RandomAccessFile(localPath, "rw");
                    randomAccessFile.seek(startPos);
                    byte[] buf = new byte[4096];
                    int bytesRead = 0;
                    while ((bytesRead = input.read(buf)) != -1) {
                        randomAccessFile.write(buf, 0, bytesRead);
                    }
                    randomAccessFile.close();
                    input.close();
                }
            });
        }
        //关闭线程池
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }

    }

    /**
     * 断点续传下载
     *
     * @param objectName
     * @param filePath
     */
    public void downBreakPoint(String objectName, String filePath) {
        ObsClient obsClient = getObsClient();
        boolean exist = obsClient.doesObjectExist(bucketName, objectName);

        if (exist) {
            DownloadFileRequest request = new DownloadFileRequest(bucketName, objectName);
            request.setDownloadFile(filePath);
            request.setTaskNum(3);
            request.setPartSize(FILE_PART_SIZE);
            request.setEnableCheckpoint(true);
            DownloadFileResult result = obsClient.downloadFile(request);
        }

    }
}
