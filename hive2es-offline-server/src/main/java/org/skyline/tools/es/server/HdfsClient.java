package org.skyline.tools.es.server;

import com.google.common.collect.Lists;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import javax.annotation.PostConstruct;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.springframework.stereotype.Component;

/**
 * @author Sean Liu
 * @date 2019-11-26
 */
@Component
@Slf4j
public class HdfsClient {

    private FileSystem fs;

    @PostConstruct
    public void init() {
        try {
            fs = FileSystem.get(getConfiguration());
        } catch (IOException e) {
            log.error("Create hadoop FileSystem failed", e);
        }
    }

    private Configuration getConfiguration() {
        Configuration conf = new Configuration();
        return conf;
    }


    public List<String> listCompletedFiles(String path) {
        List<String> results = Lists.newArrayList();
        try {
            if (fs.exists(new Path(path))) {
                FileStatus[] fileStatuses = fs.listStatus(new Path(path));
                fileStatuses = Lists.newArrayList(fileStatuses).stream()
                        .filter(x -> x.getLen() > 0 && !x.getPath().getName().endsWith("_tmp"))
                        .toArray(FileStatus[]::new);
                Path[] children = FileUtil.stat2Paths(fileStatuses);
                for (Path child : children) {
                    results.add(child.toString());
                }
            }
        } catch (IOException e) {
            log.error("List folder error", e);
        }
        return results;
    }

    public List<String> listFiles(String path) {
        List<String> results = Lists.newArrayList();
        try {
            Path[] children = FileUtil.stat2Paths(fs.listStatus(new Path(path)));
            for (Path child : children) {
                results.add(child.toString());
            }
        } catch (IOException e) {
            log.error("List folder error", e);
        }
        return results;
    }

    public void downloadFolder(String srcPath, String dstPath) throws IOException {
        File dstDir = new File(dstPath);
        if (!dstDir.exists()) {
            dstDir.mkdirs();
        }
        FileStatus[] srcFileStatus = fs.listStatus(new Path(srcPath));
        Path[] srcFilePath = FileUtil.stat2Paths(srcFileStatus);
        for (int i = 0; i < srcFilePath.length; i++) {
            String srcFile = srcFilePath[i].toString();
            int fileNamePosi = srcFile.lastIndexOf('/');
            String fileName = srcFile.substring(fileNamePosi + 1);
            download(srcPath + '/' + fileName, dstPath + '/' + fileName);
        }
    }

    public void download(String srcPath, String dstPath) throws IOException {
        if (fs.isFile(new Path(srcPath))) {
            downloadFile(srcPath, dstPath);
        } else {
            downloadFolder(srcPath, dstPath);
        }
    }

    public void downloadFile(String srcPath, String dstPath) throws IOException {
        log.info("Download from hdfs {} to local {}", srcPath, dstPath);
        fs.copyToLocalFile(false, new Path(srcPath), new Path(dstPath), true);
    }

    public void downloadAndUnzipFile(String srcPath, String dstPath) throws IOException {
        log.info("Download from hdfs {} to local {}", srcPath, dstPath);
        try (
                ZipArchiveInputStream in = new ZipArchiveInputStream(
                        fs.open(new Path(srcPath), 1024 * 1024))
        ) {
            ArchiveEntry entry;
            while ((entry = in.getNextEntry()) != null) {
                if (!in.canReadEntryData(entry)) {
                    // log something?
                    continue;
                }

                File f = Paths.get(dstPath).resolve(entry.getName()).toFile();
                if (entry.isDirectory()) {
                    if (!f.isDirectory() && !f.mkdirs()) {
                        throw new IOException("Failed to create directory " + f);
                    }
                } else {
                    File parent = f.getParentFile();
                    if (!parent.isDirectory() && !parent.mkdirs()) {
                        throw new IOException("Failed to create directory " + parent);
                    }
                    try (OutputStream o = new BufferedOutputStream(Files.newOutputStream(f.toPath()),
                            4 * 1024 * 1024)) {
                        IOUtils.copy(in, o);
                    }
                }
            }
        }
    }

    public String largestFileInDirectory(String dir) throws IOException {

        RemoteIterator<LocatedFileStatus> files = fs
                .listFiles(new Path(dir), false);
        long max = 0;
        String path = "";
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            if (file.getBlockSize() > max) {
                max = file.getBlockSize();
                path = file.getPath().toString();
            }
        }
        return path;
    }

    public String readMappingJson(String filePath) throws IOException, InterruptedException {
        Utils.downloadHdfsFile(filePath);
        log.info("download file finished");
        String mappingStr = new String(Files.readAllBytes(Paths.get("/appcom/home/hadoop/DSP-HBD-JAD-HIVE2ES-OFFLINE-SERVER/apps/mapping.json")));

        return mappingStr;
    }


}
