package org.skyline.tools.es.server;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.index.translog.Translog;
import org.skyline.tools.es.server.config.ThreadPoolConfig.VisibleThreadPoolTaskExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * @author Sean Liu
 * @date 2019-11-26
 */
@Slf4j
@Service
public class IndexBuilder {

    @Value("#{'${workDir}'.split(',')}")
    private String[] workDirs;

    @Autowired
    private HdfsClient hdfsClient;

    @Autowired
    private ESClient esClient;

    @Autowired
    private VisibleThreadPoolTaskExecutor processTaskExecutor;

    @Autowired
    private VisibleThreadPoolTaskExecutor downloadTaskExecutor;

    private static final String STATE_DIR = "_state";

    private static final String SHARD_STATE = "_shard_state";

    private Map<String, Boolean> completedIndices = Maps.newConcurrentMap();

    private String INDEX_FILE = "index";

    private String TLOG_UUID = "";

    private String TRANSLOG_PATH = "translog";

    private String TRANSLOG_TLOG_FILE = "translog-1.tolg";

    private String TRANSLOG_GENERATION_KEY = "1";

    public boolean build(Map<String, List<String>> idToShards, JSONObject configData) {

        String hdfsWorkDir = configData.getString("hdfsWorkDir");
        String indexName = configData.getString("indexName");
        log.info("start build: "+indexName+":"+hdfsWorkDir);
        //TODO no need localstatdir
        Path localStateDir = Paths.get(Utils.mostFreeDir(workDirs), indexName);
        if (downloadAndMergeAllShards(idToShards, hdfsWorkDir, indexName, localStateDir)) {
            try {
                FileUtils.deleteDirectory(localStateDir.toFile());
                log.info("Delete state file {}", localStateDir.resolve(STATE_DIR));
                completedIndices.remove(indexName);
            } catch (Exception e) {
                log.error("delete state file error", e);
            }
            return true;
        }
        return false;
    }

    private boolean downloadAndMergeAllShards(Map<String, List<String>> idToShards,
                                              String hdfsWorkDir, String indexName, Path localStateDir) {
        Set<String> chosenPaths = Sets.newConcurrentHashSet();
        int shardNum = idToShards.values().stream().mapToInt(x -> x.size()).sum();
//        CountDownLatch allShardsLatch = new CountDownLatch(shardNum);
        idToShards.entrySet().forEach(entry -> {
            String nodeId = entry.getKey();
            List<String> shards = entry.getValue();
            downloadAndMergeByNode(nodeId, shards, hdfsWorkDir, indexName, localStateDir, chosenPaths);
        });
        log.info("Wait all partition download and merge");
//        try {
//            allShardsLatch.await();
//        } catch (InterruptedException e) {
//            log.info("Wait all partition download and merge error", e);
//        }
        return true;
    }

    private void downloadAndMergeByNode(String nodeId, List<String> shards,
                                        String hdfsWorkDir,
                                        String indexName, Path localStateDir, Set<String> chosenPaths) {
        log.info("Submit download and merge index task for node [{}]", nodeId);
        log.info("assion shards: "+shards.size()+":"+shards);
//        shards.forEach(shardId -> processTaskExecutor.submit(() -> {
        for (String shardId : shards) {
            ///data/data03/es/data/paic-elasticsearch/nodes/0
            ///data/data03/es/data/paic-elasticsearch/nodes/0/indices/stock_20200324/0/index
            log.info(indexName+":"+shardId+"-"+Integer.getInteger(shardId));
            String dataPath = esClient.getShardDataPath(indexName, new Integer(shardId));
            log.info("es's data path is: "+dataPath);
            // 选择最空闲的一个路径放索引
//            chosenPaths.add(dataPath);
            log.info("Most free data dir is {}", dataPath);

            String srcPath = Paths.get(hdfsWorkDir, indexName, shardId).toString();
            log.info("hdfs path is: "+srcPath);
            String workDir = Utils.sameDiskDir(workDirs, dataPath);
            String destPath = Paths.get(workDir, indexName, shardId).toString();
            log.info("Chosen tmpwork dir is {}", workDir);

            log.info("Build index shard [{}] for node [{}]", shardId, nodeId);
            try {
                // onlu solve segment file
                downloadAndUnzipShard(srcPath, destPath, indexName);

                log.info("Merge index bundle in dir[{}] ", destPath);
                String finalIndexPath = mergeIndex(destPath);
                if(finalIndexPath.equals("1")){
                    return ;
                }
//                destPath
                moveLuceneToESDataDir(indexName, shardId, dataPath, finalIndexPath);
            } catch (IOException e) {
                log.error(
                        "Build index bundle from hdfs[" + srcPath + "] failed", e);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
//                allShardsLatch.countDown();
//                try {
//                    log.info("Delete shard tmp dir {}", destPath);
//                    FileUtils.deleteDirectory(new File(destPath));
//                } catch (IOException e) {
//                    log.error("Delete shard tmp dir error", e);
//                }
            }
        }
    }

    public void downloadAndUnzipShard(String srcPath, String destPath, String indexName) {
        log.info("Download and unzip index bundle from hdfs[{}] to local[{}] start", srcPath, destPath);
        Set<String> processedPaths = Sets.newHashSet();
        //needn't wait
//        List<CountDownLatch> latches = Lists.newArrayList();
        File dstDir = new File(destPath);
        if (!dstDir.exists()) {
            log.info("Dest path not exists and create it {}", destPath);
            dstDir.mkdirs();
        }
        while (true) {
            List<String> paths = hdfsClient.listCompletedFiles(srcPath);
            paths.removeAll(processedPaths);
            if (paths.size() == 0) {
                if (indexCompleted(indexName)) {
                    log.info("Index completed and all partition submitted");
                    break;
                } else {
                    log.info("Wait index complete for 10s...");
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        log.info("Wait index complete error", e);
                    }
                }
            } else {
                log.info("Got new path size : {} and processed path size:{}", paths.size(),
                        processedPaths.size());
//                CountDownLatch latch = new CountDownLatch(paths.size());
//                latches.add(latch);
                paths.forEach(srcFile -> {
                    String fileName = srcFile.substring(srcFile.lastIndexOf('/') + 1);
                    String from = srcPath + '/' + fileName;
                    String to = destPath;
                    submitDownloadAndUnzipShardPartitionTask(from, to);
                });

                processedPaths.addAll(paths);
            }
        }
        log.info("Wait shard partition download and unzip");
        //neend't wait
//        try {
//            for (CountDownLatch latch : latches) {
//                latch.await();
//            }
//        } catch (InterruptedException e) {
//            log.info("Wait shard partition download and unzip error", e);
//        }
        log.info("Download and unzip index bundle from hdfs[{}] to local[{}] end", srcPath, destPath);
    }

    private void submitDownloadAndUnzipShardPartitionTask(String srcPath, String destPath) {
        log.info("Submit download and unzip task from {} to {}", srcPath, destPath);
        downloadTaskExecutor.execute(() -> {
            log.info("Download and unzip start with thread pool info : ");
            downloadTaskExecutor.showThreadPoolInfo();
            try {
                hdfsClient.downloadAndUnzipFile(srcPath, destPath);
            } catch (IOException e) {
                log.error("Download index file " + srcPath + " error", e);
            } finally {
                log.info("Download and unzip end with thread pool info : ");
                downloadTaskExecutor.showThreadPoolInfo();
            }
        });
    }

    //改成之移动lucene文件,并更改lucene的user data
    private void moveLuceneToESDataDir(String indexName, String shardId,
                                                       String dataPath, String finalIndexPath) throws IOException, InterruptedException {
        // 从临时目录把lucene文件移到es的分片索引目录下面
        //shard中，0/index 这一级移到目录下，名字不用改
        Path from = Paths.get(finalIndexPath+"/"+INDEX_FILE);
        Path to = Paths.get(dataPath, "indices", indexName, shardId,INDEX_FILE);
        //删掉原lucene文件
        Utils.setPermissionRecursive(to);
        Files.delete(to);
//        if (!Files.exists(to)) {
//            log.info("Create index folder : {}", to.getParent());
//            Files.createDirectories(to.getParent());
//        }

        log.info("Move index from {} to {}", from, to);
        Files.move(from, to);

        //更改segmentsInfo信息
        //TODO:once get null from tlog file, get the es version ,and then --
        //TODO: --use Strings.randomBase64UUID() create .tlog and .ckp file
        if (getSegInfo(to.getParent().resolve(TRANSLOG_PATH).resolve(TRANSLOG_TLOG_FILE).toString())){
            //set to lucene
            setToLucene(to);
        }
        Utils.setPermissionRecursive(to);
    }

    private boolean getSegInfo(String tlog){
        log.info("getSeginfo: "+tlog);
        //TODO: ensure no flush
        //appcom/es/data/uat-es/nodes/0/indices/lead/0/translog/translog-1.tlog
        File file = new File(tlog);
        FileInputStream fileInputStream = null;
        try {
            //read UUID from translog-1.tlg:
            fileInputStream = new FileInputStream(file);
            byte[] bytes = new byte[1024];
            while(fileInputStream.read(bytes) != -1){
                TLOG_UUID = new String(bytes,20,43).trim();
            }
            log.info("get tlog file: "+tlog+" uuid: "+TLOG_UUID);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    private void setToLucene(Path segPath){
        log.info("set to lucene: "+segPath);
        try {
            FSDirectory directory = FSDirectory.open(segPath);
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
            Map<String, String> commitData = new HashMap<>(2);
            commitData.put(Translog.TRANSLOG_GENERATION_KEY, TRANSLOG_GENERATION_KEY);
            commitData.put(Translog.TRANSLOG_UUID_KEY, TLOG_UUID);

            Method setUserData = segmentInfos.getClass().getDeclaredMethod("setUserData", Map.class);
            setUserData.setAccessible(true);
            setUserData.invoke(segmentInfos, commitData);

            Method commit = segmentInfos.getClass().getDeclaredMethod("commit", Directory.class);
            commit.setAccessible(true);
            commit.invoke(segmentInfos, directory);

            log.info("set userData: "+segmentInfos.getUserData());
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private synchronized boolean downloadStateFile(String hdfsWorkDir, String indexName,
                                                   Path localStateDir) {
        if (Files.exists(localStateDir.resolve(STATE_DIR)) && Files
                .exists(localStateDir.resolve(SHARD_STATE))) {
            log.info("State file already downloaded");
            return true;
        }
        log.info("Local state dir is {}", localStateDir.toString());
        try {
            // download & unzip index state
            String hdfsStateDir = Paths.get(hdfsWorkDir, indexName, STATE_DIR).toString();
            String hdfsStateFile = hdfsClient.largestFileInDirectory(hdfsStateDir);
            log.info("Download index state file from {}", hdfsStateFile);
            hdfsClient.downloadFile(hdfsStateFile, localStateDir.resolve(STATE_DIR + ".zip").toString());
            Utils.unzip(localStateDir.resolve(STATE_DIR + ".zip"), localStateDir);
            Files.deleteIfExists(localStateDir.resolve(STATE_DIR + ".zip"));

            // download & unzip shard state
            String hdfsShardStateFile = Paths.get(hdfsWorkDir, indexName, SHARD_STATE + ".zip")
                    .toString();
            log.info("Download shard state file from {}", hdfsShardStateFile);
            hdfsClient
                    .downloadFile(hdfsShardStateFile, localStateDir.resolve(SHARD_STATE + ".zip").toString());
            Utils.unzip(localStateDir.resolve(SHARD_STATE + ".zip"), localStateDir);
            Files.deleteIfExists(localStateDir.resolve(SHARD_STATE + ".zip"));
            return true;
        } catch (IOException e) {
            log.error("Download state file from hdfs failed", e);
            return false;
        }
    }

    /**
     * 使用Lucene合并索引会非常慢，所以这里直接进行文件移动，然后重新生成segment信息
     */
    private synchronized String mergeIndex(String indexBundlePath) throws IOException {
        Path path = Paths.get(indexBundlePath);
        log.info("shard " + path.getFileName()+" in path"+ path.toString());
        List<Path> indexList = Files.list(path).filter(p -> Files.isDirectory(p))
                .collect(Collectors.toList());
        log.info("indexList: "+indexList+" size: "+indexList.size())
        if(indexList.size()==0){
            return "1";
        }
        Collections.sort(indexList);
        try (
                FSDirectory directory = FSDirectory.open(indexList.get(0).resolve("index"))
        ) {
            log.info("Original segment info file path is {}", indexList.get(0).resolve("index").toString());
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
            String originSegmentFileName = segmentInfos.getSegmentsFileName();
            Path originSegmentPath = directory.getDirectory().resolve(originSegmentFileName);
            log.info("Original segment info file path is {}", originSegmentPath.toString());
//            List<SegmentCommitInfo> infos = new ArrayList<>();
//
//            for (int i = 1; i < indexList.size(); i++) {
//                FSDirectory dir = FSDirectory.open(indexList.get(i).resolve("index"));
//                SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
//                for (SegmentCommitInfo info : sis) {
//                    String newSegName = newSegmentName(segmentInfos);
//                    log.info("New segment name is {}", newSegName);
//                    infos.add(copySegmentAsIs(directory, info, newSegName));
//                }
//            }
//            segmentInfos.addAll(infos);
//            SegmentInfos pendingCommit = segmentInfos.clone();
//            Method prepareCommit = pendingCommit.getClass()
//                    .getDeclaredMethod("prepareCommit", Directory.class);
//            prepareCommit.setAccessible(true);
//            prepareCommit.invoke(pendingCommit, directory);
//
//            log.info("Add pending segment info file");
//
//            Method updateGeneration = segmentInfos.getClass()
//                    .getDeclaredMethod("updateGeneration", SegmentInfos.class);
//            updateGeneration.setAccessible(true);
//            updateGeneration.invoke(segmentInfos, pendingCommit);
//
//            log.info("Update segment info generation");
//
//            Method finishCommit = pendingCommit.getClass()
//                    .getDeclaredMethod("finishCommit", Directory.class);
//            finishCommit.setAccessible(true);
//            finishCommit.invoke(pendingCommit, directory);
//
//            log.info("Finish segment info commit");
//
//            Files.delete(originSegmentPath);
//            log.info("Delete origin segment info file {}", originSegmentPath.toString());
//
//            log.info("merge index for shard " + path.getFileName() + " done");
//        } catch (IOException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
//            log.error("Merge index for shard " + path.getFileName() + " error", e);
        }
        return indexList.get(0).toString();
    }

    private SegmentCommitInfo copySegmentAsIs(Directory directory, SegmentCommitInfo info,
                                              String segName) throws IOException {

        SegmentInfo newInfo = new SegmentInfo(directory, info.info.getVersion(), segName,
                info.info.maxDoc(),
                info.info.getUseCompoundFile(), info.info.getCodec(),
                info.info.getDiagnostics(), info.info.getId(), info.info.getAttributes());
        SegmentCommitInfo newInfoPerCommit = new SegmentCommitInfo(newInfo, info.getDelCount(),
                info.getDelGen(),
                info.getFieldInfosGen(), info.getDocValuesGen());

        newInfo.setFiles(info.files());

        boolean success = false;

        Set<String> copiedFiles = new HashSet<>();
        try {
            // Copy the segment's files
            for (String file : info.files()) {
                Method namedForThisSegment = newInfo.getClass()
                        .getDeclaredMethod("namedForThisSegment", String.class);
                namedForThisSegment.setAccessible(true);
                final String newFileName = (String) namedForThisSegment.invoke(newInfo, file);

                FSDirectory srcDir = (FSDirectory) info.info.dir;
                FSDirectory destDir = (FSDirectory) directory;
                Path srcFile = srcDir.getDirectory().resolve(file);
                Path destFile = destDir.getDirectory().resolve(newFileName);
                Files.move(srcFile, destFile);
                log.debug("Move index file from {} to {}", srcFile.toString(), destFile.toString());
                copiedFiles.add(newFileName);
            }
            success = true;
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            log.error("Get new segment name error", e);
        } finally {
            if (!success) {
//        deleteNewFiles(copiedFiles);
            }
        }

        assert copiedFiles.equals(newInfoPerCommit.files());

        return newInfoPerCommit;
    }

    public String newSegmentName(SegmentInfos segmentInfos) {
        segmentInfos.changed();
        return "_" + Integer.toString(segmentInfos.counter++, Character.MAX_RADIX);
    }


    public void markIndexCompleted(String indexName) {
        completedIndices.put(indexName, true);
    }

    public boolean indexCompleted(String indexName) {
        return completedIndices.getOrDefault(indexName, false);
    }

}
