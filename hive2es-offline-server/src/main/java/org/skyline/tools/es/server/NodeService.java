package org.skyline.tools.es.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.skyline.tools.es.server.utils.IpUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author Sean Liu
 * @date 2019-12-05
 */
@Component
@Slf4j
public class NodeService {

    private static final String NODE_PATH = "nodes";
    private static final String ES_NODE_JOINER = ",";
    @Autowired
    private RegistryCenter registryCenter;

    @Getter
    private Node localNode;

    @Autowired
    private LeaderSelectorController leaderSelectorController;

    @Autowired
    private IndicesChangeController indicesChangeController;

    @Autowired
    private ESClient esClient;

    @Autowired
    private HdfsClient hdfsClient;

    @Autowired
    private IndexBuilder indexBuilder;

    private volatile boolean started = false;
    private static final String ASSIGN_FLAG = "_assigned";

    @PostConstruct
    public void init() throws Exception {
        String nodeId = IpUtils.getId();
        localNode = new Node(nodeId);
        this.start();
    }

    public void start() throws Exception {
        if (started) {
            return;
        }
        log.info("Start node {}", localNode.getNodeId());
        this.registerNode();
        leaderSelectorController.start();
        indicesChangeController.start();
    }

    @PreDestroy
    public void close() throws IOException {
        log.info("Close node {}", this.localNode);
        leaderSelectorController.close();
        indicesChangeController.close();
        registryCenter.close();
    }

    public void registerNode() {
        log.info("Register node {} on path {}", localNode.getNodeId(),
                registryCenter.getFullPath(localNode.getZKPath()));
        registryCenter.persistEphemeral(localNode.getZKPath(), "");
        this.updateESNodeInfo();
    }


    public void buildIndex(String indexPath, String data) {
        new Thread(() -> {
            String indexNodePath = indexPath + "/" + localNode.getNodeId();

            // 在开始build之前先各自更新一下当前节点上运行es data node数量，防止data node掉线
            this.updateESNodeInfo();
            JSONObject configData = JSON.parseObject(data);

            try {
                String serverLeaderNode = leaderSelectorController.getServerLeaderNode();
                log.info(serverLeaderNode);
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (leaderSelectorController.hasLeadership()) {
                try {
                    assignShards(configData, indexPath);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                registryCenter.persist(indexPath + "/" + ASSIGN_FLAG, "");
            }
            Map<String, List<String>> currentNodeShards = getCurrentNodeShards(indexPath, indexNodePath);
            log.info("id2Shards: "+currentNodeShards);
            if (MapUtils.isNotEmpty(currentNodeShards)) {
                log.info("Current node shards is : {}", currentNodeShards);
                if (MapUtils.isNotEmpty(currentNodeShards)) {
                    boolean success = indexBuilder.build(currentNodeShards, configData);
                    if (success) {
                        //TODO is to delete es node Path
//                        registryCenter.delete(indexNodePath);
                        log.info("Build index for {} complete", indexNodePath);
                    }
                }
            }

            if (leaderSelectorController.hasLeadership()) {
                waitAllNodeComplete(indexPath);
                String finalIndexSetting = configData.getString("finalIndexSetting");
                String indexName = configData.getString("indexName");
                log.info("Trigger cluster state change for index {}", indexName);
                esClient.triggerClusterChange(indexName);
                if (StringUtils.isNotEmpty(finalIndexSetting)) {
                    try {
                        log.info("Update final index setting : {}", finalIndexSetting);
                        esClient.updateIndexSetting(indexName, finalIndexSetting);
                    } catch (Exception e) {
                        log.error("Update final index setting failed", e);
                    }
                }
                registryCenter.delete(indexPath);
                log.info("Build index for {} all complete", indexPath);
            }
        }).start();
    }

    public void markIndexComplete(String indexPath, String data) {
        JSONObject configData = JSON.parseObject(data);
        boolean completed = configData.getString("state").equalsIgnoreCase("completed");
        if (completed) {
            String indexName = configData.getString("indexName");
            indexBuilder.markIndexCompleted(indexName);
        }
    }

    private void waitAllNodeComplete(String indexPath) {
        int leftCount;
        while ((leftCount = registryCenter.getNumChildren(indexPath)) != 1) {
            try {
                Thread.sleep(1000);
                log.info("Wait all node complete, [{}] node left ,sleep 1000 ms", leftCount - 1);
            } catch (InterruptedException e) {
                log.error("Wait all node complete error", e);
            }
        }
        log.info("All node completed for indexPath : {}", indexPath);
    }

    public Map<String, String[]> getAllRegisteredNode() {
        Map<String, String[]> result = Maps.newHashMap();
        List<String> childrenPaths = registryCenter.getChildrenPaths(NODE_PATH);
        log.info("All registered node is {}", childrenPaths);
        for (String path : childrenPaths) {
            String esNodesStr = registryCenter.getValue(NODE_PATH + "/" + path);
            String[] esNodes = esNodesStr.split(ES_NODE_JOINER);
            log.info("Node to ES node is {} : {}", path, Lists.newArrayList(esNodes));
            if (ArrayUtils.isNotEmpty(esNodes) && StringUtils.isNotEmpty(esNodesStr)) {
                result.put(path, esNodes);
            }
        }
        return result;
    }

    private void startAssignShard() {

    }

    private void assignShards(JSONObject configData, String indexPath) throws IOException, InterruptedException {
        log.info("Start assign shard");
        log.info("version 3");
        String indexName = configData.getString("indexName");
        int numberShards = configData.getInteger("numberShards");
        esClient.createIndexFirst(indexName,0,numberShards);
        log.info("create index finished: "+indexName+"---"+"--"+numberShards);
        //add mapping
            // tmp/es/custom_201912184/mapping.json
            String mappingString = hdfsClient.readMappingJson(Paths
                    .get(configData.getString("hdfsWorkDir"))
                    .resolve(configData.getString("indexName"))
                    .resolve("mapping.json")
                    .toString());
            log.info(mappingString.length()+"");
            esClient.putMapping(JSON.parseObject(mappingString),indexName,configData.getString("typeName"));


//        stsz030282 : [SOnMwP-vRlKiOqiNA_1p1w, Tbj6H9K0Q_SqgQMTW8CrKA]
        Map<String, String[]> allNodes = this.getAllRegisteredNode();

        List<String> serverAliveIds = allNodes.values().stream()
                .flatMap(x -> Lists.newArrayList(x).stream())
                .filter(x -> StringUtils.isNotEmpty(x))
                .collect(Collectors.toList());

        log.info("Server alive node id sequence is : {}", serverAliveIds);

        List<String> ids = new ArrayList<>();
        Map<Integer, String> relocationShards = new HashMap<>();

        //only solve server node down but es node is alive
        //when es node down but server node alive is ok
        Map<Integer, String> nodesShards = esClient.getNodesShards(indexName);
        log.info("es cluster first asssion shard distribution: "+nodesShards);
        for (Map.Entry<Integer, String> node2ShardsInEsCluster : nodesShards.entrySet()) {
            Integer sharId = node2ShardsInEsCluster.getKey();
            String clusterNodeId = node2ShardsInEsCluster.getValue();
            //确认该节点所在的服务是否正常
            if (serverAliveIds.contains(clusterNodeId)) {
                ids.add(sharId, clusterNodeId);
            } else {
                relocationShards.put(sharId, clusterNodeId);
            }
        }
        log.info("can't assign shard list: "+relocationShards);
        for (Map.Entry<Integer, String> relocationShard : relocationShards.entrySet()) {
            Integer shardId = relocationShard.getKey();
            String oldNodeId = relocationShard.getValue();
            //这里注意下标
            for (String newNodeId : ids) {
                if (ids.indexOf(newNodeId) == ids.lastIndexOf(newNodeId)) {
                    boolean moveResult = esClient.relocationShards(indexName, shardId, oldNodeId, newNodeId);
                    if (moveResult){
                        log.info("move shard [" + shardId  + "] from [" + oldNodeId  + "] to [" + newNodeId + "] success");
                        ids.add(shardId,newNodeId);
                        break;
                    }else {
                        log.info("move shard [" + shardId  + "] from [" + oldNodeId  + "] to [" + newNodeId + "] fail, move it to next node");
                    }
                }
            }
        }
        log.info("ES cluster final node and shard route is : {}", ids);
        log.info("Node to ES node is : {}",allNodes);
        //一台机器写一次ZK， 格式：{esclustNodeId1:[shardid1,shardid2],esclustNodeId2:[shardid1,shardid2]},hostid
        //x:stsz030282-[SOnMwP-vRlKiOqiNA_1p1w, Tbj6H9K0Q_SqgQMTW8CrKA]
        //注意keyName
        allNodes.entrySet().forEach(x -> {
            //the final shard loaction
            Map<Integer, String> newNodesShards = esClient.getNodesShards(indexName);
            //nodeid:stsz030282
            String nodeId = x.getKey();
            //用来装：clusterNodeID-shardIdList
            Map<String, List<Integer>> idToShards = Maps.newHashMap();

            List<String> clustNodeIdList = Arrays.asList(x.getValue());

            //找出每个nodeId对应的esclusterNodeId-shardIdList
            for (Map.Entry<Integer, String> shardNode : newNodesShards.entrySet()) {
                String clusternodeId = shardNode.getValue();
                //这台机器上的节点
                if (clustNodeIdList.contains(clusternodeId)){
                    List<Integer> shards = idToShards.get(clusternodeId);
                    if (shards == null){
                        shards = new ArrayList<>();
                        idToShards.put(clusternodeId,shards);
                    }
                    shards.add(shardNode.getKey());
                }
            }
            if (MapUtils.isNotEmpty(idToShards)) {
                String idToShardsJSON = JSON.toJSONString(idToShards);
                registryCenter.persist(indexPath + "/" + nodeId, idToShardsJSON);
            }
        });
        log.info("End assign shard");
    }

    private Map<String, List<String>> getCurrentNodeShards(String indexPath, String indexNodePath) {
        Map<String, List<String>> result = Maps.newHashMap();
        while (true) {
            if (registryCenter.isExisted(indexPath + "/" + ASSIGN_FLAG)) {
                break;
            }
            try {
                Thread.sleep(1000);
                log.info("Wait shard assign and sleep 1000 ms");
            } catch (InterruptedException e) {
                log.error("Wait shard assign error", e);
            }
        }
        if (registryCenter.isExisted(indexNodePath)) {
            JSONObject data = JSON.parseObject(registryCenter.getValue(indexNodePath));
            data.keySet().forEach(x -> result.put(x, data.getJSONArray(x).toJavaList(String.class)));
        }
        return result;
    }

    public void updateESNodeInfo() {
        Set<String> nodeNames = esClient.getNodeNameOnHost();
        String nodes = String.join(ES_NODE_JOINER, nodeNames);
        log.info("Update local es node info {}", nodes);
        registryCenter.update(localNode.getZKPath(), nodes);
    }

    @Data
    public static class Node {

        public Node(String nodeId) {
            this.nodeId = nodeId;
        }


        private String nodeId;

        public String getZKPath() {
            return NODE_PATH + "/" + nodeId;
        }

    }

}
