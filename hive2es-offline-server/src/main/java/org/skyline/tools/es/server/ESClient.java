package org.skyline.tools.es.server;

import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.collect.Maps;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse.StoreStatus;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.exists.ExistsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.monitor.fs.FsInfo.Path;
import org.skyline.tools.es.server.utils.IpUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author sean
 */
@Component
@Slf4j
public class ESClient {

    @Autowired
    private TransportClient client;

    //change1 start: date 20200323, only add method
    public boolean createIndexFirst(String indexName,int replicasNum,int shardNum) {
        log.info("ready to create index");
        Settings settings = Settings.builder()
                .put("number_of_replicas", replicasNum)
                .put("number_of_shards", shardNum).build();
        client.admin().indices()
                .prepareCreate(indexName)
                .setSettings(settings).get();
        log.info("finish to create index");
        return true;
    }
    //server master do this, check if server node and es node are ready to work
    public Map<Integer, String> getNodesShards(String indexName) {
        Map<Integer, String> shardAssgin = new HashMap<>();
        ClusterSearchShardsGroup[] shardsGroups = client.admin().cluster().prepareSearchShards()
                .setIndices(indexName).get().getGroups();
        for (ClusterSearchShardsGroup shardsGroup : shardsGroups) {
            ShardRouting[] shards = shardsGroup.getShards();
            for (ShardRouting shard : shards) {
                int shardId = shard.getId();
                String nodeId = shard.currentNodeId();
                shardAssgin.put(shardId, nodeId);
            }
        }
        return shardAssgin;
    }
    //only server master car do this
    //需要先禁止自动分配节点
    public boolean relocationShards(String indexName, int shardId, String fromNode, String toNode) {
        String clusterFromNodeName = getAllNodeClusterInfo().get(fromNode).get(0);
        String clusterToNodeName = getAllNodeClusterInfo().get(toNode).get(0);

        ShardId moveShardId = new ShardId(indexName, shardId);
        MoveAllocationCommand moveAllocationCommand = new MoveAllocationCommand(
                moveShardId, clusterFromNodeName, clusterToNodeName);
        boolean done = client.admin().cluster().prepareReroute().add(moveAllocationCommand).execute().actionGet().isAcknowledged();

        return done;

    }
    public Map<String, List<String>> getAllNodeClusterInfo() {
        //clusterNodeId-clusterNodeName-Ip
        HashMap<String, List<String>> nodeAllInfo = new HashMap<>();
        List<String> nodeName2Ip = new ArrayList<>();

        NodesInfoResponse clusterInfo = client.admin().cluster().prepareNodesInfo().get();
        NodeInfo[] nodes = clusterInfo.getNodes();
        for (NodeInfo nodeInfo : nodes) {
            DiscoveryNode node = nodeInfo.getNode();
            //machine IP
            String IP = node.getHostName();
            //cluster node name
            String clusterName = node.getName();
            //cluster Ip, No means
            String clusterNodeId = node.getId();

            nodeName2Ip.add(0, clusterName);
            nodeName2Ip.add(1, IP);
            nodeAllInfo.put(clusterNodeId, nodeName2Ip);
        }
        return nodeAllInfo;
    }
    //这时没有副本
    public String getShardDataPath(String indexName,int shardId){
        ShardStats[] shards = client.admin().indices().prepareStats(indexName).get().getShards();
        String shardDataPath = "";
        for (ShardStats shard : shards) {
            if (shard.getShardRouting().getId() == shardId){
                shardDataPath = shard.getDataPath();
            }
        }
        return shardDataPath;
    }
    //**************  change1 end **************************************

    public Set<String> getNodeNameOnHost() {
        return this.getDataNodeInfoOnHost().keySet();
    }

    public Map<String, NodeInfo> getDataNodeInfoOnHost() {
        Map<String, NodeInfo> result = Maps.newHashMap();
        NodesInfoResponse response = client.admin().cluster().prepareNodesInfo().get();
        for (NodeInfo info : response.getNodes()) {
            boolean currentHostDataNode = isCurrentHostDataNode(info);
            if (currentHostDataNode) {

                result.put(info.getNode().getId(), info);
            }
        }
        return result;
    }

    private boolean isCurrentHostDataNode(NodeInfo info) {
        try {
            return info.getNode().isDataNode() && (
                    info.getHostname().equalsIgnoreCase(IpUtils.getHostName())
                            || info.getNode().getHostAddress().equalsIgnoreCase(IpUtils.getIp()));
        } catch (UnknownHostException | SocketException e) {
            log.error("Check host error", e);
        }
        return false;
    }

    public String[] getDataPathByNodeId(String nodeId) {
        NodesStatsResponse resp = client.admin().cluster().prepareNodesStats(nodeId).setFs(true)
                .get();
        List<String> result = Lists.newArrayList();
        if (resp.getNodes().length > 0) {
            Iterator<Path> it = resp.getNodes()[0].getFs().iterator();
            while (it.hasNext()) {
                result.add(it.next().getPath());
            }
        }
        return result.toArray(new String[result.size()]);
    }


    public void triggerClusterChange(String indexName) {
        //TODO:Maybe should try a more elegant way to trigger
        client.admin().indices().prepareClose(indexName);
        client.admin().indices().prepareOpen(indexName);
    }

    public boolean indexExists(String indexName) {
        return client.admin().indices().prepareExists(indexName).get().isExists();
    }

    public boolean indexHealth(String indexName) {
        if (indexExists(indexName)) {
            IndicesShardStoresResponse resp = client.admin().indices()
                    .prepareShardStores(indexName).get();
            ImmutableOpenIntMap<List<StoreStatus>> shards = resp.getStoreStatuses()
                    .get(indexName);
            for (int shardId : shards.keys().toArray()) {
                for (StoreStatus storeStatus : shards.get(shardId)) {
                    if (storeStatus.getStoreException() != null) {
                        return false;
                    }
                }
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * this method will check index exists and health
     */
    public void updateIndexSetting(String indexName, String finalIndexSetting) throws Exception {
        int waitCount = 10;
        while (!indexHealth(indexName)) {
            if (waitCount < 0) {
                throw new Exception("Wait index create and check health time out");
            }
            try {
                log.info("Wait index create and check health for 10s");
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                log.error("Wait index create failed", e);
            }
            waitCount--;
        }
        client.admin().indices().prepareUpdateSettings(indexName).setSettings(finalIndexSetting).get();
    }
}
