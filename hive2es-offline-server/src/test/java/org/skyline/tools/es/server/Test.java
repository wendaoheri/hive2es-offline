package org.skyline.tools.es.server;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Test {


    public static void main(String[] args) throws UnknownHostException {
        InetSocketTransportAddress node1 = new InetSocketTransportAddress(InetAddress.getByName("26.6.0.90"), 9300);
        InetSocketTransportAddress node2 = new InetSocketTransportAddress(InetAddress.getByName("26.6.0.91"), 9300);

        TransportClient transportClient = TransportClient.builder().build().addTransportAddresses(node1, node2);
    }
}
