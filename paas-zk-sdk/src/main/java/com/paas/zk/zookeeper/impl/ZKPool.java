package com.paas.zk.zookeeper.impl;

import com.paas.zk.zookeeper.ZKClient;

import java.util.concurrent.ConcurrentHashMap;

public class ZKPool {
    private static ConcurrentHashMap<String, ZKClient> clients = new ConcurrentHashMap<String, ZKClient>();

    public ZKClient getZkClient(final String zkAddr) throws Exception {
        return clients.get(appendKey(zkAddr));
    }

    public ZKClient getZkClient(final String zkAddr, final String zkUserName) throws Exception {
        return clients.get(appendKey(zkAddr, zkUserName));
    }

    public ZKClient getZkClient(final String zkAddr, final String zkUserName, final String serviceId) throws Exception {
        return clients.get(appendKey(zkAddr, zkUserName, serviceId));
    }

    private String appendKey(String zkAddr){
        return appendKey(zkAddr, null, null);
    }

    private String appendKey(String zkAddr, String zkUserName) {
        return appendKey(zkAddr, zkUserName, null);
    }

    private String appendKey(String zkAddr, String zkUserName, String serviceId) {
        StringBuilder key = new StringBuilder(zkAddr);
        if (zkUserName!=null&&!zkUserName.trim().equals("")) {
            key.append("-");
            key.append(zkUserName);
        }

        if (serviceId!=null&&!serviceId.trim().equals("")) {
            key.append("-");
            key.append(serviceId);
        }
        return key.toString();
    }

    public void addZKClient(final String zkAddr, final String zkUserName, ZKClient zkClient) {
        clients.put(appendKey(zkAddr, zkUserName), zkClient);
    }

    public void addZKClient(final String zkAddr, final String zkUserName, final String serviceId, ZKClient zkClient) {
        clients.put(appendKey(zkAddr, zkUserName, serviceId), zkClient);
    }

    public boolean exist(final String zkAddr, final String zkUserName) {
        return clients.containsKey(appendKey(zkAddr, zkUserName));
    }

    public boolean exist(final String zkAddr, final String zkUserName, final String serviceId) {
        return clients.containsKey(appendKey(zkAddr, zkUserName, serviceId));
    }

}
