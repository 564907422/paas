package com.paas.mq.config;

import com.paas.zk.zookeeper.ZKClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class ConfigRegister {

    private static Map<String, ConfigListener> registerCenter = new ConcurrentHashMap<>();
    private static Map<String, ZKClient> clients = new ConcurrentHashMap<>();

    public static ConfigListener get(String key){
        return registerCenter.get(key);
    }

    public static void registry(String key, ConfigListener listener){
        registerCenter.put(key, listener);
    }

    public static void registryClient(String key, ZKClient client){
        clients.put(key, client);
    }

    public static ZKClient getClient(String key){
        return clients.get(key);
    }

    public static ConfigListener remove(String key){
        return registerCenter.remove(key);
    }

}
