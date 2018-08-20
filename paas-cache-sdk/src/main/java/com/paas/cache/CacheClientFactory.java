package com.paas.cache;

import com.alibaba.fastjson.JSON;
import com.paas.cache.jedis.*;
import com.paas.auth.service.AuthClientFactory;
import com.paas.auth.service.IAuthClient;
import com.paas.auth.vo.AuthDescriptor;
import com.paas.auth.vo.AuthResult;
import com.paas.zk.zookeeper.ZKClient;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created on 2016/9/26.
 */
public class CacheClientFactory {
    private static final String PATH_PRE = "/bbtree/ts/";
    private static final String SEPARATOR = "/";
    protected final static Logger log = LoggerFactory.getLogger(CacheClientFactory.class);

    private static Map<String, ICacheClient> caches = new ConcurrentHashMap<>();
    private static Map<String, CacheConfig> configs = new ConcurrentHashMap<>();
    private static Map<String, ZKClient> zkClients = new ConcurrentHashMap<>();

    /**
     * 获取缓存客户端
     * @param serviceId 服务ID
     * @param authUrl 认证中心地址
     * @return ICacheClient 缓存客户端
     */
    public static ICacheClient getClient(String serviceId, String authUrl){
        ICacheClient client = caches.get(serviceId);
        if(client == null){
            client = initClient(serviceId, authUrl);
        }
        log.debug(" ---> get client result: {}", client);
        return client;
    }

    private static synchronized ICacheClient initClient(String bizKey, String authUrl){
        log.info(" ---> init cache client ...");
        ICacheClient client = caches.get(bizKey);
        if(client == null){
            CacheConfig config = getCacheConfig(bizKey, authUrl);
            client = createClient(config);
        }
        log.info(" ---> init cache client down: {}", client);
        return client;
    }

    private static ICacheClient createClient(CacheConfig config){
        ICacheClient client = null;
        if(config != null){
            if(config.getJedisConfig().getServerArray().length > 1){
                client = new JedisClusterClient(config.getJedisConfig());
            }else{
                client = new JedisClient(config.getJedisConfig());
            }
//            caches.put(config.getBusinessKey(), client);
//            ICacheClient proxy = new ClientProxy(client, config.getBizCode());
            client = ClientTimeProxy.getProxy(client, config.getBizCode(), config.getJedisConfig().getWarnTime());
            caches.put(config.getBusinessKey(), client);
        }
        return client;
    }

    private static CacheConfig getCacheConfig(String bizKey, String authUrl){
        log.info(" ---> init cache config ...");
        CacheConfig config = configs.get(bizKey);
        if(config == null){
            String[] arr = bizKey.split("-");
            String configPath = new StringBuilder(PATH_PRE)
                    .append(arr[0]).append(SEPARATOR)
                    .append(arr[1]).append(SEPARATOR)
                    .append(bizKey).toString();
            CacheConfig newConfig = new CacheConfig(bizKey, authUrl, configPath);
            newConfig.setBizCode(arr[0]);
            try {
                JedisConfig jc = getJedisConfig(newConfig);
                if(jc != null){
                    newConfig.setJedisConfig(jc);
                    configs.put(bizKey, newConfig);
                    config = newConfig;
                }
            } catch (Exception e) {
                log.error(" ---> init cache config error. ", e);
            }
        }
        log.info(" ---> init cache config: {}", JSON.toJSONString(config));
        return config;
    }

    private static JedisConfig getJedisConfig(CacheConfig cacheConfig) throws Exception {
        log.info(" ---> authorize cache config, key: {}, url: {}", cacheConfig.getBusinessKey(), cacheConfig.getAuthUrl());
        String result = doAuth(cacheConfig);
        cacheConfig.setZkAdress(result);
        log.info(" ---> authorize result: {}", result);

        if(result == null || result.trim().length() == 0){
            return null;
        }

        ZKClient zkClient = new ZKClient(result, 3000);
        zkClients.put(cacheConfig.getBusinessKey(), zkClient);
        String zkConfig = zkClient.getNodeData(cacheConfig.getConfigPath(), new Watcher(){
            @Override
            public void process(WatchedEvent event) {
                log.info(" --->  watch zk process.{};{};{}", event.getPath(), event.getState(), event.getType());
                if(event.getPath().equals(cacheConfig.getConfigPath()) && Event.EventType.NodeDataChanged.equals(event.getType())){
                    resetCache(cacheConfig);
                }
            }
        });
        log.info(" ---> jedis zk config: {}", zkConfig);
        return parseConfig(zkConfig);

        // for test
//        String zkConfig = "{\"servers\":\"172.16.1.135:7001,172.16.0.117:7000,172.16.0.115:7001,172.16.1.135:7000,172.16.0.116:7001,172.16.0.115:7000\"," +
//                "\"conf\":{\"maxIdle\":100,\"testOnBorrow\":\"false\",\"testOnReturn\":\"true\",\"maxWait\":3000,\"maxActive\":1024}}";

//        String zkConfig = "{\"servers\":\"172.16.0.115:19000\"," +
//                "\"conf\":{\"maxIdle\":100,\"testOnBorrow\":\"false\",\"testOnReturn\":\"true\",\"maxWait\":3000,\"maxActive\":1024}}";
//        return parseConfig(zkConfig);
    }

    private static String doAuth(CacheConfig config) throws Exception {
        IAuthClient iauth = AuthClientFactory.getAuthClient();
        AuthDescriptor ad = new AuthDescriptor(config.getAuthUrl(), config.getBusinessKey());
        AuthResult ar = iauth.auth(ad);
        String result = ar.getZkAdress();

//        String result = "114.215.202.56:32181";
        return result;
    }

    private static JedisConfig parseConfig(String config){
        JedisConfig conf = JSON.parseObject(config, JedisConfig.class);
        if(log.isDebugEnabled()){
            log.debug(" ---> jedis parseConfig: {}", JSON.toJSON(conf));
        }
        return conf;
    }

    private static void resetCache(CacheConfig cacheConfig){
        CacheConfig cc = configs.get(cacheConfig.getBusinessKey());
        if(cc == null){
            return;
        }
        try {
            ZKClient zkClient = zkClients.get(cacheConfig.getBusinessKey());
            String zkConfig = zkClient.getNodeData(cacheConfig.getConfigPath());
            log.info(" ---> reset jedis zk config: {}", zkConfig);
            JedisConfig oldJc = cc.getJedisConfig();

            JedisConfig jc = parseConfig(zkConfig);
            cacheConfig.setJedisConfig(jc);
            log.info(" ---> reset jedis config, new: [{}], old: [{}]", jc, oldJc);

            ICacheClient oldClient = caches.get(cacheConfig.getBusinessKey());
            ICacheClient client = createClient(cacheConfig);

            log.info(" ---> reset jedis client, new: [{}], old: [{}]", client, oldClient);
            if(client != null && oldClient != null){
                if(oldClient instanceof JedisClient){
                    ((JedisClient)oldClient).destroy();
                }
            }
        } catch (Exception e) {
            log.error(" ---> reset cache error. ", e);
        }
    }

}
