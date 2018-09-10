package com.paas.mq.config;

import com.alibaba.fastjson.JSON;
import com.paas.auth.service.AuthClientFactory;
import com.paas.auth.service.IAuthClient;
import com.paas.auth.vo.AuthDescriptor;
import com.paas.auth.vo.AuthResult;
import com.paas.zk.zookeeper.ConfigWatcher;
import com.paas.zk.zookeeper.ZKClient;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


public class ConfigFactory {

    private static final String PATH_PRE = "/bbtree/ts/";
    private static final String SEPARATOR = "/";

    private static Logger log = LoggerFactory.getLogger(ConfigFactory.class);

    private static Map<String, MqConfig> configs = new ConcurrentHashMap<>();

    public static MqConfig getMqConfig(String serviceId, String authUrl){
        MqConfig config = configs.get(serviceId);
        if(config == null){
            try {
                config = initMqConfig(serviceId, authUrl);
            } catch (Exception e) {
                log.info(" ---> init mq error. ", e);
            }
        }
        log.debug(" ---> MqConfig: {}. ", config);
        return config;
    }

    private static synchronized MqConfig initMqConfig(String serviceId, String authUrl) throws Exception{
        MqConfig config = configs.get(serviceId);
        if(config != null){
            return config;
        }
        config = new MqConfig();
        config.setServiceId(serviceId);
        config.setAuthUrl(authUrl);

        log.info(" ---> authorize mq config, key: {}, url: {}", serviceId, authUrl);
        String zkAddress = doAuth(serviceId, authUrl);
        log.info(" ---> authorize zookeeper : {}", zkAddress);
        config.setZkAddress(zkAddress);

        if(zkAddress == null || zkAddress.trim().length() == 0){
            return null;
        }

        String[] arr = serviceId.split("-");
        String configPath = new StringBuilder(PATH_PRE).append(arr[0]).append(SEPARATOR)
                .append(arr[1]).append(SEPARATOR).append(serviceId).toString();
        config.setZkConfigPath(configPath);

        ZKClient zkClient = new ZKClient(zkAddress, 3000);
        ConfigRegister.registryClient(configPath, zkClient);

        config = getAndParseConfig(serviceId, configPath, config);
        configs.put(serviceId, config);
        return config;
    }

    private static MqConfig getAndParseConfig(String serviceId, String configPath, MqConfig origConfig) throws Exception{
        String zkConfig = ConfigRegister.getClient(configPath).getNodeData(configPath, new Watcher(){
            @Override
            public void process(WatchedEvent event) {
                log.info(" --->  watch zk process.{};{};{}", event.getPath(), event.getState(), event.getType());
                if(ConfigWatcher.Event.KeeperState.Expired.equals(event.getState())){
                    processExpired(serviceId, configPath);
                }else if(configPath.equals(event.getPath()) && ConfigWatcher.Event.EventType.NodeDataChanged.equals(event.getType())){
                    doChange(serviceId, configPath);
                }
            }
        });
        log.info(" ---> mq zk config: {}", zkConfig);
        MqConfig newConfig = parseMqConfig(zkConfig, origConfig);
        return newConfig;
    }

    private static void doChange(String serviceId, String configPath){
        MqConfig oldConfig = configs.get(serviceId);
        MqConfig newConfig = null;
        try {
            newConfig = getAndParseConfig(serviceId, configPath, oldConfig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        configs.put(serviceId, newConfig);
        ConfigListener cc = ConfigRegister.get(configPath);
        if(cc != null) {
            cc.changed(configPath);
        }
    }

    private static void processExpired(String serviceId, String configPath){
        ZKClient zkClient = ConfigRegister.getClient(configPath);
        try {
            while(!zkClient.isConnected()){
                try {
                    zkClient.retryConnection();
                } catch (IllegalStateException e) {
                    log.error("retry connection zk.", e);
                    break;
                } catch (Exception e){
                    log.error("retry connection zk failed.", e);
                }
                TimeUnit.SECONDS.sleep(2);
            }
            doChange(serviceId, configPath);
        } catch (Exception e) {
            log.error("retry connection zk failed.", e);
        }
    }

    public static MqConfig parseMqConfig(String config, MqConfig origConfig){
        MqConfig conf = JSON.parseObject(config, MqConfig.class);
        if(log.isDebugEnabled()){
            log.debug(" ---> mq parseConfig: {}", JSON.toJSON(conf));
        }
        conf.setServiceId(origConfig.getServiceId());
        conf.setAuthUrl(origConfig.getAuthUrl());
        conf.setZkAddress(origConfig.getZkAddress());
        conf.setZkConfigPath(origConfig.getZkConfigPath());
        return conf;
    }

    private static String doAuth(String businessKey, String authUrl) throws Exception {
        IAuthClient iauth = AuthClientFactory.getAuthClient();
        AuthResult ar = iauth.auth(new AuthDescriptor(authUrl, businessKey));
        String result = ar.getZkAdress();
//        String result = "114.215.202.56:32181";
        return result;
    }

}
