package com.paas.mq;

import com.alibaba.fastjson.JSON;
import com.paas.mq.config.ConfigFactory;
import com.paas.mq.config.MqConfig;
import com.paas.mq.config.MqRouting;
import com.paas.mq.core.RabbitMqClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class ClientFactory {

    private static Logger log = LoggerFactory.getLogger(ClientFactory.class);

    private static Map<String, MqConfig> configs = new ConcurrentHashMap<>();
    private static Map<String, IMqClient> clients = new HashMap<>();
    private static Lock lock = new ReentrantLock();


    public static IMqClient getClient(String serviceId, String authUrl){
        IMqClient client = clients.get(serviceId);
        if(client == null){
            try {
                lock.lock();
                client = clients.get(serviceId);
                if (client == null) {
                    MqConfig config = ConfigFactory.getMqConfig(serviceId, authUrl);
                    // MqConfig config = getJsonConfig();
                    configs.put(serviceId, config);
                    client = new RabbitMqClient(config);
                    clients.put(serviceId, client);
                }
            }finally {
                lock.unlock();
            }
        }
        log.debug(" get client: {}.", client);
        return client;
    }

    private static MqConfig getJsonConfig(){
        String config = "{\"username\":\"bbtree\",\"password\":\"hyww@1z3\"," +
                "\"port\":5672,\"host\":\"114.55.104.39\",\"exchange\":\"bbtree_ad_pv_exchange\"," +
                "\"routings\":[{\"count\":1,\"key\":\"ad_exposure_key\",\"queue\":\"bbtree_ad_pv_queue\"}],\"consumerThreads\":10}";

        return JSON.parseObject(config, MqConfig.class);
    }

    private static MqConfig getTestConfig(){
        MqConfig config = new MqConfig();
//        config.setHost("120.26.125.193");
//        config.setPort(55673);
        config.setHost("114.55.104.39");
        config.setPort(5672);
        config.setUsername("bbtree");
        config.setPassword("hyww@1z3");
        config.setConsumerThreads(10);
        config.setAutoAck(false);
        config.setExchange("bbtree_ad_pv_exchange");

        List<MqRouting> routings = new ArrayList<>();
        routings.add(new MqRouting("ad_exposure_key", "bbtree_ad_pv_queue"));
        //routings.get(0).setCount(2);
        config.setRoutings(routings);

        config.setMaxTotal(5);
        config.setMaxIdle(2);

        // config.setConfirm(new IConfirm() {  });
        System.out.println(JSON.toJSON(config));

        return config;
    }
}
