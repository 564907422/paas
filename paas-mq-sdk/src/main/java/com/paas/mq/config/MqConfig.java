package com.paas.mq.config;

import com.paas.mq.IConfirm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MqConfig {

    private String serviceId; // 服务id
    private String authUrl; // 认证中心url
    private String zkAddress;
    private String zkConfigPath; // zk配置路径

    private String host;
    private Integer port = 5672;
    private String username = "guest";
    private String password = "guest";
    private String vhost = "/";

    private String exchange;
    private String type = "direct";
    private List<MqRouting> routings; // 路由与队列

    private IConfirm confirm;  // 如果确认, 确认监听

    private int consumerThreads = 20; // 消费者线程数
    private int maxTotal = 30; // 生产者线程数
    private int maxIdle = 20;
    private int maxWaitMillis = 3000;

    private boolean durable = true; // 是否持久化
    private boolean autoAck = true; // 是否自动应答

    private int reconnect = 2; // 断开后每2s重连一次

    //对应着网络层Socket实例connect()方法中的timeout参数，指的是完成TCP三次握手的超时时间 单位毫秒  默认60s
    private int connectionTimeout = 3000;
    //而读取超时是从socket中读取字节流的等待时间 单位毫秒  默认10s
    private int handshakeTimeout = 3000;
    //channel 读取字节流时间 单位毫秒  默认没有设置
    private int channelRpcTimeout = 2000;
    //心跳时间  s
    private int requestedHeartbeat = 60;
    private List<String> queues;

    private Map<String, List<MqRouting>> routingMap;

    private String needSuffix;//是否拼环境信息

    public Map<String, List<MqRouting>> getRoutingMap(){
        if(routingMap == null){
            routingMap = new HashMap<>();
            for(MqRouting mr : routings){
                List<MqRouting> list = routingMap.get(mr.getKey());
                if(list == null){
                    list = new ArrayList<>();
                    routingMap.put(mr.getKey(), list);
                }
                list.add(mr);
            }
        }
        return routingMap;
    }

    public List<String> getQueues(){
        if(queues == null){
            queues = new ArrayList<>();
            for(MqRouting mr : routings){
                if(mr.getCount() == 1){
                    queues.add(mr.getQueue());
                }else if(mr.getCount() > 1){
                    for(int i=0; i<mr.getCount(); i++){
                        queues.add(mr.getQueue()+"_"+i);
                    }
                }
            }
        }
        return queues;
    }

    public String getServiceId() {
        return serviceId;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public String getAuthUrl() {
        return authUrl;
    }

    public void setAuthUrl(String authUrl) {
        this.authUrl = authUrl;
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    public String getZkConfigPath() {
        return zkConfigPath;
    }

    public void setZkConfigPath(String zkConfigPath) {
        this.zkConfigPath = zkConfigPath;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getVhost() {
        return vhost;
    }

    public void setVhost(String vhost) {
        this.vhost = vhost;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<MqRouting> getRoutings() {
        return routings;
    }

    public void setRoutings(List<MqRouting> routings) {
        this.routings = routings;
    }

    public IConfirm getConfirm() {
        return confirm;
    }

    public void setConfirm(IConfirm confirm) {
        this.confirm = confirm;
    }

    public int getConsumerThreads() {
        return consumerThreads;
    }

    public void setConsumerThreads(int consumerThreads) {
        this.consumerThreads = consumerThreads;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public void setMaxTotal(int maxTotal) {
        this.maxTotal = maxTotal;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    public int getMaxWaitMillis() {
        return maxWaitMillis;
    }

    public void setMaxWaitMillis(int maxWaitMillis) {
        this.maxWaitMillis = maxWaitMillis;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isConfirm() {
        return confirm != null;
    }

    public boolean isAutoAck() {
        return autoAck;
    }

    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }

    public int getReconnect() {
        return reconnect;
    }

    public void setReconnect(int reconnect) {
        this.reconnect = reconnect;
    }

    public String getNeedSuffix() {
        return needSuffix;
    }

    public void setNeedSuffix(String needSuffix) {
        this.needSuffix = needSuffix;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getHandshakeTimeout() {
        return handshakeTimeout;
    }

    public void setHandshakeTimeout(int handshakeTimeout) {
        this.handshakeTimeout = handshakeTimeout;
    }

    public int getChannelRpcTimeout() {
        return channelRpcTimeout;
    }

    public void setChannelRpcTimeout(int channelRpcTimeout) {
        this.channelRpcTimeout = channelRpcTimeout;
    }

    public int getRequestedHeartbeat() {
        return requestedHeartbeat;
    }

    public void setRequestedHeartbeat(int requestedHeartbeat) {
        this.requestedHeartbeat = requestedHeartbeat;
    }
}
