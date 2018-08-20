package com.paas.cache.jedis;

import java.util.Arrays;

/**
 * Created on 2016/9/23.
 */
public class JedisConfig {

    private String servers;
    private String[] serverArray;
    private ServerInfo serverInfo;
    private PoolConfig conf;
    private Integer warnTime = 1000;

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public ServerInfo getServerInfo() {
        return serverInfo;
    }

    public String[] getServerArray() {
        if(serverArray == null){
            serverArray = servers.split(",");
        }
        return serverArray;
    }

    public Integer getWarnTime() {
        return warnTime;
    }

    public void setWarnTime(Integer warnTime) {
        this.warnTime = warnTime;
    }

    public void setServerArray(String[] serverArray) {
        this.serverArray = serverArray;
    }

    public void setServerInfo(ServerInfo serverInfo) {
        this.serverInfo = serverInfo;
    }

    public boolean isRedisNeedAuth(){
        if(serverInfo != null && serverInfo.getPassword() != null && serverInfo.getPassword().trim().length() > 0){
            return true;
        }
        return false;
    }

    public PoolConfig getConf() {
        return conf;
    }

    public void setConf(PoolConfig conf) {
        this.conf = conf;
    }

    public static class ServerInfo{
        private String password;

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    public static class PoolConfig {
        private Integer maxIdle;
        private Boolean testOnBorrow;
        private Boolean testOnReturn;
        private Integer maxWait;
        private Integer maxActive;
        private Integer timeout = 15000;

        public Integer getMaxIdle() {
            return maxIdle;
        }

        public void setMaxIdle(Integer maxIdle) {
            this.maxIdle = maxIdle;
        }

        public Boolean getTestOnBorrow() {
            return testOnBorrow;
        }

        public void setTestOnBorrow(Boolean testOnBorrow) {
            this.testOnBorrow = testOnBorrow;
        }

        public Boolean getTestOnReturn() {
            return testOnReturn;
        }

        public void setTestOnReturn(Boolean testOnReturn) {
            this.testOnReturn = testOnReturn;
        }

        public Integer getMaxWait() {
            return maxWait;
        }

        public void setMaxWait(Integer maxWait) {
            this.maxWait = maxWait;
        }

        public Integer getMaxActive() {
            return maxActive;
        }

        public void setMaxActive(Integer maxActive) {
            this.maxActive = maxActive;
        }

        public Integer getTimeout() {
            return timeout;
        }

        public void setTimeout(Integer timeout) {
            this.timeout = timeout;
        }

        @Override
        public String toString() {
            return "PoolConfig{" +
                    "maxIdle=" + maxIdle +
                    ", testOnBorrow=" + testOnBorrow +
                    ", testOnReturn=" + testOnReturn +
                    ", maxWait=" + maxWait +
                    ", maxActive=" + maxActive +
                    ", timeout=" + timeout +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "JedisConfig{" +
                "servers='" + servers + '\'' +
                ", serverArray=" + Arrays.toString(serverArray) +
                ", serverInfo=" + serverInfo +
                ", conf=" + conf +
                '}';
    }

}
