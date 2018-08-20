package com.paas.cache.jedis;

/**
 * Created on 2016/9/26.
 */
public class CacheConfig {

    private String businessKey;
    private String bizCode;
    private String authUrl;
    private String configPath;
    private String zkAdress;
    private JedisConfig jedisConfig;

    public CacheConfig(){}

    public CacheConfig(String bizKey, String authUrl, String configPath){
        this.businessKey = bizKey;
        this.authUrl = authUrl;
        this.configPath = configPath;
    }

    public String getBizCode() {
        return bizCode;
    }

    public void setBizCode(String bizCode) {
        this.bizCode = bizCode;
    }

    public String getBusinessKey() {
        return businessKey;
    }

    public void setBusinessKey(String businessKey) {
        this.businessKey = businessKey;
    }

    public String getAuthUrl() {
        return authUrl;
    }

    public void setAuthUrl(String authUrl) {
        this.authUrl = authUrl;
    }

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    public String getZkAdress() {
        return zkAdress;
    }

    public void setZkAdress(String zkAdress) {
        this.zkAdress = zkAdress;
    }

    public JedisConfig getJedisConfig() {
        return jedisConfig;
    }

    public void setJedisConfig(JedisConfig jedisConfig) {
        this.jedisConfig = jedisConfig;
    }
}
