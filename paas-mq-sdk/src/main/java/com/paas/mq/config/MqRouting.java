package com.paas.mq.config;


public class MqRouting {

    private String key = "";
    private String queue;
    private int count = 1;

    public MqRouting() {}

    public MqRouting(String queue) {
        this.queue = queue;
    }

    public MqRouting(String key, String queue) {
        this.key = key;
        this.queue = queue;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
