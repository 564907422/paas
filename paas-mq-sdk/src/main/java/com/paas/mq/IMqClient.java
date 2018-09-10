package com.paas.mq;


public interface IMqClient {

    /**
     * 消费服务
     * @param callback
     */
    void consume(IConsumer callback);

    void publish(String routKey, String message);

    /**
     * 发布消息
     * @param message
     */
    void publish(String message);

}
