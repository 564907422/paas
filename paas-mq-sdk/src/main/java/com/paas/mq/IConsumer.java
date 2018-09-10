package com.paas.mq;


public interface IConsumer {

    /**
     * 处理消息
     * @param routingKey
     * @param contentType
     * @param message
     * @return 是否处理成功
     */
    boolean handle(String routingKey, String contentType, String message);

}
