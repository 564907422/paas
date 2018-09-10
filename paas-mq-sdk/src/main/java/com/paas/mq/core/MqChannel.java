package com.paas.mq.core;

import com.paas.mq.IConfirm;
import com.paas.mq.IConsumer;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class MqChannel implements ShutdownListener, ConfirmListener {
    private static Logger log = LoggerFactory.getLogger(MqChannel.class);

    private MqConnection connection;
//    private ThreadLocal<Channel> targetLocal = new ThreadLocal<Channel>();
    private Channel target;
    private IConfirm confirm;
    private boolean bindDeclared = false;
    private int reconnect = 2; // 断开后每2s重连一次
    private volatile boolean stop = false;

    private Map<String, Boolean> cQueues = new HashMap<>();
    private Map<String, Boolean> pQueues = new HashMap<>();
    private Map<String, ConsumerInfo> consumers = new HashMap<>();


    public MqChannel(MqConnection connection, IConfirm confirm) {
        this.connection = connection;
        this.confirm = confirm;
    }

    public void consume(String queue, boolean autoAck, final IConsumer callback) throws IOException, TimeoutException {
        ConsumerInfo info = new ConsumerInfo(callback, queue, autoAck);
        consumers.put(queue, info);
        consume(info);
    }

    public void consume(ConsumerInfo info) throws IOException, TimeoutException {
        Channel target = getTarget();
        target.basicConsume(info.getQueue(), info.isAutoAck(),
                new DefaultConsumer(target) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body)
                            throws IOException
                    {
                        String routingKey = envelope.getRoutingKey();
                        try {
                            String contentType = properties.getContentType();
                            long deliveryTag = envelope.getDeliveryTag();
                            String msg = new String(body);
                            log.debug(" ---> routingKey: {}, contentType:{}, message:[{}]", routingKey, contentType, msg);
                            info.getCallback().handle(routingKey, contentType, msg);
                            if (!info.isAutoAck()) {
                                target.basicAck(deliveryTag, false);
                            }
                        } catch (Exception e){
                            log.error("consume error.", e);
                        } finally {
                        }
                    }
                });
    }



    public Channel getTarget() throws IOException, TimeoutException {
//        Channel target = targetLocal.get();
        if(target == null || !target.isOpen()){
            synchronized (this) {
                target = connection.getConnection().createChannel();
                if(confirm != null) {
                    target.confirmSelect();
                    target.addConfirmListener(this);
                }
                target.addShutdownListener(this);
//                targetLocal.set(target);
            }
        }
        return target;
    }

    private void reconsume() throws IOException, TimeoutException {
        getTarget();
        for(Map.Entry<String, Boolean> me : cQueues.entrySet()){
            consumerQueueDeclare(me.getKey(), me.getValue());
        }
        for(ConsumerInfo ci : consumers.values()){
            consume(ci);
        }
    }

    public void close() {
        try {
            Channel target = getTarget();
            if(target != null) {
                try {
                    target.close();
                } catch (Exception e) {
                    log.warn("close rabbitmq channel error.{}", e.getMessage());
                }
            }
        } catch (IOException e) {
            log.error("",e);
        } catch (TimeoutException e) {
            log.error("",e);
        }


    }

    public boolean isOpen(){
        try {
            Channel target = getTarget();
            return connection != null && connection.getConnection().isOpen() && target != null && target.isOpen();
        } catch (IOException e) {
            log.warn("valide channel error.{}", e.getMessage());
        } catch (TimeoutException e) {
            log.warn("valide channel error.{}", e.getMessage());
        }
        return false;
    }

    @Override
    public void shutdownCompleted(ShutdownSignalException cause) {
        this.bindDeclared = false;
        log.info("channel is shutdown. channel:{}, cause:{}",this, cause.getReason());
        // 订阅者重连，发布者使用对象池管理
        if(!isStop() && !cQueues.isEmpty()){
            reconnect();
        }
    }

    private void reconnect(){
        do {
            try {
                TimeUnit.SECONDS.sleep(reconnect);
            } catch (InterruptedException e1) {
                // Thread.currentThread().interrupt();
            }
            try {
                log.info("Trying to reconnect mq server. ");
                reconsume();
            } catch (Exception e) {
                log.warn("Reconnect failed.", e);
            }
        } while (connection == null || !connection.isOpen());
    }

    public void publish(String exchange, String routingKey, String message) throws IOException, TimeoutException {
        Channel target = getTarget();
        target.basicPublish(exchange, routingKey, null, message.getBytes());
    }

    public void publish(String exchange, String routingKey, String message, AMQP.BasicProperties props) throws IOException, TimeoutException {
        Channel target = getTarget();
        target.basicPublish(exchange, routingKey, props, message.getBytes());
    }

    @Override
    public void handleAck(long deliveryTag, boolean multiple) throws IOException {
        // TODO un implement
        log.debug("{} ack, multiple:{}.",deliveryTag, multiple);
    }

    @Override
    public void handleNack(long deliveryTag, boolean multiple) throws IOException {
        // TODO un implement
        log.debug("{} Nack, multiple:{}.",deliveryTag, multiple);
    }

    public void consumerQueueDeclare(String queue, boolean durable) throws IOException {
        log.debug(" ---> Consumer declare queue : {}.", queue);
        Channel target = null;
        try {
            target = getTarget();
        } catch (TimeoutException e) {
            log.error("",e);
        }
        cQueues.put(queue, durable);
        AMQP.Queue.DeclareOk do1 = target.queueDeclare(queue, durable, false, false, null);
        log.debug(" ---> Consumer Queue.DeclareOk : {}", do1);
    }

    public void producerQueueDeclare(String queue, boolean durable) throws IOException {
        log.debug(" ---> Producer declare queue : {}.", queue);
        Channel target = null;
        try {
            target = getTarget();
        } catch (TimeoutException e) {
            log.error("",e);
        }
        pQueues.put(queue, durable);
        AMQP.Queue.DeclareOk do1 = target.queueDeclare(queue, durable, false, false, null);
        log.debug(" ---> Producer Queue.DeclareOk : {}", do1);
    }

    public void exchangeDeclare(String exchange, String type, boolean durable) throws IOException {
        log.debug(" ---> declare exchange : {}, type: {}", exchange, type);
        Channel target = null;
        try {
            target = getTarget();
        } catch (TimeoutException e) {
            log.error("",e);
        }
        AMQP.Exchange.DeclareOk edo = target.exchangeDeclare(exchange, type, durable);
        log.debug(" ---> Exchange.DeclareOk:{}", edo);
    }

    public void queueBind(String exchange, String queue, String key) throws IOException {
        log.debug(" ---> declare bind : {}--{}-->{}", exchange, key, queue);
        Channel target = null;
        try {
            target = getTarget();
        } catch (TimeoutException e) {
            log.error("",e);
        }
        AMQP.Queue.BindOk rs1 = target.queueBind(queue, exchange, key);
        log.debug(" ---> Queue.BindOk : {}", rs1);
    }

    public boolean isBindDeclared() {
        return bindDeclared;
    }

    public void setBindDeclared(boolean bindDeclared) {
        this.bindDeclared = bindDeclared;
    }

    public int getReconnect() {
        return reconnect;
    }

    public void setReconnect(int reconnect) {
        this.reconnect = reconnect;
    }

    public boolean isStop() {
        return stop;
    }

    public void setStop(boolean stop) {
        this.stop = stop;
    }

    public static class ConsumerInfo{
        private IConsumer callback;
        private String queue;
        private boolean autoAck;

        public ConsumerInfo(IConsumer callback, String queue, boolean autoAck) {
            this.callback = callback;
            this.queue = queue;
            this.autoAck = autoAck;
        }

        public IConsumer getCallback() {
            return callback;
        }

        public void setCallback(IConsumer callback) {
            this.callback = callback;
        }

        public String getQueue() {
            return queue;
        }

        public void setQueue(String queue) {
            this.queue = queue;
        }

        public boolean isAutoAck() {
            return autoAck;
        }

        public void setAutoAck(boolean autoAck) {
            this.autoAck = autoAck;
        }
    }

}
