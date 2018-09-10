package com.paas.mq.core;

import com.paas.mq.IConfirm;
import com.paas.mq.IConsumer;
import com.paas.mq.IMqClient;
import com.paas.mq.config.*;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created on 2016/10/28.
 */
public class RabbitMqClient implements IMqClient, ConfigListener {

    private static Logger log = LoggerFactory.getLogger(RabbitMqClient.class);

    private MqConfig config;
    private ConnectionFactory factory;
    private MqConnection consumerConnection;
    private List<MqChannel> consumerChannels = new ArrayList<>();
    private IConsumer consumeCallback;
    private MqConnection producerConnection;
    private GenericObjectPoolConfig producerPoolConfig;
    private GenericObjectPool<MqChannel> producerChannel;
    private final Map<String, AtomicLong> mesIndex = new ConcurrentHashMap<>();

    private Object la = new Object();
    private Object lb = new Object();
    private ExecutorService executorService = new ThreadPoolExecutor(8,300,
            60L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(800));


    public RabbitMqClient(MqConfig config){
        this.config = config;
        initConnectionFactory();
        ConfigRegister.registry(config.getZkConfigPath(), this);
    }

    private synchronized void initConsumerConnection(){
        if(consumerConnection == null) {
            consumerConnection = new MqConnection(config, factory);
        }
    }

    private synchronized void initProducerConnection(){
        producerConnection = new MqConnection(config, factory);
        if (producerPoolConfig == null) {
            producerPoolConfig = new GenericObjectPoolConfig();
            producerPoolConfig.setTestOnBorrow(true);
            producerPoolConfig.setTestOnReturn(true);
            producerPoolConfig.setMaxIdle(config.getMaxIdle());
            producerPoolConfig.setMaxTotal(config.getMaxTotal());
            producerPoolConfig.setMaxWaitMillis(config.getMaxWaitMillis());
        }
        producerChannel = new GenericObjectPool<>(new ChannelFactory(producerConnection, config.getConfirm()), producerPoolConfig);
    }

    private  void  initConnectionFactory(){
        synchronized(la) {
            if (factory == null) {
                try {
                    log.debug("init ConnectionFactory ...");
                    factory = new ConnectionFactory();
                    //        factory.setUri("amqp://bbtree:hyww@1z3@120.26.125.193:55673/virtualHost");
                    factory.setHost(config.getHost());
                    factory.setPort(config.getPort());
                    factory.setUsername(config.getUsername());
                    factory.setPassword(config.getPassword());
                    factory.setVirtualHost(config.getVhost());
                    //对应着网络层Socket实例connect()方法中的timeout参数，指的是完成TCP三次握手的超时时间 单位毫秒  默认60s
                    factory.setConnectionTimeout(config.getConnectionTimeout());
                    //而读取超时是从socket中读取字节流的等待时间 单位毫秒  默认10s
                    factory.setHandshakeTimeout(config.getHandshakeTimeout());
                    //channel 读取字节流时间 单位毫秒  默认没有设置
                    factory.setChannelRpcTimeout(config.getChannelRpcTimeout());

                    // connection that will recover automatically
                    factory.setAutomaticRecoveryEnabled(false);
                    // attempt recovery every 5 seconds
                    //factory.setNetworkRecoveryInterval(5000);
                    // topology recovery
                    //factory.setTopologyRecoveryEnabled(true);
                    // set the heartbeat timeout to 30 seconds
                    factory.setRequestedHeartbeat(config.getRequestedHeartbeat());
                    //TODO
//                factory.setRequestedChannelMax(5*config.getMaxTotal());
//                factory.setRequestedFrameMax(config.getMaxTotal());

                    //nio
//                factory.useNio();
                    log.debug("init ConnectionFactory done.");
                }catch (Exception e){
                    log.error("init ConnectionFactory error---",e);
                    throw e;
                }
            }
        }
    }

    @Override
    public void consume(IConsumer callback) {
        if(consumerConnection == null){
            initConsumerConnection();
        }
        try {
            doConsume(consumerConnection, consumerChannels, callback);
        } catch (IOException e) {
            throw new RuntimeException(" --> consume error.", e);
        } catch (TimeoutException e) {
            throw new RuntimeException(" --> consume timeout error.", e);
        }
    }

    private void doConsume(MqConnection conn, List<MqChannel> channels, IConsumer callback) throws IOException, TimeoutException {
        synchronized (lb) {
            //TODO
            consumeCallback = callback;
            for (int i = 0; i < config.getConsumerThreads(); i++) {
                MqChannel cc = conn.createChannel();
                log.debug(" --> created channel: {}", cc);
                for (String queue : config.getQueues()) {
                    String envqueue = getEnvQueue(queue);
                    log.debug(" --> consumerQueueDeclare channel: {},queue :{}", cc, envqueue);
                    cc.consumerQueueDeclare(envqueue, config.isDurable());
                }
                channels.add(cc);
            }
            log.warn(" --> created consume channels: {}", channels.size());
        }
        for(MqChannel cc:channels) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        cc.getTarget();
                        for (String queue : config.getQueues()) {
                            String envqueue = getEnvQueue(queue);
                            cc.consume(envqueue, config.isAutoAck(), callback);
                        }
                    } catch (IOException e) {
                        log.error(" --> consume ",e);
                    } catch (TimeoutException e) {
                        log.error(" --> consume ",e);
                    } catch (Exception e) {
                        log.error(" --> consume ",e);
                    }
                }});

        }



//        for(MqChannel cc:channels) {
//            cc.getTarget();
//            for (String queue : config.getQueues()) {
//                cc.consume(queue, config.isAutoAck(), callback);
//            }
//        }



    }

    public void consumerQueueDeclare(MqChannel channel) throws IOException, TimeoutException {
        // queue 声明
        log.info(" ---> Consumer declare queues ...");
        for(String queue : config.getQueues()){
            channel.consumerQueueDeclare(queue, config.isDurable());
        }
    }

    @Override
    public void publish(String message){
        publish(null, message);
    }

    @Override
    public void publish(String routKey, String message){
        if(producerConnection == null){
            initProducerConnection();
        }
        try {
            doPublish(routKey, message);
        } catch (IOException e) {
            throw new RuntimeException(" --> publish error.", e);
        } catch (TimeoutException e) {
            throw new RuntimeException(" --> publish timeout error.", e);
        } catch (Exception e){
            throw new RuntimeException(" --> publish error.", e);
        } finally {
        }
    }


    private void doPublish(String routingKey, String message) throws IOException, TimeoutException, Exception {
        // MqChannel cc = producerConnection.createChannel();
        MqChannel cc = producerChannel.borrowObject();
        log.debug("doPublish,get channel from pool: {}", cc);
        try {
            if(!cc.isBindDeclared()) {
                bindDeclare(cc);
            }
            String envexchange = getEnvExcehange();
            Set<String> keys = getPublishKeys(routingKey);
            for(String key : keys){
                cc.publish(envexchange, key, message);
            }
        } catch (Exception e){
            log.error("",e);
            throw e;
        }  finally {
            producerChannel.returnObject(cc);
        }
    }

    private Set<String> getPublishKeys(String routingKey){
        List<MqRouting> routings = null;
        if(routingKey == null){
            routings = config.getRoutings();
        }else{
            routings = config.getRoutingMap().get(routingKey);
        }
        if(routings == null){
            throw new RuntimeException("not found routing key: "+routingKey);
        }
        Set<String> set = new HashSet<>();
        for(MqRouting mr : routings){
            if(mr.getCount() == 1){
                set.add(mr.getKey());
            }else if(mr.getCount() > 1){
                long index = mesIndex.get(mr.getKey()).incrementAndGet();
                set.add(mr.getKey()+"_"+(index%mr.getCount()));
            }
        }
        return set;
    }

    public void bindDeclare(MqChannel channel) throws IOException, TimeoutException {
        String envexchange = getEnvExcehange();

        // exchange 声明
        channel.exchangeDeclare(envexchange, config.getType(), config.isDurable());

        // queue declare
        for(String queue : config.getQueues()){
            String envqueue = getEnvQueue(queue);
            if(envqueue!=null&&envqueue.length()>0) {
                channel.producerQueueDeclare(envqueue, config.isDurable());
            }else{
                log.error("---> bindDeclare  producerQueueDeclare envqueue is null. ");
            }
        }

        // bind declare
        List<MqRouting> mrs = config.getRoutings();
        for(MqRouting mr : mrs){
            if(mr.getCount() == 1){
                String envqueue = getEnvQueue(mr.getQueue());
                if(envqueue!=null&&envqueue.length()>0) {
                    channel.queueBind(envexchange, envqueue, mr.getKey());
                }else{
                    log.error("---> bindDeclare  queueBind envqueue is null. ");
                }
            }else if(mr.getCount() > 1){
                for(int i=0; i<mr.getCount(); i++){
                    String envqueue = getEnvQueue(mr.getQueue());
                    if(envqueue!=null&&envqueue.length()>0) {
                        channel.queueBind(envexchange, envqueue+"_"+i, mr.getKey()+"_"+i);
                    }else{
                        log.error("---> bindDeclare  queueBind envqueue is null. ");
                    }
                }
                if(!mesIndex.containsKey(mr.getKey())) {
                    mesIndex.put(mr.getKey(), new AtomicLong(0));
                }
            }
        }
        channel.setBindDeclared(true);
    }

    private String getEnvQueue(String queue) {
//        String env = EnvBean.getEnv();
//        if("local".equals(env)||"prod".equals(env)){
            //不ping
            return queue;
//        }else{
//            String needSuffix = config.getNeedSuffix()==null?"true":config.getNeedSuffix();
//            if("true".equals(needSuffix)){
//                return env+"_"+queue;
//            }
//        }
//        return queue;
    }

    private String getEnvExcehange() {
//        String env = EnvBean.getEnv();
        String envEx = config.getExchange();
//        if("local".equals(env)||"prod".equals(env)){
            //不ping
            return envEx;
//        }else{
//            String needSuffix = config.getNeedSuffix()==null?"true":config.getNeedSuffix();
//            if("true".equals(needSuffix)){
//                return env+"_"+envEx;
//            }
//        }
//        return envEx;
    }

    public void destory(){
        consumerChannels.stream().forEach(mc -> mc.close());
        if(producerChannel != null){
            producerChannel.close();
        }
        if(consumerConnection != null){
            consumerConnection.close();
        }
        if(producerConnection != null){
            producerConnection.close();
        }
    }

    @Override
    public void changed(String path) {
        log.debug(" ===> config changed in zk, path: {}", path);
        config = ConfigFactory.getMqConfig(config.getServiceId(), config.getAuthUrl());
        // 重设Producer
        reproduce();
        // 重新注册Consumer
        reconsume();
    }

    private synchronized void reproduce(){
        if(producerConnection == null){ // 没有启动生产者
            return;
        }
        MqConnection oldConn = producerConnection;
        GenericObjectPool<MqChannel> oldPool = producerChannel;
        producerPoolConfig = null;
        producerConnection = null;
        producerChannel = null;
        initProducerConnection();

        if(oldPool != null) {
            oldPool.close();
        }
        if(oldConn != null) {
            oldConn.close();
        }
    }

    private synchronized void reconsume(){
        if(consumerConnection == null){ // 没有启用订阅
            return;
        }
        while (true){
            try {
                doReconsume();
                log.info(" --> reconsum Success.");
                break;
            } catch (Exception e) {
                log.warn(" --> reconsum error.", e);
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e1) {

                }
            }
        }
    }

    private void doReconsume() throws Exception {
        // 获取新的配置
//        ZKClient zkClient = ConfigRegister.getClient(config.getZkConfigPath());
//        String zkConfig = zkClient.getNodeData(config.getZkConfigPath());
//        log.info(" ---> reset mq zk config: {}", zkConfig);
//        config = ConfigFactory.parseMqConfig(zkConfig);
        // 初始化新的连接
        MqConnection newConn = new MqConnection(config, factory);
        List<MqChannel> newChannels = new ArrayList<>();
        // 重新订阅
        doConsume(newConn, newChannels, consumeCallback);

        MqConnection oldConn = consumerConnection;
        List<MqChannel> oldChannels = consumerChannels;
        consumerConnection = newConn;
        consumerChannels = newChannels;
        // 释放原来的连接
        oldChannels.stream().forEach(mc -> { mc.setStop(true); mc.close(); });
        if(oldConn != null){
            oldConn.close();
        }
    }

    private class ChannelFactory implements PooledObjectFactory<MqChannel> {

        private MqConnection connection;
        private IConfirm confirm;

        public ChannelFactory(MqConnection connection, IConfirm confirm){
            this.connection = connection;
            this.confirm = confirm;
        }

        @Override
        public PooledObject<MqChannel> makeObject() throws Exception {
            MqChannel channel = connection.createChannel(confirm);
            log.debug("make channel in pool: {}", channel);
            return new DefaultPooledObject<>(channel);
        }

        @Override
        public void destroyObject(PooledObject<MqChannel> p) throws Exception {
            log.debug("destroy channel in pool: {}", p.getObject().isOpen());
            p.getObject().close();
        }

        @Override
        public boolean validateObject(PooledObject<MqChannel> p) {
            log.debug("validate channel in pool: {}", p.getObject().isOpen());
            return p.getObject().isOpen();
        }

        @Override
        public void activateObject(PooledObject<MqChannel> p) throws Exception {
            // do nothing
        }

        @Override
        public void passivateObject(PooledObject<MqChannel> p) throws Exception {
            // do nothing
        }
    }

}
