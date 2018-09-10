package com.paas.mq.core;

import com.paas.mq.IConfirm;
import com.paas.mq.config.MqConfig;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;


public class MqConnection implements ShutdownListener  {
    private Logger log = LoggerFactory.getLogger(MqConnection.class);

    private MqConfig config;
    private ConnectionFactory factory;
    private Connection connection;

    public MqConnection(MqConfig config, ConnectionFactory factory) {
        this.config = config;
        this.factory = factory;
    }

    public MqChannel createChannel(IConfirm confirm) throws IOException, TimeoutException {
        getConnection();
        MqChannel channel = new MqChannel(this, confirm);
        channel.getTarget();
        return channel;
    }

    public MqChannel createChannel() throws IOException, TimeoutException {
        return createChannel(null);
    }

    public Connection getConnection() throws IOException, TimeoutException {
        if(!isOpen()) {
            synchronized (this) {
                if (!isOpen()) {
                    connection = factory.newConnection();
                    connection.addShutdownListener(this);
                }
            }
        }
        return connection;
    }

    public MqConfig getConfig() {
        return config;
    }

    public void setConfig(MqConfig config) {
        this.config = config;
    }

    public ConnectionFactory getFactory() {
        return factory;
    }

    public void setFactory(ConnectionFactory factory) {
        this.factory = factory;
    }

    public boolean isOpen(){
        return this.connection != null && this.connection.isOpen();
    }

    public void close() {
        if(this.connection != null){
            try {
                this.connection.close();
            } catch (IOException e) {
                log.warn("close rabbitmq connection error.{}", e.getMessage());
            }
        }
    }

    @Override
    public void shutdownCompleted(ShutdownSignalException cause) {
        log.warn("connection is shutdown.this:{}, cause:{}",this, cause.getReason());
    }

}
