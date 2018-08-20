package com.paas.cache.exception;

import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * Created by Jimmy on 2016/9/23.
 */
public class CacheClientException extends RuntimeException{

    public CacheClientException(){
        super();
    }

    public CacheClientException(Exception e){
        super(e);
    }

    public CacheClientException(JedisConnectionException jce){
        super(jce);
    }

}
