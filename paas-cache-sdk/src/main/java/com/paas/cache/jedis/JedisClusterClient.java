package com.paas.cache.jedis;

import com.paas.cache.ICacheClient;
import com.paas.cache.exception.CacheClientException;
import com.paas.commons.serialize.SerializerUtil;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisClusterException;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created on 2016/9/26.
 */
public class JedisClusterClient implements ICacheClient {
    protected static Logger log = LoggerFactory.getLogger(JedisClusterClient.class);

    private JedisCluster jedisCluster;
    private JedisConfig config;
    private GenericObjectPoolConfig poolConfig;

    public JedisClusterClient(JedisConfig config) {
        this.config = config;
        initPoolConfig();
        createCluster();
    }

    private void initPoolConfig() {
        poolConfig = new JedisPoolConfig();
        JedisConfig.PoolConfig conf = config.getConf();
        if (conf != null) {
            poolConfig.setMaxIdle(conf.getMaxIdle());
            poolConfig.setMaxTotal(conf.getMaxActive());
            poolConfig.setMinIdle(conf.getMinIdle());
            poolConfig.setTestOnBorrow(conf.getTestOnBorrow());
            poolConfig.setTestOnReturn(conf.getTestOnReturn());
            poolConfig.setMaxWaitMillis(conf.getMaxWait());
            poolConfig.setTestWhileIdle(conf.isTestWhileIdle());
        }
    }

    private void getCluster() {
        createCluster();
    }

    private void createCluster() {
        log.info(" ---> create jedis cluster begin ...");
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        try {
            for (String address : config.getServerArray()) {
                String[] ipAndPort = address.split(":");
                jedisClusterNodes.add(new HostAndPort(ipAndPort[0], Integer.parseInt(ipAndPort[1])));
                log.debug(" ---> jedis cluster address: {}", address);
            }
            if (config.isRedisNeedAuth()) {
                jedisCluster = new JedisCluster(jedisClusterNodes, config.getConf().getTimeout(), config.getConf().getSoTimeout(),
                        5, config.getServerInfo().getPassword(), poolConfig);
            } else {
                jedisCluster = new JedisCluster(jedisClusterNodes, config.getConf().getTimeout(), poolConfig);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        log.info(" ---> create jedis cluster down.");
    }

    protected boolean canConnection() {
        try {
            jedisCluster.get("ok");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
        return true;
    }


    public String set(String key, String value) {
        try {
            return jedisCluster.set(key, value);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return set(key, value);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public String getSet(String key, String value) {
        try {
            return jedisCluster.getSet(key, value);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return getSet(key, value);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public String setex(String key, int seconds, String value) {
        try {
            return jedisCluster.setex(key, seconds, value);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return setex(key, seconds, value);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public String get(String key) {
        try {
            return jedisCluster.get(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return get(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long del(String key) {
        try {
            return jedisCluster.del(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return del(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long del(String... keys) {
        try {
            return jedisCluster.del(keys);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return del(keys);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long expire(String key, int seconds) {
        try {
            return jedisCluster.expire(key, seconds);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return expire(key, seconds);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Long expireAt(String key, long seconds) {
        return expireAt(key.getBytes(), seconds);
    }

    public Long ttl(String key) {
        try {
            return jedisCluster.ttl(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return ttl(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public boolean exists(String key) {
        try {
            return jedisCluster.exists(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return exists(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long incr(String key) {
        try {
            return jedisCluster.incr(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return incr(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long incrBy(String key, long increment) {
        try {
            return jedisCluster.incrBy(key, increment);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return incrBy(key, increment);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long decr(String key) {
        try {
            return jedisCluster.decr(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return decr(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long decrBy(String key, long decrement) {
        try {
            return jedisCluster.decrBy(key, decrement);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return decrBy(key, decrement);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long lpush(String key, String... strings) {
        try {
            return jedisCluster.lpush(key, strings);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return lpush(key, strings);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long rpush(String key, String... strings) {
        try {
            return jedisCluster.rpush(key, strings);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return rpush(key, strings);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long llen(String key) {
        try {
            return jedisCluster.llen(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return llen(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public String lpop(String key) {
        try {
            return jedisCluster.lpop(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return lpop(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public String rpop(String key) {
        try {
            return jedisCluster.rpop(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return rpop(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public List<String> lrange(String key, long start, long end) {
        try {
            return jedisCluster.lrange(key, start, end);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return lrange(key, start, end);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public List<String> lrangeAll(String key) {
        try {
            return jedisCluster.lrange(key, 0, -1);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return lrange(key, 0, -1);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long hset(String key, String field, String value) {
        try {
            return jedisCluster.hset(key, field, value);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hset(key, field, value);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long hsetnx(String key, String field, String value) {
        try {
            return jedisCluster.hsetnx(key, field, value);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hsetnx(key, field, value);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public String hmset(String key, Map<String, String> hash) {
        try {
            return jedisCluster.hmset(key, hash);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hmset(key, hash);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public String hget(String key, String field) {
        try {
            return jedisCluster.hget(key, field);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hget(key, field);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public List<String> hmget(final String key, final String... fields) {
        try {
            return jedisCluster.hmget(key, fields);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hmget(key, fields);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Boolean hexists(String key, String field) {
        try {
            return jedisCluster.hexists(key, field);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hexists(key, field);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long hdel(String key, String... fields) {
        try {
            return jedisCluster.hdel(key, fields);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hdel(key, fields);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long hlen(String key) {
        try {
            return jedisCluster.hlen(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hlen(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Map<String, String> hgetAll(String key) {
        try {
            return jedisCluster.hgetAll(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hgetAll(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long sadd(String key, String... members) {
        try {
            return jedisCluster.sadd(key, members);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return sadd(key, members);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Set<String> smembers(String key) {
        try {
            return jedisCluster.smembers(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return smembers(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long srem(String key, String... members) {
        try {
            return jedisCluster.srem(key, members);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return srem(key, members);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long scard(String key) {
        try {
            return jedisCluster.scard(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return scard(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Set<String> sunion(String... keys) {
        try {
            return jedisCluster.sunion(keys);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return sunion(keys);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Set<String> sdiff(String... keys) {
        try {
            return jedisCluster.sdiff(keys);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return sdiff(keys);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long sdiffstore(String dstkey, String... keys) {
        try {
            return jedisCluster.sdiffstore(dstkey, keys);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return sdiffstore(dstkey, keys);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public String set(byte[] key, byte[] value) {
        try {
            return jedisCluster.set(key, value);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return set(key, value);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public String setex(byte[] key, int seconds, byte[] value) {
        try {
            return jedisCluster.setex(key, seconds, value);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return setex(key, seconds, value);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public byte[] get(byte[] key) {
        try {
            return jedisCluster.get(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return get(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long del(byte[] key) {
        try {
            return jedisCluster.del(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return del(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long del(byte[]... keys) {
        try {
            return jedisCluster.del(keys);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return del(keys);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long expire(byte[] key, int seconds) {
        try {
            return jedisCluster.expire(key, seconds);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return expire(key, seconds);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Long expireAt(byte[] key, long seconds) {
        try {
            return jedisCluster.expireAt(key, seconds);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return expireAt(key, seconds);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long ttl(byte[] key) {
        try {
            return jedisCluster.ttl(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return ttl(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public boolean exists(byte[] key) {
        try {
            return jedisCluster.exists(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return exists(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long incr(byte[] key) {
        try {
            return jedisCluster.incr(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return incr(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long incrBy(byte[] key, long increment) {
        try {
            return jedisCluster.incrBy(key, increment);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return incrBy(key, increment);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long decr(byte[] key) {
        try {
            return jedisCluster.decr(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return decr(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long decrBy(byte[] key, long decrement) {
        try {
            return jedisCluster.decrBy(key, decrement);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return decrBy(key, decrement);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long lpush(byte[] key, byte[]... strings) {
        try {
            return jedisCluster.lpush(key, strings);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return lpush(key, strings);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long rpush(byte[] key, byte[]... strings) {
        try {
            return jedisCluster.rpush(key, strings);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return rpush(key, strings);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long llen(byte[] key) {
        try {
            return jedisCluster.llen(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return llen(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public byte[] lpop(byte[] key) {
        try {
            return jedisCluster.lpop(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return lpop(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public byte[] rpop(byte[] key) {
        try {
            return jedisCluster.rpop(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return rpop(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public List<byte[]> lrange(byte[] key, long start, long end) {
        try {
            return jedisCluster.lrange(key, start, end);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return lrange(key, start, end);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public List<byte[]> lrangeAll(byte[] key) {
        try {
            return jedisCluster.lrange(key, 0, -1);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return lrange(key, 0, -1);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long hset(byte[] key, byte[] field, byte[] value) {
        try {
            return jedisCluster.hset(key, field, value);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hset(key, field, value);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        try {
            return jedisCluster.hsetnx(key, field, value);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hsetnx(key, field, value);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Long setnx(byte[] key, byte[] value) {
        try {
            return jedisCluster.setnx(key, value);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return jedisCluster.setnx(key, value);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Long setnx(String key, String value) {
        return setnx(key.getBytes(), value.getBytes());
    }

    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        try {
            return jedisCluster.hmset(key, hash);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hmset(key, hash);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public byte[] hget(byte[] key, byte[] field) {
        try {
            return jedisCluster.hget(key, field);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hget(key, field);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public List<byte[]> hmget(final byte[] key, final byte[]... fields) {
        try {
            return jedisCluster.hmget(key, fields);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hmget(key, fields);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Boolean hexists(byte[] key, byte[] field) {
        try {
            return jedisCluster.hexists(key, field);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hexists(key, field);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long hdel(byte[] key, byte[]... fields) {
        try {
            return jedisCluster.hdel(key, fields);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hdel(key, fields);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long hlen(byte[] key) {
        try {
            return jedisCluster.hlen(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hlen(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Map<byte[], byte[]> hgetAll(byte[] key) {
        try {
            return jedisCluster.hgetAll(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hgetAll(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long sadd(byte[] key, byte[]... members) {
        try {
            return jedisCluster.sadd(key, members);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return sadd(key, members);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Set<byte[]> smembers(byte[] key) {
        try {
            return jedisCluster.smembers(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return smembers(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long srem(byte[] key, byte[]... members) {
        try {
            return jedisCluster.srem(key, members);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return srem(key, members);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long scard(byte[] key) {
        try {
            return jedisCluster.scard(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return scard(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Set<byte[]> sunion(byte[]... keys) {
        try {
            return jedisCluster.sunion(keys);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return sunion(keys);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Set<byte[]> sdiff(byte[]... keys) {
        try {
            return jedisCluster.sdiff(keys);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return sdiff(keys);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    public Long sdiffstore(byte[] dstkey, byte[]... keys) {
        try {
            return jedisCluster.sdiffstore(dstkey, keys);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return sdiffstore(dstkey, keys);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Long hincrBy(String key, String field, long value) {
        try {
            return jedisCluster.hincrBy(key, field, value);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hincrBy(key, field, value);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Double incrByFloat(String key, double value) {
        try {
            return jedisCluster.incrByFloat(key, value);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return incrByFloat(key, value);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Double hincrByFloat(String key, String field, double value) {
        try {
            return jedisCluster.hincrByFloat(key, field, value);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return hincrByFloat(key, field, value);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Long lrem(String key, long count, String value) {
        try {
            return jedisCluster.lrem(key, count, value);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return lrem(key, count, value);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Long lrem(byte[] key, long count, byte[] value) {
        try {
            return jedisCluster.lrem(key, count, value);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return lrem(key, count, value);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Long zadd(String key, double score, String member) {
        try {
            return jedisCluster.zadd(key, score, member);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zadd(key, score, member);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) {
        try {
            return jedisCluster.zadd(key, scoreMembers);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zadd(key, scoreMembers);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Long zcount(final String key, final double min, final double max) {
        try {
            return jedisCluster.zcount(key, min, max);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zcount(key, min, max);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Long zcount(final String key, final String min, final String max) {
        try {
            return jedisCluster.zcount(key, min, max);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zcount(key, min, max);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Double zincrby(final String key, final double score, final String member) {
        try {
            return jedisCluster.zincrby(key, score, member);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zincrby(key, score, member);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Set<String> zrange(final String key, final long start, final long end) {
        try {
            return jedisCluster.zrange(key, start, end);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zrange(key, start, end);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Set<String> zrangeByScore(final String key, final double min, final double max) {
        try {
            return jedisCluster.zrangeByScore(key, min, max);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zrangeByScore(key, min, max);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Set<String> zrangeByScore(final String key, final String min, final String max) {
        try {
            return jedisCluster.zrangeByScore(key, min, max);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zrangeByScore(key, min, max);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Set<String> zrangeByScore(final String key, final double min, final double max, final int offset, int count) {
        try {
            return jedisCluster.zrangeByScore(key, min, max, offset, count);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zrangeByScore(key, min, max, offset, count);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Set<String> zrevrange(final String key, final long start, final long end) {
        try {
            return jedisCluster.zrevrange(key, start, end);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zrevrange(key, start, end);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final double max, final double min) {
        try {
            return jedisCluster.zrevrangeByScore(key, max, min);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zrevrangeByScore(key, max, min);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final String max, final String min) {
        try {
            return jedisCluster.zrevrangeByScore(key, max, min);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zrevrangeByScore(key, max, min);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final double max, final double min, final int offset, int count) {
        try {
            return jedisCluster.zrevrangeByScore(key, max, min, offset, count);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zrevrangeByScore(key, max, min, offset, count);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final String max, final String min, final int offset, int count) {
        try {
            return jedisCluster.zrevrangeByScore(key, max, min, offset, count);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zrevrangeByScore(key, max, min, offset, count);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Long zrevrank(final String key, final String member) {
        try {
            return jedisCluster.zrevrank(key, member);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zrevrank(key, member);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Long zrem(final String key, final String... member) {
        try {
            return jedisCluster.zrem(key, member);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zrem(key, member);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Long zremrangeByRank(final String key, final long start, final long end) {
        try {
            return jedisCluster.zremrangeByRank(key, start, end);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zremrangeByRank(key, start, end);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Long zremrangeByScore(final String key, final double start, final double end) {
        try {
            return jedisCluster.zremrangeByScore(key, start, end);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zremrangeByScore(key, start, end);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Long zremrangeByScore(final String key, final String start, final String end) {
        try {
            return jedisCluster.zremrangeByScore(key, start, end);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zremrangeByScore(key, start, end);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Boolean sismember(String key, String object) {
        try {
            return jedisCluster.sismember(key, object);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return sismember(key, object);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    /**
     * 设置key的过期时间。如果key已过期，将会被自动删除。
     *
     * @param key          cache中存储数据的key
     * @param milliseconds 过期的秒数
     * @return 被设置key的数量
     * @
     */
    @Override
    public Long pexpire(String key, long milliseconds) {
        if (milliseconds < 1) {
            throw new CacheClientException("非法参数!");
        }
        try {
            return jedisCluster.pexpire(key, milliseconds);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return pexpire(key, milliseconds);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Boolean ltrim(String listKey, long start, long stop) {
        if (start < 0) {
            throw new CacheClientException("非法参数!");
        }
        try {
            String rtu = jedisCluster.ltrim(listKey, start, stop);
            return "OK".equals(rtu);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return ltrim(listKey, start, stop);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        try {
            return jedisCluster.zrevrangeByScoreWithScores(key, max, min, offset, count);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        try {
            return jedisCluster.zrevrangeByScoreWithScores(key, max, min, offset, count);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        try {
            return jedisCluster.zrevrangeByScoreWithScores(key, max, min);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zrevrangeByScoreWithScores(key, max, min);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public Long zcard(String key) {
        try {
            return jedisCluster.zcard(key);
        } catch (JedisClusterException jcException) {
            getCluster();
            if (canConnection()) {
                return zcard(key);
            }
            log.error(jcException.getMessage(), jcException);
            throw new CacheClientException(jcException);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
        }
    }

    @Override
    public String setObjectEx(byte[] key, int seconds, Object serializable) {
        if (!(serializable instanceof Serializable)) {
            throw new CacheClientException();
        }
//		byte[] valueser = SerializerUtil.serialize((Serializable)serializable);
        byte[] valueser = SerializerUtil.defaultSerialize((Serializable) serializable);
        log.debug("---> setObjectEx value size{}.", valueser.length);
//		byte[] keyser = SerializerUtil.serialize(key);
        return setex(key, seconds, valueser);
    }

    @Override
    public Object getObject(byte[] key) {
//		byte[] keyser = SerializerUtil.serialize(key);
        byte[] result = get(key);
        if (result == null)
            return null;
//		return SerializerUtil.deserialize(result);
        return SerializerUtil.defaultDeserialize(result);
    }

}
