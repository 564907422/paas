package com.paas.cache.jedis;

import com.paas.cache.ICacheClient;
import com.paas.cache.exception.CacheClientException;
import com.paas.commons.serialize.SerializerUtil;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created on 2016/9/23.
 */
public class JedisClient implements ICacheClient {
    protected static Logger log = LoggerFactory.getLogger(JedisClient.class);
    private JedisPool cachePool;
    private JedisConfig config;

    private GenericObjectPoolConfig poolConfig;
    private static Lock lock = new ReentrantLock();


    public JedisConfig getConfig() {
        return config;
    }

    public JedisClient(JedisConfig config) {
        this.config = config;
        initPoolConfig();
        initPool();
    }

    private void initPoolConfig() {
        poolConfig = new JedisPoolConfig();
        JedisConfig.PoolConfig conf = config.getConf();
        if (conf != null) {
            poolConfig.setMinIdle(conf.getMinIdle());
            poolConfig.setMaxIdle(conf.getMaxIdle());
            poolConfig.setMaxTotal(conf.getMaxActive());
            poolConfig.setTestOnBorrow(conf.getTestOnBorrow());
            poolConfig.setTestOnReturn(conf.getTestOnReturn());
            poolConfig.setMaxWaitMillis(conf.getMaxWait());
            poolConfig.setMinEvictableIdleTimeMillis(conf.getMinEvictableIdleTimeMillis());
            poolConfig.setTestWhileIdle(conf.isTestWhileIdle());
        }
    }

    protected final void createPool() {
        if (!canConnection()) {
            lock.lock();
            try {
                if (!canConnection()) {
                    log.info(" ---> Create JedisPool Begin ...");
                    String[] hostArr = config.getServers().split(":");
                    JedisPool oldPool = cachePool;
                    if (config.isRedisNeedAuth()) {
                        cachePool = new JedisPool(poolConfig, hostArr[0], Integer.parseInt(hostArr[1]),
                                config.getConf().getTimeout(), config.getServerInfo().getPassword());
                    } else {
                        cachePool = new JedisPool(poolConfig, hostArr[0], Integer.parseInt(hostArr[1]));
                    }
                    destroyPool(oldPool);
                    if (canConnection()) {
                        log.info(" ---> Redis Server Info: {}", config.getServers());
                    }
                    log.info(" ---> Create JedisPool Done. {}", cachePool);
                }
            } catch (Exception e) {
                throw new CacheClientException(e);
            } finally {
                lock.unlock();
            }

        }
    }

    // destroy origin pool
    private void destroyPool(JedisPool oldPool) {
        if (oldPool != null) {
            try {
                oldPool.destroy();
            } catch (Exception e) {
                log.warn("destroy origin pool fail.", e);
            }
        }
    }

    protected final boolean canConnection() {
        if (cachePool == null) {
            return false;
        }
        try (Jedis jedis = getJedis()) {
            jedis.connect();
            jedis.get("ok");
            return true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        }
    }

    protected Jedis getJedis() {
        Jedis jedis = cachePool.getResource();
        if (config.getDb() != null && config.getDb().intValue() > 0) {
            jedis.select(config.getDb());
        }
        return jedis;
    }

    private synchronized void initPool() {
        if (cachePool != null) {
            return;
        }
        createPool();
    }

    public void destroy() {
        if (cachePool != null) {
            cachePool.destroy();
        }
    }

    void returnResource(Jedis jedis) {
        jedis.close();
    }

    public String set(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.set(key, value);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return set(key, value);
            } else {
                log.error(jedisException.getMessage(), jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }

    }

    public String getSet(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.getSet(key, value);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return getSet(key, value);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public String setex(String key, int seconds, String value) {
        if (seconds <= 0 || key == null || key.length() == 0) {
            throw new CacheClientException("参数无效");
        }
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.setex(key, seconds, value);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return setex(key, seconds, value);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public String get(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.get(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return get(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long del(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.del(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return del(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long hincrBy(String key, String field, long value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hincrBy(key, field, value);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hincrBy(key, field, value);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Double incrByFloat(String key, double value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.incrByFloat(key, value);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return incrByFloat(key, value);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Double hincrByFloat(String key, String field, double value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hincrByFloat(key, field, value);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hincrByFloat(key, field, value);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long del(String... keys) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.del(keys);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return del(keys);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long expire(String key, int seconds) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.expire((key), seconds);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return expire(key, seconds);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Long expireAt(String key, long seconds) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.expireAt((key), seconds);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return expireAt(key, seconds);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long ttl(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.ttl(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return ttl(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public boolean exists(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.exists(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return exists(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long incr(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.incr(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return incr(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long incrBy(String key, long increment) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.incrBy(key, increment);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return incrBy(key, increment);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long decr(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.decr(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return decr(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long decrBy(String key, long decrement) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.decrBy(key, decrement);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return decrBy(key, decrement);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long lpush(String key, String... strings) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.lpush(key, strings);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return lpush(key, strings);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long rpush(String key, String... strings) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.rpush(key, strings);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return rpush(key, strings);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long llen(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.llen(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return llen(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public String lpop(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.lpop(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return lpop(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public String rpop(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.rpop(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return rpop(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public List<String> lrange(String key, long start, long end) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.lrange(key, start, end);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return lrange(key, start, end);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public List<String> lrangeAll(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.lrange(key, 0, -1);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return lrangeAll(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long hset(String key, String field, String value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hset(key, field, value);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hset(key, field, value);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long hsetnx(String key, String field, String value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hsetnx(key, field, value);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hsetnx(key, field, value);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public String hmset(String key, Map<String, String> hash) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hmset(key, hash);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hmset(key, hash);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public String hget(String key, String field) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hget(key, field);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hget(key, field);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public List<String> hmget(final String key, final String... fields) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hmget(key, fields);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hmget(key, fields);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Boolean hexists(String key, String field) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hexists(key, field);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hexists(key, field);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long hdel(String key, String... fields) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hdel(key, fields);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hdel(key, fields);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long hlen(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hlen(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hlen(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Map<String, String> hgetAll(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hgetAll(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hgetAll(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long sadd(String key, String... members) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.sadd(key, members);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return sadd(key, members);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Set<String> smembers(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.smembers(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return smembers(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long srem(String key, String... members) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.srem(key, members);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return srem(key, members);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long scard(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.scard(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return scard(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Set<String> sunion(String... keys) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.sunion(keys);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return sunion(keys);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Set<String> sdiff(String... keys) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.sdiff(keys);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return sdiff(keys);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long sdiffstore(String dstkey, String... keys) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.sdiffstore(dstkey, keys);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return sdiffstore(dstkey, keys);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public String set(byte[] key, byte[] value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.set(key, value);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return set(key, value);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public String setex(byte[] key, int seconds, byte[] value) {
        if (seconds <= 0 || key == null || key.length == 0) {
            throw new CacheClientException("参数无效");
        }
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.setex(key, seconds, value);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return setex(key, seconds, value);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public byte[] get(byte[] key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.get(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return get(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long del(byte[] key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.del(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return del(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long del(byte[]... keys) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.del(keys);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return del(keys);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long expire(byte[] key, int seconds) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.expire(key, seconds);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return expire(key, seconds);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Long expireAt(byte[] key, long seconds) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.expireAt(key, seconds);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return expireAt(key, seconds);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long ttl(byte[] key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.ttl(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return ttl(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public boolean exists(byte[] key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.exists(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return exists(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long incr(byte[] key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.incr(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return incr(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long incrBy(byte[] key, long increment) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.incrBy(key, increment);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return incrBy(key, increment);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long decr(byte[] key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.decr(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return decr(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long decrBy(byte[] key, long decrement) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.decrBy(key, decrement);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return decrBy(key, decrement);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long lpush(byte[] key, byte[]... strings) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.lpush(key, strings);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return lpush(key, strings);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long rpush(byte[] key, byte[]... strings) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.rpush(key, strings);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return rpush(key, strings);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long llen(byte[] key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.llen(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return llen(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public byte[] lpop(byte[] key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.lpop(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return lpop(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public byte[] rpop(byte[] key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.rpop(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return rpop(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public List<byte[]> lrange(byte[] key, long start, long end) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.lrange(key, start, end);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return lrange(key, start, end);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public List<byte[]> lrangeAll(byte[] key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.lrange(key, 0, -1);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return lrangeAll(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long hset(byte[] key, byte[] field, byte[] value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hset(key, field, value);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hset(key, field, value);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hsetnx(key, field, value);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hsetnx(key, field, value);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Long setnx(byte[] key, byte[] value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.setnx(key, value);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return jedis.setnx(key, value);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Long setnx(String key, String value) {
        return setnx(key.getBytes(Charset.defaultCharset()), value.getBytes(Charset.defaultCharset()));
    }

    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hmset(key, hash);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hmset(key, hash);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public byte[] hget(byte[] key, byte[] field) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hget(key, field);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hget(key, field);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public List<byte[]> hmget(final byte[] key, final byte[]... fields) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hmget(key, fields);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hmget(key, fields);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Boolean hexists(byte[] key, byte[] field) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hexists(key, field);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hexists(key, field);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long hdel(byte[] key, byte[]... fields) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hdel(key, fields);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hdel(key, fields);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long hlen(byte[] key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hlen(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hlen(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Map<byte[], byte[]> hgetAll(byte[] key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hgetAll(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return hgetAll(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long sadd(byte[] key, byte[]... members) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.sadd(key, members);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return sadd(key, members);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Set<byte[]> smembers(byte[] key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.smembers(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return smembers(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long srem(byte[] key, byte[]... members) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.srem(key, members);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return srem(key, members);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long scard(byte[] key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.scard(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return scard(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Set<byte[]> sunion(byte[]... keys) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.sunion(keys);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return sunion(keys);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Set<byte[]> sdiff(byte[]... keys) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.sdiff(keys);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return sdiff(keys);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    public Long sdiffstore(byte[] dstkey, byte[]... keys) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.sdiffstore(dstkey, keys);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return sdiffstore(dstkey, keys);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Long lrem(String key, long count, String value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.lrem(key, count, value);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return lrem(key, count, value);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Long lrem(byte[] key, long count, byte[] value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.lrem(key, count, value);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return lrem(key, count, value);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Long zadd(String key, double score, String member) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zadd(key, score, member);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zadd(key, score, member);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zadd(key, scoreMembers);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zadd(key, scoreMembers);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Long zcount(final String key, final double min, final double max) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zcount(key, min, max);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zcount(key, min, max);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Long zcount(final String key, final String min, final String max) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zcount(key, min, max);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zcount(key, min, max);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Double zincrby(final String key, final double score, final String member) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zincrby(key, score, member);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zincrby(key, score, member);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Set<String> zrange(final String key, final long start, final long end) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zrange(key, start, end);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zrange(key, start, end);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Set<String> zrangeByScore(final String key, final double min, final double max) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zrangeByScore(key, min, max);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zrangeByScore(key, min, max);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Set<String> zrangeByScore(final String key, final String min, final String max) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zrangeByScore(key, min, max);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zrangeByScore(key, min, max);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Set<String> zrangeByScore(final String key, final double min, final double max, final int offset, int count) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zrangeByScore(key, min, max, offset, count);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zrangeByScore(key, min, max, offset, count);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Set<String> zrevrange(final String key, final long start, final long end) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zrevrange(key, start, end);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zrevrange(key, start, end);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final double max, final double min) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zrevrangeByScore(key, max, min);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zrevrangeByScore(key, max, min);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final String max, final String min) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zrevrangeByScore(key, max, min);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zrevrangeByScore(key, max, min);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final double max, final double min, final int offset, int count) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zrevrangeByScore(key, max, min, offset, count);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zrevrangeByScore(key, max, min, offset, count);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final String max, final String min, final int offset, int count) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zrevrangeByScore(key, max, min, offset, count);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zrevrangeByScore(key, max, min, offset, count);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Long zrevrank(final String key, final String member) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zrevrank(key, member);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zrevrank(key, member);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Long zrem(final String key, final String... member) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zrem(key, member);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zrem(key, member);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Long zremrangeByRank(final String key, final long start, final long end) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zremrangeByRank(key, start, end);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zremrangeByRank(key, start, end);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Long zremrangeByScore(final String key, final double start, final double end) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zremrangeByScore(key, start, end);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zremrangeByScore(key, start, end);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Long zremrangeByScore(final String key, final String start, final String end) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zremrangeByScore(key, start, end);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zremrangeByScore(key, start, end);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Boolean sismember(String key, String object) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.sismember(key, object);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return sismember(key, object);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    /**
     * 设置key的过期时间。如果key已过期，将会被自动删除。
     *
     * @param key          cache中存储数据的key
     * @param milliseconds 过期的毫秒数
     * @return 被设置key的数量
     * @
     */
    @Override
    public Long pexpire(String key, long milliseconds) {
        if (milliseconds < 1) {
            throw new CacheClientException("非法参数!");
        }
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.pexpire(key, milliseconds);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return pexpire(key, milliseconds);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Boolean ltrim(String listKey, long start, long stop) {
        if (start < 1) {
            throw new CacheClientException("非法参数!");
        }
        Jedis jedis = null;
        try {
            jedis = getJedis();
            String rtu = jedis.ltrim(listKey, start, stop);
            return "OK".equals(rtu);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return ltrim(listKey, start, stop);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
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

    /**
     * 返回key的有序集合中的分数在min和max之间的所有元素（包括分数等于max或者min的元素）。元素被认为是从低分到高分排序的。
     * 具有相同分数的元素按字典序排列, 指定返回结果的数量及区间。 返回元素和其分数，而不只是元素
     *
     * @param key
     * @param max
     * @param min
     * @param offset
     * @param count
     * @return
     */
    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zrevrangeByScoreWithScores(key, max, min, offset, count);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    /**
     * 返回key的有序集合中的分数在min和max之间的所有元素（包括分数等于max或者min的元素）。元素被认为是从低分到高分排序的。
     * 具有相同分数的元素按字典序排列, 指定返回结果的数量及区间。 返回元素和其分数，而不只是元素
     *
     * @param key
     * @param max
     * @param min
     * @param offset
     * @param count
     * @return
     */
    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zrevrangeByScoreWithScores(key, max, min, offset, count);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zrevrangeByScoreWithScores(key, max, min);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zrevrangeByScoreWithScores(key, max, min);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }

    /**
     * 返回key的有序集元素个数。
     *
     * @param key
     * @return
     */
    @Override
    public Long zcard(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zcard(key);
        } catch (JedisConnectionException jedisException) {
            createPool();
            if (canConnection()) {
                return zcard(key);
            } else {
                log.error(jedisException.getMessage(),
                        jedisException);
                throw new CacheClientException(jedisException);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CacheClientException(e);
        } finally {
            if (jedis != null)
                returnResource(jedis);
        }
    }
}
