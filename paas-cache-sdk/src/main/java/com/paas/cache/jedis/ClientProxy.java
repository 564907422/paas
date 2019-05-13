package com.paas.cache.jedis;

import com.paas.cache.ICacheClient;
import com.paas.commons.env.EnvBean;
import redis.clients.jedis.Tuple;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created on 2016/9/28.
 */
public class ClientProxy implements ICacheClient {

    private static String env = EnvBean.getEnv();
    private static boolean isProd = EnvBean.ENV_PROD.equals(env);

    private ICacheClient client;
    private String bizCode;
    private String bizCodeEnv;
    private boolean needSuffix = true;

    public ClientProxy(ICacheClient client, String bizCode) {
        this.client = client;
        this.bizCode = bizCode;
        this.bizCodeEnv = bizCode + env;
    }

    public ClientProxy() {
    }

    public JedisConfig getConfig() {
        if (client instanceof JedisClient) {
            return ((JedisClient) client).getConfig();
        }
        return null;
    }

    protected void setClient(ICacheClient client) {
        this.client = client;
    }

    public ICacheClient getClient() {
        return this.client;
    }

    protected void setBizCode(String bizCode) {
        this.bizCode = bizCode;
        this.bizCodeEnv = bizCode + env;
    }

    protected void setNeedSuffix(boolean needSuffix) {
        this.needSuffix = needSuffix;
    }

    // pretreat key
    private String pretKey(String key) {
        if (!needSuffix) {
            return key;
        }
        if (isProd) {
            return key + bizCode;
        }
        return key + bizCodeEnv;
    }

    private String[] pretKeys(String... keys) {
        if (!needSuffix) {
            return keys;
        }
        if (isProd) {
            return Arrays.stream(keys).map(str -> str + bizCode).toArray(String[]::new);
        }
        return Arrays.stream(keys).map(str -> str + bizCodeEnv).toArray(String[]::new);
    }

    private byte[] pretKey(byte[] key) {
        if (!needSuffix) {
            return key;
        }
        if (isProd) {
            return contact(key, bizCode.getBytes());
        }
        return contact(key, bizCodeEnv.getBytes());
    }

    private byte[][] pretKeys(byte[]... keys) {
        if (!needSuffix) {
            return keys;
        }
        if (isProd) {
            return foreach(keys, bizCode.getBytes());
        }
        return foreach(keys, bizCodeEnv.getBytes());
    }

    private byte[][] foreach(byte[][] keys, byte[] add) {
        byte[][] newKeys = new byte[keys.length][];
        int index = 0;
        for (byte[] key : keys) {
            newKeys[index++] = contact(key, add);
        }
        return newKeys;
    }

    private byte[] contact(byte[] src1, byte[] src2) {
        byte[] target = new byte[src1.length + src2.length];
        System.arraycopy(src1, 0, target, 0, src1.length);
        System.arraycopy(src2, 0, target, src1.length, src2.length);
        return target;
    }

//    @Override
//    public String set(String key, String value) {
//        return client.set(pretKey(key), value);
//    }
//
//    @Override
//    public String getSet(String key, String value) {
//        return client.getSet(pretKey(key), value);
//    }

    @Override
    public String setex(String key, int seconds, String value) {
        return client.setex(pretKey(key), seconds, value);
    }

    @Override
    public String get(String key) {
        return client.get(pretKey(key));
    }

    @Override
    public Long del(String key) {
        return client.del(pretKey(key));
    }

    @Override
    public Long del(String... keys) {
        return client.del(pretKeys(keys));
    }

    @Override
    public Long expire(String key, int seconds) {
        return client.expire(pretKey(key), seconds);
    }

    @Override
    public Long expireAt(String key, long timestamp) {
        return client.expireAt(pretKey(key), timestamp);
    }

    @Override
    public Long ttl(String key) {
        return client.ttl(pretKey(key));
    }

    @Override
    public boolean exists(String key) {
        return client.exists(pretKey(key));
    }

    @Override
    public Long incr(String key) {
        return client.incr(pretKey(key));
    }

    @Override
    public Long incrBy(String key, long increment) {
        return client.incrBy(pretKey(key), increment);
    }

    @Override
    public Long decr(String key) {
        return client.decr(pretKey(key));
    }

    @Override
    public Long decrBy(String key, long decrement) {
        return client.decrBy(pretKey(key), decrement);
    }

    @Override
    public Long lpush(String key, String... strings) {
        return client.lpush(pretKey(key), strings);
    }

    @Override
    public Long rpush(String key, String... strings) {
        return client.rpush(pretKey(key), strings);
    }

    @Override
    public Long lrem(String key, long count, String value) {
        return client.lrem(pretKey(key), count, value);
    }

    @Override
    public Long llen(String key) {
        return client.llen(pretKey(key));
    }

    @Override
    public String lpop(String key) {
        return client.lpop(pretKey(key));
    }

    @Override
    public String rpop(String key) {
        return client.rpop(pretKey(key));
    }

    @Override
    public List<String> lrange(String key, long start, long end) {
        return client.lrange(pretKey(key), start, end);
    }

    @Override
    public List<String> lrangeAll(String key) {
        return client.lrangeAll(pretKey(key));
    }

    @Override
    public Long hset(String key, String field, String value) {
        return client.hset(pretKey(key), field, value);
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        return client.hsetnx(pretKey(key), field, value);
    }

    @Override
    public String hmset(String key, Map<String, String> hash) {
        return client.hmset(pretKey(key), hash);
    }

    @Override
    public String hget(String key, String field) {
        return client.hget(pretKey(key), field);
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        return client.hmget(pretKey(key), fields);
    }

    @Override
    public Boolean hexists(String key, String field) {
        return client.hexists(pretKey(key), field);
    }

    @Override
    public Long hdel(String key, String... fields) {
        return client.hdel(pretKey(key), fields);
    }

    @Override
    public Long hlen(String key) {
        return client.hlen(pretKey(key));
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return client.hgetAll(pretKey(key));
    }

    @Override
    public Long sadd(String key, String... members) {
        return client.sadd(pretKey(key), members);
    }

    @Override
    public Set<String> smembers(String key) {
        return client.smembers(pretKey(key));
    }

    @Override
    public Long srem(String key, String... members) {
        return client.srem(pretKey(key), members);
    }

    @Override
    public Long scard(String key) {
        return client.scard(pretKey(key));
    }

    @Override
    public Set<String> sunion(String... keys) {
        return client.sunion(pretKeys(keys));
    }

    @Override
    public Set<String> sdiff(String... keys) {
        return client.sdiff(pretKeys(keys));
    }

    @Override
    public Long sdiffstore(String dstkey, String... keys) {
        return client.sdiffstore(pretKey(dstkey), pretKeys(keys));
    }


//    @Override
//    public String set(byte[] key, byte[] value) {
//        return client.set(pretKey(key), value);
//    }

    @Override
    public String setex(byte[] key, int seconds, byte[] value) {
        return client.setex(pretKey(key), seconds, value);
    }

    @Override
    public byte[] get(byte[] key) {
        return client.get(pretKey(key));
    }

    @Override
    public Long del(byte[] key) {
        return client.del(pretKey(key));
    }

    @Override
    public Long del(byte[]... keys) {
        return client.del(pretKeys(keys));
    }

    @Override
    public Long expire(byte[] key, int seconds) {
        return client.expire(pretKey(key), seconds);
    }

    @Override
    public Long expireAt(byte[] key, long timestamp) {
        return client.expireAt(pretKey(key), timestamp);
    }

    @Override
    public Long ttl(byte[] key) {
        return client.ttl(pretKey(key));
    }

    @Override
    public boolean exists(byte[] key) {
        return client.exists(pretKey(key));
    }

    @Override
    public Long incr(byte[] key) {
        return client.incr(pretKey(key));
    }

    @Override
    public Long incrBy(byte[] key, long increment) {
        return client.incrBy(pretKey(key), increment);
    }

    @Override
    public Long decr(byte[] key) {
        return client.decr(pretKey(key));
    }

    @Override
    public Long decrBy(byte[] key, long decrement) {
        return client.decrBy(pretKey(key), decrement);
    }

    @Override
    public Long lpush(byte[] key, byte[]... strings) {
        return client.lpush(pretKey(key), strings);
    }

    @Override
    public Long rpush(byte[] key, byte[]... strings) {
        return client.rpush(pretKey(key), strings);
    }

    @Override
    public Long llen(byte[] key) {
        return client.llen(pretKey(key));
    }

    @Override
    public Long lrem(byte[] key, long count, byte[] value) {
        return client.lrem(pretKey(key), count, value);
    }

    @Override
    public byte[] lpop(byte[] key) {
        return client.lpop(pretKey(key));
    }

    @Override
    public byte[] rpop(byte[] key) {
        return client.rpop(pretKey(key));
    }

    @Override
    public List<byte[]> lrange(byte[] key, long start, long end) {
        return client.lrange(pretKey(key), start, end);
    }

    @Override
    public List<byte[]> lrangeAll(byte[] key) {
        return client.lrangeAll(pretKey(key));
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        return client.hset(pretKey(key), field, value);
    }

    @Override
    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        return client.hsetnx(pretKey(key), field, value);
    }

    @Override
    public Long setnx(byte[] key, byte[] value) {
        return client.setnx(pretKey(key), value);
    }

    @Override
    public Long setnx(String key, String value) {
        return client.setnx(pretKey(key), value);
    }

    @Override
    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        return client.hmset(pretKey(key), hash);
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        return client.hget(pretKey(key), field);
    }

    @Override
    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        return client.hmget(pretKey(key), fields);
    }

    @Override
    public Boolean hexists(byte[] key, byte[] field) {
        return client.hexists(pretKey(key), field);
    }

    @Override
    public Long hdel(byte[] key, byte[]... fields) {
        return client.hdel(pretKey(key), fields);
    }

    @Override
    public Long hlen(byte[] key) {
        return client.hlen(pretKey(key));
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return client.hgetAll(pretKey(key));
    }

    @Override
    public Long sadd(byte[] key, byte[]... members) {
        return client.sadd(pretKey(key), members);
    }

    @Override
    public Set<byte[]> smembers(byte[] key) {
        return client.smembers(pretKey(key));
    }

    @Override
    public Long srem(byte[] key, byte[]... members) {
        return client.srem(pretKey(key), members);
    }

    @Override
    public Long scard(byte[] key) {
        return client.scard(pretKey(key));
    }

    @Override
    public Set<byte[]> sunion(byte[]... keys) {
        return client.sunion(pretKeys(keys));
    }

    @Override
    public Set<byte[]> sdiff(byte[]... keys) {
        return client.sdiff(pretKeys(keys));
    }

    @Override
    public Long sdiffstore(byte[] dstkey, byte[]... keys) {
        return client.sdiffstore(pretKey(dstkey), pretKeys(keys));
    }

    @Override
    public Long hincrBy(String key, String field, long value) {
        return client.hincrBy(pretKey(key), field, value);
    }

    @Override
    public Double incrByFloat(String key, double value) {
        return client.incrByFloat(pretKey(key), value);
    }

    @Override
    public Double hincrByFloat(String key, String field, double value) {
        return client.hincrByFloat(pretKey(key), field, value);
    }

    @Override
    public Long zadd(String key, double score, String member) {
        return client.zadd(pretKey(key), score, member);
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) {
        return client.zadd(pretKey(key), scoreMembers);
    }

    @Override
    public Long zcount(String key, double min, double max) {
        return client.zcount(pretKey(key), min, max);
    }

    @Override
    public Long zcount(String key, String min, String max) {
        return client.zcount(pretKey(key), min, max);
    }

    @Override
    public Double zincrby(String key, double score, String member) {
        return client.zincrby(pretKey(key), score, member);
    }

    @Override
    public Set<String> zrange(String key, long start, long end) {
        return client.zrange(pretKey(key), start, end);
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) {
        return client.zrangeByScore(pretKey(key), min, max);
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) {
        return client.zrangeByScore(pretKey(key), min, max);
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        return client.zrangeByScore(pretKey(key), min, max, offset, count);
    }

    @Override
    public Set<String> zrevrange(String key, long start, long end) {
        return client.zrevrange(pretKey(key), start, end);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min) {
        return client.zrevrangeByScore(pretKey(key), max, min);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min) {
        return client.zrevrangeByScore(pretKey(key), max, min);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return client.zrevrangeByScore(pretKey(key), max, min, offset, count);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        return client.zrevrangeByScore(pretKey(key), max, min, offset, count);
    }

    @Override
    public Long zrevrank(String key, String member) {
        return client.zrevrank(pretKey(key), member);
    }

    @Override
    public Long zrem(String key, String... members) {
        return client.zrem(pretKey(key), members);
    }

    @Override
    public Long zremrangeByRank(String key, long start, long end) {
        return client.zremrangeByRank(pretKey(key), start, end);
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) {
        return client.zremrangeByScore(pretKey(key), start, end);
    }

    @Override
    public Long zremrangeByScore(String key, String start, String end) {
        return client.zremrangeByScore(pretKey(key), start, end);
    }

    @Override
    public String setObjectEx(byte[] key, int seconds, Object serializable) {
        return client.setObjectEx(pretKey(key), seconds, serializable);
    }

    @Override
    public Object getObject(byte[] key) {
        return client.getObject(pretKey(key));
    }

    @Override
    public Boolean sismember(String key, String object) {
        return client.sismember(pretKey(key), object);
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
        return client.pexpire(pretKey(key), milliseconds);
    }

    /**
     * 修剪(trim)一个已存在的 list，这样 list 就会只包含指定范围的指定元素。
     * start 和 stop 都是由0开始计数的， 这里的 0 是列表里的第一个元素（表头），1 是第二个元素，以此类推。
     *
     * @param listKey cache中存储数据的list
     * @param start   启示位置，stop 结束位置
     * @return 被设置key的数量
     * @
     */
    @Override
    public Boolean ltrim(String listKey, long start, long stop) {
        return client.ltrim(pretKey(listKey), start, stop);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return client.zrevrangeByScoreWithScores(pretKey(key), max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        return client.zrevrangeByScoreWithScores(pretKey(key), max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        return client.zrevrangeByScoreWithScores(pretKey(key), max, min);
    }

    @Override
    public Long zcard(String key) {
        return client.zcard(pretKey(key));
    }

}
