package com.paas.cache.jedis;

import com.paas.cache.ICacheClient;
import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Created on 2016/10/8.
 */
public class ClientTimeProxy {

    protected static Logger log = LoggerFactory.getLogger(ClientTimeProxy.class);

    /**
     * 获取时间代理
     * @param client jedis 或 jedisCluster的实现
     * @param bizCode 业务编码
     * @param threshold 超时阀值
     * @return
     */
    public static ICacheClient getProxy(ICacheClient client, String bizCode, int threshold){
        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(ClientProxy.class);
        factory.setFilter(new MethodFilter() {
            @Override
            public boolean isHandled(Method m) {
                if(Modifier.isPublic(m.getModifiers()) && !Modifier.isFinal(m.getModifiers())){
                    boolean result = !m.getName().equals("toString") && !m.getName().equals("equals")
                            && !m.getName().equals("hashCode"); // hashCode toString equals
                    if(result){
                        log.debug(" ---> handled method: {}. ", m.getName());
                    }
                    return result;
                }
                return false;
            }
        });

        Class c = factory.createClass();
        MethodHandler mi = new MethodHandler() {
            public Object invoke(Object self, Method m, Method proceed,
                                 Object[] args) throws Throwable {
                long start = System.currentTimeMillis();
                Object result = proceed.invoke(self, args);
                long time = (System.currentTimeMillis()-start);
                // log.debug(" ---> jedis invoke [{}] cose: {}ms.", m.getName(), time);
                if(time > threshold){
                    log.warn(" ---> jedis invoke timeout: {}ms.", time);
                }
                return result;
            }
        };

        try {
            ClientProxy proxy = (ClientProxy) c.newInstance();
            ((Proxy)proxy).setHandler(mi);
            proxy.setClient(client);
            proxy.setBizCode(bizCode);
            log.debug(" ---> created jedis proxy: {}.", proxy);
            return proxy;
        } catch (Exception e) {
            log.error(" ---> create jedis time proxy error.", e);
        }
        return new ClientProxy(client, bizCode);
    }

}
