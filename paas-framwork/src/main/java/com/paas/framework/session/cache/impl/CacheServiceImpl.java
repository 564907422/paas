package com.paas.framework.session.cache.impl;


import com.paas.cache.CacheClientFactory;
import com.paas.cache.ICacheClient;
import com.paas.framework.session.cache.ICacheService;
import com.paas.framework.session.conf.SessionConf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class CacheServiceImpl<Serializable> implements ICacheService<Serializable> {

	@Autowired
	private SessionConf sessionConf;


	@Override
	public Serializable get(String key) {
		ICacheClient cc = CacheClientFactory.getClient(sessionConf.getSessionStoreId(), sessionConf.getSessionStoreAuth());
		Object res = cc.getObject(key.getBytes());
		if(res == null)
			return null;
		return (Serializable)res;

	}

	@Override
	public void setEx(String key, long expire, Serializable t) {
		ICacheClient cc = CacheClientFactory.getClient(sessionConf.getSessionStoreId(), sessionConf.getSessionStoreAuth());
		cc.setObjectEx(key.getBytes(),new Long(expire).intValue(),t);

	}

	@Override
	public void del(String key) {
		ICacheClient cc = CacheClientFactory.getClient(sessionConf.getSessionStoreId(), sessionConf.getSessionStoreAuth());
		cc.del(key.getBytes());
	}



}
