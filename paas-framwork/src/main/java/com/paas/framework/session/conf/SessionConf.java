package com.paas.framework.session.conf;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.baidu.disconf.client.common.annotations.DisconfFile;
import com.baidu.disconf.client.common.annotations.DisconfFileItem;
import com.baidu.disconf.client.common.annotations.DisconfUpdateService;
import com.baidu.disconf.client.common.update.IDisconfUpdate;

@Service
@Scope("singleton")
@DisconfFile(filename = "session.properties")
@DisconfUpdateService(classes = {SessionConf.class})
public class SessionConf implements IDisconfUpdate{
	//分布式session是否生效
	private String sessionEnable;
	//存放在redis中  session key 前缀
	private String sessionIdPrefix;
	//session  cookie名称
	private String sessionIdCookie;
	//session有效期的更新周期  毫秒
	private int expirationUpdateInterval = 100;
	//session的有效期  秒
	private int maxInactiveInterval = 1800;
	//session domain域
	private String domain = "";

	//session存储的标识  缓存ID
	private String sessionStoreId;
	//session存储的认证地址
	private String sessionStoreAuth;

	@DisconfFileItem(name = "session.id.prefix", associateField = "sessionIdPrefix")
	public String getSessionIdPrefix() {
		return sessionIdPrefix;
	}

	public void setSessionIdPrefix(String sessionIdPrefix) {
		this.sessionIdPrefix = sessionIdPrefix;
	}
	
	@DisconfFileItem(name = "session.id.cookie", associateField = "sessionIdCookie")
	public String getSessionIdCookie() {
		return sessionIdCookie;
	}

	public void setSessionIdCookie(String sessionIdCookie) {
		this.sessionIdCookie = sessionIdCookie;
	}
	
	@DisconfFileItem(name = "session.expiration.updateInterval", associateField = "expirationUpdateInterval")
	public int getExpirationUpdateInterval() {
		return expirationUpdateInterval;
	}

	public void setExpirationUpdateInterval(int expirationUpdateInterval) {
		this.expirationUpdateInterval = expirationUpdateInterval;
	}
	
	@DisconfFileItem(name = "session.maxInactiveInterval", associateField = "maxInactiveInterval")
	public int getMaxInactiveInterval() {
		return maxInactiveInterval;
	}

	public void setMaxInactiveInterval(int maxInactiveInterval) {
		this.maxInactiveInterval = maxInactiveInterval;
	}
	
	@DisconfFileItem(name = "session.domain", associateField = "domain")
	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	@DisconfFileItem(name = "session.enable", associateField = "sessionEnable")
	public String getSessionEnable() {
		return sessionEnable;
	}

	public void setSessionEnable(String sessionEnable) {
		this.sessionEnable = sessionEnable;
	}

	public String getSessionStoreId() {
		return sessionStoreId;
	}

	@DisconfFileItem(name = "session.store.id", associateField = "sessionStoreId")
	public void setSessionStoreId(String sessionStoreId) {
		this.sessionStoreId = sessionStoreId;
	}

	public String getSessionStoreAuth() {
		return sessionStoreAuth;
	}
	@DisconfFileItem(name = "session.store.auth", associateField = "sessionStoreAuth")
	public void setSessionStoreAuth(String sessionStoreAuth) {
		this.sessionStoreAuth = sessionStoreAuth;
	}

	@Override
	public void reload() throws Exception {
		// TODO Auto-generated method stub
		
	}

}
