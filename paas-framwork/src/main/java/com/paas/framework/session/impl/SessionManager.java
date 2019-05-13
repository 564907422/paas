package com.paas.framework.session.impl;

import java.util.UUID;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.paas.framework.session.RequestEventObserver;
import com.paas.framework.session.SessionException;
import com.paas.framework.session.cache.ICacheService;
import com.paas.framework.session.conf.SessionConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;


@Service
@Scope("singleton")
public class SessionManager {
	private static final Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);
//	private static final String SESSION_ID_PREFIX = "R_JSID_";
//	private static final String SESSION_ID_COOKIE = "WOEGO_JSESSIONID";
	@Autowired
	private ICacheService<CacheHttpSession> cacheClient ;
	@Autowired
	private SessionConf sessionConf;
	
//	private int expirationUpdateInterval = 300;
//	private int maxInactiveInterval = 1800;
//	private String domain = "";


	public CacheHttpSession createSession(
			SessionHttpServletRequestWrapper request,
			HttpServletResponse response,
			RequestEventSubject requestEventSubject, boolean create) {
		String sessionId = getRequestedSessionId(request);

		CacheHttpSession session = null;
		if ((StringUtils.isEmpty(sessionId)) && (!create))
			return null;
		if (StringUtils.hasText(sessionId)) {
			session = loadSession(sessionId);
		}
		if ((session == null) && (create)) {
			session = createEmptySession(request, response);
		}
		if (session != null)
			attachEvent(session, request, response, requestEventSubject);
		return session;
	}

	private String getRequestedSessionId(HttpServletRequestWrapper request) {
		Cookie[] cookies = request.getCookies();
		if ((cookies == null) || (cookies.length == 0))
			return null;
		for (Cookie cookie : cookies) {
			if (sessionConf.getSessionIdCookie().equals(cookie.getName()))
				return cookie.getValue();
		}
		return null;
	}

	private void saveSession(CacheHttpSession session) {
		try {
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("--------------CacheHttpSession saveSession [ID={},isNew={},isDiry={},isExpired={}]"
						,session.id,session.isNew,session.isDirty,session.expired);
			if (session.expired)
				this.removeSessionFromCache(generatorSessionKey(session.id));
			else
				this.saveSessionToCache(generatorSessionKey(session.id),
						session, session.maxInactiveInterval);
		} catch (Exception e) {
			LOGGER.error("",e);
			throw new SessionException(e);
		}
	}

	private CacheHttpSession createEmptySession(
			SessionHttpServletRequestWrapper request,
			HttpServletResponse response) {
		CacheHttpSession session = new CacheHttpSession();
		session.id = createSessionId();
		session.creationTime = System.currentTimeMillis();
		session.maxInactiveInterval = sessionConf.getMaxInactiveInterval();
		session.isNew = true;
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("--------CacheHttpSession Create [ID={}]",session.id);
		saveCookie(session, request, response);
		return session;
	}

	private String createSessionId() {
		return UUID.randomUUID().toString().replace("-", "").toUpperCase();
	}

	private void attachEvent(final CacheHttpSession session,
			final HttpServletRequestWrapper request,
			final HttpServletResponse response,
			RequestEventSubject requestEventSubject) {
		session.setListener(new SessionListenerAdaptor() {
			public void onInvalidated(CacheHttpSession session) {
				SessionManager.this.saveCookie(session, request, response);
			}
		});
		requestEventSubject.attach(new RequestEventObserver() {
			public void completed(HttpServletRequest servletRequest,
					HttpServletResponse response) {
				int updateInterval = (int) ((System.currentTimeMillis() - session.lastAccessedTime) / 1000L);
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("-----------CacheHttpSession Request completed [ID={},lastAccessedTime={},updateInterval={}]"
									,session.id,session.lastAccessedTime,updateInterval);
				}
				if ((!session.isNew)
						&& (!session.isDirty)
						&& (updateInterval < sessionConf.getExpirationUpdateInterval()))
					return;
				if ((session.isNew) && (session.expired))
					return;
				session.lastAccessedTime = System.currentTimeMillis();
				SessionManager.this.saveSession(session);
			}
		});
	}

	private void addCookie(CacheHttpSession session,
			HttpServletRequestWrapper request, HttpServletResponse response) {
		Cookie cookie = new Cookie(sessionConf.getSessionIdCookie(), null);
		if (!StringUtils.isEmpty(sessionConf.getDomain()))
			cookie.setDomain(sessionConf.getDomain());
		cookie.setPath(StringUtils.isEmpty(request.getContextPath())?"/":request.getContextPath());
		if (session.expired)
			cookie.setMaxAge(0);
		else if (session.isNew) {
			cookie.setValue(session.getId());
		}
		response.addCookie(cookie);
	}

	private void saveCookie(CacheHttpSession session,
			HttpServletRequestWrapper request, HttpServletResponse response) {
		if ((!session.isNew) && (!session.expired))
			return;

		Cookie[] cookies = request.getCookies();
		if ((cookies == null) || (cookies.length == 0)) {
			addCookie(session, request, response);
		} else {
			for (Cookie cookie : cookies) {
				if (sessionConf.getSessionIdCookie().equals(cookie.getName())) {
					if (StringUtils.hasText(sessionConf.getDomain()))
						cookie.setDomain(sessionConf.getDomain());
					cookie.setPath(StringUtils.isEmpty(request.getContextPath())?"/":request.getContextPath());
					cookie.setMaxAge(0);
				}
			}
			addCookie(session, request, response);
		}
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("---------------CacheHttpSession saveCookie [ID={}]",session.id);
	}

	private CacheHttpSession loadSession(String sessionId) {
		CacheHttpSession session;
		try {
			HttpSession data = this
					.getSessionFromCache(generatorSessionKey(sessionId));

			if (data == null) {
				LOGGER.debug("-------------Session {} not found in Redis",sessionId);
				session = null;
			} else {
				session = (CacheHttpSession) data;
			}
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("-------------CacheHttpSession Load [ID={},exist={}]"
						,sessionId, (session != null));
			if (session != null) {
				session.isNew = false;
				session.isDirty = false;
			}
			return session;
		} catch (Exception e) {
			LOGGER.warn("------------exception loadSession [Id={}]",sessionId, e);
		}
		return null;
	}

	private String generatorSessionKey(String sessionId) {
		return sessionConf.getSessionIdPrefix().concat(sessionId);
		// return "R_JSID_".concat(sessionId);
	}

	public SessionManager() {
	}

	public void init() {
	}


	public CacheHttpSession getSessionFromCache(String id) {
//		Object obj = null;
//		if ("true".equalsIgnoreCase(twemproxy)) {
//			obj = cacheClient.getSession(id);
//		} else {
//			obj = cacheClient.getSession(dbIndex, id);
//		}
//		if (obj != null && obj instanceof HttpSession) {
//			redisDowntime = false;
//			return (HttpSession) obj;
//		} else if(obj != null && obj instanceof RedisDowntime) {
//			redisDowntime = true;
//			log.warn("--------------redis宕机-------------");
//			return localSessions.get(id);
//		}else {
//			redisDowntime = false;
//			return null;
//		}
		return cacheClient.get(id);
	}

	public void saveSessionToCache(String id, CacheHttpSession session, int liveTime) {
		cacheClient.setEx(id, liveTime, session);
//		if(redisDowntime){
//			localSessions.put(id, session);
//		}else{
//			if ("true".equalsIgnoreCase(twemproxy)) {
//				cacheClient.addItem(id, session, liveTime);
//			} else {
//				cacheClient.addItem(dbIndex, id, session, liveTime);
//			}
//			localSessions.clear();
//		}

	}

	public void removeSessionFromCache(String id) {
		cacheClient.del(id);
//		if(redisDowntime){
//			localSessions.remove(id);
//		}else{
//			if ("true".equalsIgnoreCase(twemproxy)) {
//				cacheClient.delItem(id);
//			} else {
//				cacheClient.delItem(dbIndex, id);
//			}
//		}
		
	}

	/*
	 * private static SessionManager rs1=null; public static SessionManager
	 * getRs(){ if (rs1==null){ rs1=new SessionManager(); rs1.serializer=new
	 * SerializeUtil(); } return rs1; }
	 */
	
	public boolean isEnableSession(){
		if(StringUtils.isEmpty(sessionConf.getSessionEnable())||"true".equals(sessionConf.getSessionEnable()))
			return true;
		return false;
	}

}
