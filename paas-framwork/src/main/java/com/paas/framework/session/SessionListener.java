package com.paas.framework.session;

import com.paas.framework.session.impl.CacheHttpSession;

public abstract interface SessionListener {
	  public abstract void onAttributeChanged(CacheHttpSession paramRedisHttpSession);

	  public abstract void onInvalidated(CacheHttpSession paramRedisHttpSession);
}
