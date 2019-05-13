package com.paas.framework.session.cache;

public interface ICacheService<T> {
	T get(String key);

	void setEx(String key, long expire, T value);
	
	void del(String key);

}
