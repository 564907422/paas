package com.paas.framework.except.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.alibaba.fastjson.serializer.SerializeFilter;
import com.alibaba.fastjson.serializer.SerializerFeature;

public class JsonWriter {
	private static final SerializeConfig DEFAULT_CONFIG = SerializeConfig.getGlobalInstance();
	private static final SerializeFilter[] DEFAULT_FILTERS = new SerializeFilter[0];
	private static final SerializerFeature[] DEFAULT_FEATURES = new SerializerFeature[0];

	private SerializeConfig config;
	private SerializeFilter[] filters;
	private SerializerFeature[] features;

	public JsonWriter(SerializeConfig config, SerializeFilter[] filters, SerializerFeature[] features) {
		this.config = config == null ? DEFAULT_CONFIG : config;
		this.filters = filters == null ? DEFAULT_FILTERS : filters;
		this.features = features == null ? DEFAULT_FEATURES : features;
	}

	public JsonWriter() {
		this(DEFAULT_CONFIG, DEFAULT_FILTERS, DEFAULT_FEATURES);
	}

	public <T> String toJsonString(T object) {
		return JSON.toJSONString(object, config, filters, features);
	}

	public <T> String toJsonpString(T object, String jsonpFunc) {
		StringBuilder buffer = new StringBuilder();
		buffer.append(jsonpFunc).append('(');
		buffer.append(toJsonString(object));
		buffer.append(");");
		return buffer.toString();
	}
}
