package com.paas.framework.conf;

import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;
import java.util.Set;

public class CustomPropertyPlaceholderConfigurer extends PropertyPlaceholderConfigurer {

	public void setCustomerPropFiles(Set<String> customerPropFiles) {
		Properties properties = new Properties();
		for (String properFile : customerPropFiles) {
			try {
				File file = ResourceUtils.getFile("classpath:" + properFile);
				FileReader reader = new FileReader(file);
				logger.info("Loading properites file from " + properFile);
				properties.load(reader);
			} catch (Exception e) {
				logger.error("Loading properites file from " + properFile,e);
			}
		}
		this.setProperties(properties); // 关键方法,调用的PropertyPlaceholderConfigurer中的方法,
		// 通过这个方法将自定义加载的properties文件加入spring中

	}
}
