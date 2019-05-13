package com.paas.framework.depend;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.stereotype.Service;

/**
 * spring获取工具类
 * 
``] */
//@SuppressWarnings("unchecked")
@Service
public class SpringBeanFactory implements BeanFactoryAware {

	private static BeanFactory beanFactory;

	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		SpringBeanFactory.beanFactory = beanFactory;
	}

	public static Object getBean(String name) {
		return beanFactory.getBean(name);
	}

	public static <T> T getBean(String name, Class<T> clazz) {
		return (T) beanFactory.getBean(name);
	}

	public static boolean containsBean(String name) {
		return beanFactory.containsBean(name);
	}

}
