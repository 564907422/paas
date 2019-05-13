package com.paas.framework.db;

import com.paas.framework.conf.DataSourceConf;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import java.lang.reflect.Method;

/**
 * 数据源切换
 */
public class DataSourceAspect {
	
	@Autowired
	private DataSourceConf conf;

    private static Logger logger = LoggerFactory.getLogger(DataSourceAspect.class);

    public void before(JoinPoint joinPoint) {

        logger.debug("log begin method: " + joinPoint.getTarget().getClass().getName() + "." + joinPoint.getSignature().getName());

        try {
            Object target = joinPoint.getTarget();
            String method = joinPoint.getSignature().getName();
            Class<?>[] parameterTypes = ((MethodSignature) joinPoint.getSignature()).getMethod().getParameterTypes();

            String dataSourceValue;
            dataSourceValue = buildDataSource(target, method, parameterTypes);
            dataSourceValue = buildTransactional(dataSourceValue, target, method, parameterTypes);

            if (dataSourceValue == null) {
                String defaultDataSource = conf.getDefaultDataSourceName();
                defaultDataSource = defaultDataSource == null ? DynamicDataSource.getDefaultDataSourceName() : defaultDataSource;
                logger.debug("切换默认数据源：[ " + defaultDataSource + " ]");
                DynamicDataSource.setDataSourceKey(defaultDataSource);
            } else {
                logger.debug("切换数据源到：[ " + dataSourceValue + " ]");
                DynamicDataSource.setDataSourceKey(dataSourceValue);

            }

        } catch (Exception e) {
            logger.error("切换数据源异常", e);
        }

    }

    private String buildDataSource(Object target, String method, Class<?>[] parameterTypes) throws NoSuchMethodException {
        String dataSourceValue = null;
        Class<?>[] classes = target.getClass().getInterfaces();
        Method method1 = classes[0].getMethod(method, parameterTypes);
        if (method1 != null) {
            if (method1.isAnnotationPresent(DataSource.class)) {
                dataSourceValue = method1.getAnnotation(DataSource.class).value();
            }
        }
        return dataSourceValue;
    }

    /**
     * 多数据源备注：Transactional readonly=true，用读库 readonly=false，用写库，没有Transactional，用default的设置
     * @param dataSourceValue
     * @param target
     * @param method
     * @param parameterTypes
     * @return
     * @throws NoSuchMethodException
     */
    private String buildTransactional(String dataSourceValue, Object target, String method, Class<?>[] parameterTypes) throws NoSuchMethodException {
        Class<?> clazz = target.getClass();
        Method m = clazz.getMethod(method, parameterTypes);
        // 添加对@Transactional的支持，如果为readonly=true，则读slave库，反之，读master库
        if (m.isAnnotationPresent(Transactional.class)) {
            Transactional transactional = m.getAnnotation(Transactional.class);
            if (transactional.readOnly()) {
                dataSourceValue = DynamicDataSource.getSlaveDataSource();
            } else {
                dataSourceValue = DynamicDataSource.getMasterDataSourceName();
            }
        }
        return dataSourceValue;
    }

    public Object doAround(ProceedingJoinPoint pjp) throws Throwable {
        long time = System.currentTimeMillis();
        Object retVal = pjp.proceed();
        time = System.currentTimeMillis() - time;
        logger.debug(pjp.getTarget().getClass().getName() + "." + pjp.getSignature().getName() + "process time: " + time + " ms");
        return retVal;
    }

    public void doAfter(JoinPoint jp) {
        logger.debug("log Ending method: " + jp.getTarget().getClass().getName() + "." + jp.getSignature().getName());
    }
}
