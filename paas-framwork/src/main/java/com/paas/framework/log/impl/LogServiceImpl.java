package com.paas.framework.log.impl;

import com.paas.framework.log.ILogService;
import org.aspectj.lang.JoinPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LogServiceImpl implements ILogService {
	private static final Logger LOGGER = LoggerFactory.getLogger(LogServiceImpl.class);
	
    @Override
    public void log(JoinPoint point) {
    	LOGGER.info("-=> {}.{}({}) ", point.getTarget().getClass().getName(), point.getSignature().getName(), point.getArgs());
    }

    //有参并有返回值的方法
    public void logArgAndReturn(JoinPoint point, Object returnObj) {
        if(LOGGER.isDebugEnabled()){
        	LOGGER.debug("-=> {}.{}({}); return {}", point.getTarget().getClass().getName(), point.getSignature().getName(), point.getArgs(), returnObj);
        }else{
        	LOGGER.info("-=> {}.{}() return.", point.getTarget().getClass().getName(), point.getSignature().getName());
        }
    }
    
}
