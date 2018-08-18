package com.paas.auth.service.impl;


import com.alibaba.fastjson.JSON;
import com.paas.auth.service.IAuthClient;
import com.paas.auth.util.HttpUtil;
import com.paas.auth.vo.AuthDescriptor;
import com.paas.auth.vo.AuthResult;

public class AuthClientImpl implements IAuthClient {

	/**
	 * 服务号认证
	 */
	public AuthResult auth(AuthDescriptor ad) throws Exception  {
		String authAdress = ad.getAuthAdress();//用户认证地址，用来请求web端
		String serviceId = ad.getServiceId();
		String res = HttpUtil.get(authAdress+"?serviceId="+serviceId);
		return JSON.parseObject(res, AuthResult.class);
	}


	
	

}
