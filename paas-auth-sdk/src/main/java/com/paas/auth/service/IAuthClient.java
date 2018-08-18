package com.paas.auth.service;

import com.paas.auth.vo.AuthDescriptor;
import com.paas.auth.vo.AuthResult;

public interface IAuthClient {
	/**
	 * 校验用户名、密码
	 * @param ad
	 * @return true 合法  false 不合法
	 * @throws Exception 
	 */
	public AuthResult auth(AuthDescriptor ad) throws Exception;
}
