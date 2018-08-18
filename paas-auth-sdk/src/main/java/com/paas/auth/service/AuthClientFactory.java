package com.paas.auth.service;

import com.paas.auth.service.impl.AuthClientImpl;

public class AuthClientFactory {
	private static IAuthClient iAuthClient;
	private AuthClientFactory() {

	}
	public static IAuthClient getAuthClient() {
		if(iAuthClient==null)
			iAuthClient = new AuthClientImpl();
		return iAuthClient;
	}
	
	
}
