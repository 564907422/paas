package com.paas.auth.vo;

import java.io.Serializable;

public class AuthDescriptor implements Serializable{
	private static final long serialVersionUID = -431793174759343174L;
	private String serviceId  = null;
	private String authAdress =null; //用户认证地址，rest地址，我们通过这个地址去请求我们的web，然后调用dubbo服务做认证。
	public AuthDescriptor() {

	}

	public AuthDescriptor(String authAdress, String serviceId)  {
		this.authAdress=authAdress;
		this.serviceId = serviceId;
	}
	

	public String getAuthAdress() {
		return authAdress;
	}

	public void setAuthAdress(String authAdress) {
		this.authAdress = authAdress;
	}

	public String getServiceId() {
		return serviceId;
	}

	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}
	
}