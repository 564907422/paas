package com.paas.auth.vo;

import java.io.Serializable;

public class AuthResult implements Serializable {
	private static final long serialVersionUID = -1064949882761359027L;

	public AuthResult() {
		//test
	}

	private String serviceId;
	private String zkAdress;
	private String msg;

	public String getServiceId() {
		return serviceId;
	}
	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}
	public String getZkAdress() {
		return zkAdress;
	}
	public void setZkAdress(String zkAdress) {
		this.zkAdress = zkAdress;
	}
	public String getMsg() {
		return msg;
	}
	public void setMsg(String msg) {
		this.msg = msg;
	}
	
	
}
