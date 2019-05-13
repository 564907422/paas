package com.paas.framework.session;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface RequestEventObserver {
	public abstract void completed(HttpServletRequest paramHttpServletRequest,
                                   HttpServletResponse paramHttpServletResponse);
}
