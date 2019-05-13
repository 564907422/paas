package com.paas.framework.session.filter;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.paas.framework.depend.SpringBeanFactory;
import com.paas.framework.session.impl.RequestEventSubject;
import com.paas.framework.session.impl.SessionHttpServletRequestWrapper;
import com.paas.framework.session.impl.SessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class CacheSessionFilter implements Filter {
	private static final Logger LOGGER = LoggerFactory.getLogger(CacheSessionFilter.class);

	//public static final String[] IGNORE_SUFFIX = { ".png", ".jpg", ".jpeg",
	//		".gif", ".css", ".js", ".html", ".htm" };
	public static String[] IGNORE_SUFFIX={};

	private SessionManager sessionManager;

	public void destroy() {

	}

	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
		 if(sessionManager==null){
			    sessionManager = SpringBeanFactory.getBean("sessionManager",SessionManager.class);
		 }
		 if(!sessionManager.isEnableSession()){
				filterChain.doFilter(servletRequest, servletResponse);
				return;
		}
			 
		HttpServletRequest request = (HttpServletRequest)servletRequest;

	    if (!shouldFilter(request)) {
	      filterChain.doFilter(servletRequest, servletResponse);
	      return;
	    }
	    HttpServletResponse response = (HttpServletResponse)servletResponse;
	    RequestEventSubject eventSubject = new RequestEventSubject();
	   
	    LOGGER.debug("---------sessionManager--{}---",sessionManager);
	    SessionHttpServletRequestWrapper requestWrapper = new SessionHttpServletRequestWrapper(request, response, this.sessionManager, eventSubject);
		try {
			filterChain.doFilter(requestWrapper, servletResponse);
		} catch (Exception e) {
			LOGGER.error("", e);
		} finally {
			eventSubject.completed(request, response);
		}
	  }

	  private boolean shouldFilter(HttpServletRequest request)
	  {
	    String uri = request.getRequestURI().toLowerCase();
        for (String suffix : IGNORE_SUFFIX) {
	      if (uri.endsWith(suffix)) return false;
	    }
	    return true;
	  }
	public void init(FilterConfig fc) throws ServletException {
		String ignore_suffix=fc.getInitParameter("ignore_suffix");
		if (!"".equals(ignore_suffix))
		    IGNORE_SUFFIX=fc.getInitParameter("ignore_suffix").split(",");
	}

}
