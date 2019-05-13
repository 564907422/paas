package com.paas.framework.except.converter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import javax.servlet.http.HttpServletRequest;

import com.paas.framework.except.util.JsonWriter;
import com.paas.framework.vo.RspVo;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;


import com.google.common.base.Preconditions;

/**
 * JSON转换器.在webmvc的controller中,如果使用{@link ResponseBody}注解一个方法,并且返回值是
 * {@link RspVo}.那么结果可以被自动序列化为json 输出格式可以是json,也可以是jsonp,这取决于请求路径中是.json还是.jsonp
 * jsonp的参数名默认取决于callback参数,参数名可以配置
 * 
 * @author lijia
 *
 */
public class RspVoMessageConverter extends AbstractHttpMessageConverter<RspVo>implements InitializingBean {
	private final static Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	private final static String DEFAULT_JSONP_FUNC_NAME = "callback";
	private JsonWriter jsonWriter;
	private Charset charset;
	private String jsonpFuncName = DEFAULT_JSONP_FUNC_NAME;

	public RspVoMessageConverter(JsonWriter jsonWriter, Charset charset) {
		super(new MediaType("application", "json", charset), new MediaType("application", "*+json", charset));
		this.jsonWriter = jsonWriter;
		this.charset = charset;
	}

	public RspVoMessageConverter(JsonWriter jsonWriter) {
		this(jsonWriter, DEFAULT_CHARSET);
	}

	public void setJsonpFuncName(String jsonpFuncName) {
		this.jsonpFuncName = jsonpFuncName;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Preconditions.checkNotNull(this.jsonWriter, "property jsonWriter must be provided");
	}

	private HttpServletRequest getRequest() {
		return ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
	}

	private boolean requestJsonp(HttpServletRequest request) {
		return request.getRequestURI().endsWith(".jsonp");
	}

	private String getJsonpFunc(HttpServletRequest request) {
		String func = request.getParameter(jsonpFuncName);
		return StringUtils.isEmpty(func) ? "null" : func;
	}

	@Override
	protected boolean supports(Class<?> clazz) {
		return RspVo.class.isAssignableFrom(clazz);
	}

	@Override
	protected RspVo readInternal(Class<? extends RspVo> clazz, HttpInputMessage inputMessage)
			throws IOException, HttpMessageNotReadableException {
		// 这个方法调用不到,因为这个类只会用于json输出
		return null;
	}

	@Override
	protected void writeInternal(RspVo message, HttpOutputMessage outputMessage)
			throws IOException, HttpMessageNotWritableException {
		String output = null;

		HttpServletRequest request = getRequest();

		if (requestJsonp(request)) {
			String func = getJsonpFunc(request);
			output = jsonWriter.toJsonpString(message, func);
		} else {
			output = jsonWriter.toJsonString(message);
		}

		OutputStream out = outputMessage.getBody();
		byte[] bytes = output.getBytes(charset);
		out.write(bytes);
		out.flush();
	}
}
