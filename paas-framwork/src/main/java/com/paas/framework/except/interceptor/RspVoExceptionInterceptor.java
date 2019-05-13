package com.paas.framework.except.interceptor;


import com.google.common.base.Preconditions;
import com.paas.framework.constants.DefaultCodeEnum;
import com.paas.framework.except.util.JsonWriter;
import com.paas.framework.vo.RspVo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class RspVoExceptionInterceptor extends HandlerInterceptorAdapter implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(RspVoExceptionInterceptor.class);

    private final static Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private final static String DEFAULT_JSONP_FUNC_NAME = "callback";
    private JsonWriter jsonWriter;
    private Charset charset = DEFAULT_CHARSET;
    private String jsonpFuncName = DEFAULT_JSONP_FUNC_NAME;

    public RspVoExceptionInterceptor(JsonWriter jsonWriter) {
        this.jsonWriter = jsonWriter;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public void setJsonpFuncName(String jsonpFuncName) {
        this.jsonpFuncName = jsonpFuncName;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Preconditions.checkNotNull(this.jsonWriter, "property jsonWriter must be provided");
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex)
            throws Exception {
        if (ex != null) {
            logger.error("error with url " + request.getRequestURI() + " and with params " + request.getQueryString(),
                    ex);
            if (handler instanceof HandlerMethod) {
                HandlerMethod handlerMethod = (HandlerMethod) handler;
                ResponseBody annotation = handlerMethod.getMethodAnnotation(ResponseBody.class);
                if (annotation != null) {

                    RspVo vo = RspVo.error(DefaultCodeEnum.PARAMETERS_EX_CODE.code(), "参数有误");

                    String outputMessage;

                    if (request.getRequestURI().endsWith(".jsonp")) {
                        String func = request.getParameter(jsonpFuncName);
                        func = StringUtils.isEmpty(func) ? "null" : func;
                        outputMessage = jsonWriter.toJsonpString(vo, func);
                    } else {
                        outputMessage = jsonWriter.toJsonString(vo);
                    }

                    response.setContentType(request.getContentType());
                    OutputStream out = response.getOutputStream();
                    byte[] bytes = outputMessage.getBytes(charset);
                    out.write(bytes);
                    out.close();
                }
            }
        }
    }

    private String getAppMessage(Exception ex, String msg) {
        String[] lines = ex.getMessage().split("\r\n");
        if (lines.length > 0) {
            String[] rows = lines[0].split(":");
            if (rows.length > 1) {
                msg = StringUtils.trim(rows[1]);
            }
        }
        return msg;
    }

}
