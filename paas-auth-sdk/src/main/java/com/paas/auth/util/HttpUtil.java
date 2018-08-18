package com.paas.auth.util;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import org.apache.commons.io.IOUtils;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * http工具类
 */
@SuppressWarnings("deprecation")
public class HttpUtil {

    public static final int connTimeout = 5000;
    public static final int readTimeout = 5000;
    public static final String charset = "UTF-8";

    private static Logger logger = LoggerFactory.getLogger(HttpUtil.class);


    /**
     * post 表单提交
     *
     * @param url
     * @param parameterStr 参数串
     * @return
     * @throws ConnectTimeoutException
     * @throws SocketTimeoutException
     * @throws Exception
     */
    public static String postParameters(String url, String parameterStr) throws Exception {
        return post(url, parameterStr, "application/x-www-form-urlencoded", charset, connTimeout, readTimeout);
    }

    /**
     * post 表单提交可设定参数
     *
     * @param url
     * @param parameterStr
     * @param charset
     * @param connTimeout
     * @param readTimeout
     * @return
     * @throws ConnectTimeoutException
     * @throws SocketTimeoutException
     * @throws Exception
     */
    public static String postParameters(String url, String parameterStr, String charset, Integer connTimeout, Integer readTimeout) throws Exception {
        return post(url, parameterStr, "application/x-www-form-urlencoded", charset, connTimeout, readTimeout);
    }

    /**
     * post 表单提交
     *
     * @param url
     * @param params map类型
     * @return
     * @throws ConnectTimeoutException
     * @throws SocketTimeoutException
     * @throws Exception
     */
    public static String postParameters(String url, Map<String, String> params) throws
            Exception {
        return postForm(url, params, null, connTimeout, readTimeout);
    }

    /**
     * post 表单提交 参数可设置
     *
     * @param url
     * @param params
     * @param connTimeout
     * @param readTimeout
     * @return
     * @throws ConnectTimeoutException
     * @throws SocketTimeoutException
     * @throws Exception
     */
    public static String postParameters(String url, Map<String, String> params, Integer connTimeout, Integer readTimeout) throws
            Exception {
        return postForm(url, params, null, connTimeout, readTimeout);
    }

    /**
     * get 请求
     *
     * @param url
     * @return
     * @throws Exception
     */
    public static String get(String url) throws Exception {
        return get(url, charset, null, null);
    }

    /**
     * get 请求可设参数
     *
     * @param url
     * @param charset
     * @return
     * @throws Exception
     */
    public static String get(String url, String charset) throws Exception {
        return get(url, charset, connTimeout, readTimeout);
    }

    /**
     * 发送一个 Post 请求, 使用指定的字符集编码.
     *
     * @param url
     * @param body        RequestBody
     * @param mimeType    例如 application/xml "application/x-www-form-urlencoded" a=1&b=2&c=3
     * @param charset     编码
     * @param connTimeout 建立链接超时时间,毫秒.
     * @param readTimeout 响应超时时间,毫秒.
     * @return ResponseBody, 使用指定的字符集编码.
     * @throws ConnectTimeoutException 建立链接超时异常
     * @throws SocketTimeoutException  响应超时
     * @throws Exception
     */
    public static String post(String url, String body, String mimeType, String charset, Integer connTimeout, Integer readTimeout)
            throws Exception {
        long startTime = System.currentTimeMillis();
        HttpClient client = null;
        HttpPost post = new HttpPost(url);
        String result = "";
        try {
            if (body!=null&&!"".equals(body)) {
                HttpEntity entity = new StringEntity(body, ContentType.create(mimeType, charset));
                post.setEntity(entity);
            }
            // 设置参数
            Builder customReqConf = RequestConfig.custom();
            if (connTimeout != null) {
                customReqConf.setConnectTimeout(connTimeout);
            }
            if (readTimeout != null) {
                customReqConf.setSocketTimeout(readTimeout);
            }
            post.setConfig(customReqConf.build());

            HttpResponse res;
            if (url.startsWith("https")) {
                client = createSSLInsecureClient();
                res = client.execute(post);
            } else {
                client = createDefaultClient();
                res = client.execute(post);
            }
            result = IOUtils.toString(res.getEntity().getContent(), charset);

            long endTime = System.currentTimeMillis();
            logger.info("HttpClient Method:[post] ,URL[" + url + "] ,Time:[" + (endTime - startTime) + "ms] ,result:" + result);
        } catch (ConnectTimeoutException ex) {
            logger.warn("连接超时", ex);
            throw ex;
        } catch (SocketTimeoutException ex) {
            logger.warn("响应超时", ex);
            throw ex;
        } catch (Exception ex) {
            logger.warn("连接异常", ex);
            throw ex;
        } finally {
            post.releaseConnection();
            if (url.startsWith("https") && client != null && client instanceof CloseableHttpClient) {
                ((CloseableHttpClient) client).close();
            }
        }
        return result;
    }


    /**
     * 提交form表单
     *
     * @param url
     * @param params
     * @param connTimeout
     * @param readTimeout
     * @return
     * @throws ConnectTimeoutException
     * @throws SocketTimeoutException
     * @throws Exception
     */
    public static String postForm(String url, Map<String, String> params, Map<String, String> headers, Integer connTimeout, Integer readTimeout)
            throws Exception {
        long startTime = System.currentTimeMillis();
        HttpClient client = null;
        HttpPost post = new HttpPost(url);
        String result = "";
        try {
            if (params != null && !params.isEmpty()) {
                List<NameValuePair> formParams = new ArrayList<NameValuePair>();
                Set<Entry<String, String>> entrySet = params.entrySet();
                for (Entry<String, String> entry : entrySet) {
                    formParams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
                }
                UrlEncodedFormEntity entity = new UrlEncodedFormEntity(formParams, Consts.UTF_8);
                post.setEntity(entity);
            }

            if (headers != null && !headers.isEmpty()) {
                for (Entry<String, String> entry : headers.entrySet()) {
                    post.addHeader(entry.getKey(), entry.getValue());
                }
            }
            // 设置参数  
            Builder customReqConf = RequestConfig.custom();
            if (connTimeout != null) {
                customReqConf.setConnectTimeout(connTimeout);
            }
            if (readTimeout != null) {
                customReqConf.setSocketTimeout(readTimeout);
            }
            post.setConfig(customReqConf.build());
            HttpResponse res = null;
            if (url.startsWith("https")) {
                // 执行 Https 请求.  
                client = createSSLInsecureClient();
                res = client.execute(post);
            } else {
                // 执行 Http 请求.  
                client = createDefaultClient();
                res = client.execute(post);
            }
            result = IOUtils.toString(res.getEntity().getContent(), charset);

            long endTime = System.currentTimeMillis();
            logger.info("HttpClient Method:[postForm] ,URL[" + url + "] ,Time:[" + (endTime - startTime) + "ms] ,result:" + result);
        } catch (ConnectTimeoutException ex) {
            logger.warn("连接超时", ex);
            throw ex;
        } catch (SocketTimeoutException ex) {
            logger.warn("响应超时", ex);
            throw ex;
        } catch (Exception ex) {
            logger.warn("连接异常", ex);
            throw ex;
        } finally {
            post.releaseConnection();
            if (url.startsWith("https") && client != null && client instanceof CloseableHttpClient) {
                ((CloseableHttpClient) client).close();
            }
        }
        return result;
    }


    /**
     * 发送一个 GET 请求
     *
     * @param url
     * @param charset
     * @param connTimeout 建立链接超时时间,毫秒.
     * @param readTimeout 响应超时时间,毫秒.
     * @return
     * @throws ConnectTimeoutException 建立链接超时
     * @throws SocketTimeoutException  响应超时
     * @throws Exception
     */
    public static String get(String url, String charset, Integer connTimeout, Integer readTimeout)
            throws Exception {
        long startTime = System.currentTimeMillis();
        HttpClient client = null;
        HttpGet get = new HttpGet(url);
        String result = "";
        try {
            // 设置参数  
            Builder customReqConf = RequestConfig.custom();
            if (connTimeout != null) {
                customReqConf.setConnectTimeout(connTimeout);
            }
            if (readTimeout != null) {
                customReqConf.setSocketTimeout(readTimeout);
            }
            get.setConfig(customReqConf.build());

            HttpResponse res = null;

            if (url.startsWith("https")) {
                // 执行 Https 请求.  
                client = createSSLInsecureClient();
                res = client.execute(get);
            } else {
                // 执行 Http 请求.  
                client = createDefaultClient();
                res = client.execute(get);
            }
            result = IOUtils.toString(res.getEntity().getContent(), charset);

            long endTime = System.currentTimeMillis();
            logger.info("HttpClient Method:[postForm] ,URL[" + url + "] ,Time:[ " + (endTime - startTime) + "ms ] ,result:" + result);
        } catch (ConnectTimeoutException ex) {
            logger.warn("连接超时", ex);
            throw ex;
        } catch (SocketTimeoutException ex) {
            logger.warn("响应超时", ex);
            throw ex;
        } catch (Exception ex) {
            logger.warn("连接异常", ex);
            throw ex;
        } finally {
            get.releaseConnection();
            if (url.startsWith("https") && client != null && client instanceof CloseableHttpClient) {
                ((CloseableHttpClient) client).close();
            }
        }
        return result;
    }


    /**
     * 从 response 里获取 charset
     *
     * @param ressponse
     * @return
     */
    @SuppressWarnings("unused")
    private static String getCharsetFromResponse(HttpResponse ressponse) {
        if (ressponse.getEntity() != null && ressponse.getEntity().getContentType() != null && ressponse.getEntity().getContentType().getValue() != null) {
            String contentType = ressponse.getEntity().getContentType().getValue();
            if (contentType.contains("charset=")) {
                return contentType.substring(contentType.indexOf("charset=") + 8);
            }
        }
        return null;
    }


    /**
     * 创建连接
     *
     * @return
     * @throws GeneralSecurityException
     */
    private static CloseableHttpClient createDefaultClient() {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        return httpclient;
    }

    /**
     * 创建 SSL连接
     *
     * @return
     * @throws GeneralSecurityException
     */
    private static CloseableHttpClient createSSLInsecureClient() throws GeneralSecurityException {
        try {
            SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
                public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                    return true;
                }
            }).build();

            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext, new X509HostnameVerifier() {

                @Override
                public boolean verify(String arg0, SSLSession arg1) {
                    return true;
                }

                @Override
                public void verify(String host, SSLSocket ssl) throws IOException {
                }

                @Override
                public void verify(String host, X509Certificate cert) throws SSLException {
                }

                @Override
                public void verify(String host, String[] cns, String[] subjectAlts) throws SSLException {
                }

            });
            return HttpClients.custom().setSSLSocketFactory(sslsf).build();
        } catch (GeneralSecurityException e) {
            throw e;
        }
    }
    
    
    
    /**
     * 发送一个 Post 请求, 使用指定的字符集编码.
     *
     * @param url
     * @param headers 
     * @param body        RequestBody
     * @param mimeType    例如 application/xml "application/x-www-form-urlencoded" a=1&b=2&c=3
     * @param charset     编码
     * @param connTimeout 建立链接超时时间,毫秒.
     * @param readTimeout 响应超时时间,毫秒.
     * @return ResponseBody, 使用指定的字符集编码.
     * @throws ConnectTimeoutException 建立链接超时异常
     * @throws SocketTimeoutException  响应超时
     * @throws Exception
     */
    public static String post(String url, Map<String, String> headers,String body, String mimeType, String charset, Integer connTimeout, Integer readTimeout)
            throws Exception {
        long startTime = System.currentTimeMillis();
        HttpClient client = null;
        HttpPost post = new HttpPost(url);
        String result = "";
        try {
            if (body!=null&&!"".equals(body)) {
                HttpEntity entity = new StringEntity(body, ContentType.create(mimeType, charset));
                post.setEntity(entity);
            }
            if (headers != null && !headers.isEmpty()) {
                for (Entry<String, String> entry : headers.entrySet()) {
                    post.addHeader(entry.getKey(), entry.getValue());
                }
            }
            // 设置参数
            Builder customReqConf = RequestConfig.custom();
            if (connTimeout != null) {
                customReqConf.setConnectTimeout(connTimeout);
            }
            if (readTimeout != null) {
                customReqConf.setSocketTimeout(readTimeout);
            }
            post.setConfig(customReqConf.build());

            HttpResponse res;
            if (url.startsWith("https")) {
                client = createSSLInsecureClient();
                res = client.execute(post);
            } else {
                client = createDefaultClient();
                res = client.execute(post);
            }
            result = IOUtils.toString(res.getEntity().getContent(), charset);

            long endTime = System.currentTimeMillis();
            logger.info("HttpClient Method:[post] ,URL[" + url + "] ,Time:[" + (endTime - startTime) + "ms] ,result:" + result);
        } catch (ConnectTimeoutException ex) {
            logger.warn("连接超时", ex);
            throw ex;
        } catch (SocketTimeoutException ex) {
            logger.warn("响应超时", ex);
            throw ex;
        } catch (Exception ex) {
            logger.warn("连接异常", ex);
            throw ex;
        } finally {
            post.releaseConnection();
            if (url.startsWith("https") && client != null && client instanceof CloseableHttpClient) {
                ((CloseableHttpClient) client).close();
            }
        }
        return result;
    }

}