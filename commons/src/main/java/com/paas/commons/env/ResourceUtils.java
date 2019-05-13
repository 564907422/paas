package com.paas.commons.env;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Created on 2016/9/27.
 */
public class ResourceUtils{
    private static Logger log = LoggerFactory.getLogger(ResourceUtils.class);
    public static final String CLASSPATH_URL_PREFIX = "classpath:";
    public static final String URL_PROTOCOL_FILE = "file";

    public static File getFile(String resourceLocation) {
        if (resourceLocation.startsWith(CLASSPATH_URL_PREFIX)) {
            String path = resourceLocation.substring(CLASSPATH_URL_PREFIX.length());
            String description = "class path resource [" + path + "]";
            ClassLoader cl = getDefaultClassLoader();
            URL url = (cl != null ? cl.getResource(path) : ClassLoader.getSystemResource(path));
            if (url == null) {
                log.info("{} cannot be resolved to absolute file path because it does not exist", description);
                return null;
            }
            return getFile(url, description);
        }
        try {
            // try URL
            return getFile(new URL(resourceLocation));
        }
        catch (MalformedURLException ex) {
            // no URL -> treat as file path
            return new File(resourceLocation);
        }
    }

    public static File getFile(URL resourceUrl) {
        return getFile(resourceUrl, "URL");
    }

    public static File getFile(URL resourceUrl, String description) {
        if (!URL_PROTOCOL_FILE.equals(resourceUrl.getProtocol())) {
            log.info("{} cannot be resolved to absolute file path because it does not reside in the file system: {}",
                    description, resourceUrl);
            return null;
        }
        try {
            return new File(toURI(resourceUrl).getSchemeSpecificPart());
        }
        catch (URISyntaxException ex) {
            // Fallback for URLs that are not valid URIs (should hardly ever happen).
            return new File(resourceUrl.getFile());
        }
    }

    public static URI toURI(URL url) throws URISyntaxException {
        return toURI(url.toString());
    }

    public static URI toURI(String location) throws URISyntaxException {
        if(location != null){
            return new URI(location.replaceAll("%20", " "));
        }
        return new URI(location);
    }

    public static ClassLoader getDefaultClassLoader() {
        ClassLoader cl = null;
        try {
            cl = Thread.currentThread().getContextClassLoader();
        }
        catch (Throwable ex) {
            // Cannot access thread context ClassLoader - falling back...
        }
        if (cl == null) {
            // No thread context class loader -> use class loader of this class.
            cl = ResourceUtils.class.getClassLoader();
            if (cl == null) {
                // getClassLoader() returning null indicates the bootstrap ClassLoader
                try {
                    cl = ClassLoader.getSystemClassLoader();
                }
                catch (Throwable ex) {
                    // Cannot access system ClassLoader - oh well, maybe the caller can live with null...
                }
            }
        }
        return cl;
    }

}
