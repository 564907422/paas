package com.paas.framework.conf;

import com.paas.framework.conf.help.EnvBeanHelp;
import com.paas.framework.conf.help.EnvHepler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;
import org.springframework.util.ClassUtils;

import java.io.File;
import java.net.URL;

public class EnvBean implements BeanDefinitionRegistryPostProcessor, PriorityOrdered, ApplicationContextAware {

    private static String env = null;
    private static Logger logger = LoggerFactory.getLogger(EnvBean.class);

    public synchronized static void init() {
        env = EnvBeanHelp.getEnv();
        logger.info("--------------bbtree  env--{}------------", env);
        setlocalEnv(env);

        String path = "disconf.properties";
        EnvHepler.replaceContentToFile(path, env);

    }

    private static String getProjectName() {
        ClassLoader cl = ClassUtils.getDefaultClassLoader();
        URL urlCan = (cl != null ? cl.getResource("log4j2.xml") : ClassLoader.getSystemResource("log4j2.xml"));
        String canPath = urlCan.getPath().substring(0, urlCan.getPath().indexOf(File.separator + "WEB-INF"));
        int ind = canPath.lastIndexOf(File.separator);
        return canPath.substring(ind);
    }

    private static void setlocalEnv(String env) {
        ClassLoader cl = ClassUtils.getDefaultClassLoader();
        URL url = (cl != null ? cl.getResource("env.properties") : ClassLoader.getSystemResource("env.properties"));
        String canPath = "";
        if (url == null) {
            String tempLog = "log4j2.xml";
            URL urlCan = (cl != null ? cl.getResource(tempLog) : ClassLoader.getSystemResource(tempLog));
            if (urlCan == null) {
                tempLog = "log4j.properties";
                urlCan = (cl != null ? cl.getResource(tempLog) : ClassLoader.getSystemResource(tempLog));
            }
            canPath = urlCan.getPath().substring(0, urlCan.getPath().length() - tempLog.length());
        } else {
            EnvHepler.deteleFile(url.getFile());
            canPath = url.getPath().substring(0, url.getPath().length() - "env.properties".length());
        }
        StringBuilder con = new StringBuilder();
        con.append("bbtree_env=").append(env).append("\n");
        EnvHepler.writeContentToFile(canPath + "env.properties", con.toString());
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getOrder() {
        // TODO Auto-generated method stub
        return Ordered.HIGHEST_PRECEDENCE + 2;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        // TODO Auto-generated method stub

    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
        // TODO Auto-generated method stub

    }


}
