package com.paas.framework.conf.help;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ResourceUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * Created on 2016/9/27.
 */
public class EnvBeanHelp {

    public static final String ENV_LOCAL = "local";

    private final static Logger log = LoggerFactory.getLogger(EnvBeanHelp.class);
    private static String env = null;

    static {
        env = initEnv();
        log.info(" ---> init env [{}].", env);
    }

    private static String initEnv() {
        String env = System.getProperty("paas_env");
        if (env != null && env.trim().length() > 0) {
            return env.trim();
        }
        env = readEnvFromProfile("/etc/profile");
        if (env != null && env.trim().length() > 0) {
            return env.trim();
        }
        env = readEnvFromProfile("/etc/profile.d/paasenv.sh");
        if (env == null) {
            env = ENV_LOCAL;
        }
        return env.trim();
    }

    private static String readEnvFromProfile(String path) {
        File file = null;
        try {
            file = ResourceUtils.getFile(path);
        } catch (FileNotFoundException e) {
            log.info(" ---> file not exist.", e);
        }
        if (file == null || !file.exists()) {
            log.info(" ---> file not exist [{}].", path);
            return null;
        }
        String env = null;
        try (FileReader read = new FileReader(file);
             BufferedReader br = new BufferedReader(read)) {
            while (br.ready()) {
                String line = br.readLine();
                if (line != null && line.startsWith("paas_env=")) {
                    env = line.split("=")[1];
                    break;
                }
            }
        } catch (Exception e) {
            log.info(" ---> init env error.", e);
        }
        return env;
    }

    public static String getEnv() {
        return env;
    }

    public static void main(String[] args) {
        System.out.println(getEnv());
    }
}
