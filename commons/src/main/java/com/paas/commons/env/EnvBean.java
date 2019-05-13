package com.paas.commons.env;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Created on 2016/9/27.
 */
public class EnvBean {

    public static final String ENV_LOCAL = "local";
    public static final String ENV_PROD = "prod";

    private final static Logger log = LoggerFactory.getLogger(EnvBean.class);
    private static String env = null;

    static {
        env = initEnv();
        log.info(" ---> init env [{}].", env);
    }

    private static String initEnv() {
        String env = System.getProperty("bbtree_env");
        if (env != null && env.trim().length() > 0) {
            return env.trim();
        }
        env = readEnvFromProfile("/etc/profile");
        if(env != null && env.trim().length() > 0){
            return env.trim();
        }
        env = readEnvFromProfile("/etc/profile.d/bbtreeenv.sh");
        if (env == null) {
            env = ENV_LOCAL;
        }
        return env.trim();
    }

    private static String readEnvFromProfile(String path) {
        // String envFile = "/etc/profile";
        File file = ResourceUtils.getFile(path);
        if(file == null || !file.exists()){
            log.info(" ---> file not exist [{}].", path);
            return null;
        }
        String env = null;
        try (FileReader read = new FileReader(file);
             BufferedReader br = new BufferedReader(read)) {
            while (br.ready()) {
                String line = br.readLine();
                if (line != null && line.startsWith("bbtree_env=")) {
                    env = line.split("=")[1];
                    break;
                }
            }
        } catch (Exception e) {
            log.info(" ---> init env error.", e);
        }
        return env;
    }

//    private static String readFromEnvFile(){
//        String envFile ="/etc/profile.d/bbtreeenv.sh";
//        File file = ResourceUtils.getFile(envFile);
//        if(file == null){
//            return null;
//        }
//        String env = null;
//        try(FileReader read = new FileReader(file);BufferedReader br = new BufferedReader(read);){
//            while (br.ready()) {
//                String line = br.readLine();
//                if(line != null && line.startsWith("bbtree_env=")){
//                    env = line.split("=")[1];
//                    break;
//                }
//            }
//        }catch(Exception e){
//            // do nothing
//        }
//        return env;
//    }

    public static String getEnv() {
        return env;
    }

}
