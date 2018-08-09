package com.paas.zk.zookeeper.impl;

import com.paas.zk.constants.ConfigCenterConstants;
import com.paas.zk.zookeeper.ZKClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZKPoolFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKPoolFactory.class);

    private static final ZKPool zkPool = new ZKPool();

    public static ZKPool getZKPool(String zkAddress, String zkUser, String zkPasswd, int timeOut) throws Exception {
        validateParam(zkAddress, zkUser, zkPasswd);
        return getZkPool0(zkAddress, zkUser, zkPasswd, null, timeOut);
    }

    public static ZKPool getZKPool(String zkAddress, String zkUser, String zkPasswd, String serviceId, int timeOut) throws Exception {
        validateParam(zkAddress, zkUser, zkPasswd, serviceId);
        return getZkPool0(zkAddress, zkUser, zkPasswd, serviceId, timeOut);
    }

    public static ZKPool getZKPool(String zkAddress, String zkUser, String zkPasswd) throws Exception {
        return getZKPool(zkAddress, zkUser, zkPasswd, 60000);
    }

    public static ZKPool getZKPool(String zkAddress, String zkUser, String zkPasswd, String serviceId) throws Exception {
        return getZKPool(zkAddress, zkUser, zkPasswd, serviceId, 60000);
    }

    public static ZKPool getZKPool(String zkAddress, int timeOut, String... authInfo) throws Exception {
        String zkUser = null;
        String zkPasswd = null;
        if ((null != authInfo) && (authInfo.length >= 2)) {
            if (authInfo[0]!=null&&!"".equals(authInfo[0].trim())) {
                zkUser = authInfo[0];
            }
            if (authInfo[1]!=null&&!"".equals(authInfo[1].trim())) {
                zkPasswd = authInfo[1];
            }
        }
        return getZkPool0(zkAddress, zkUser, zkPasswd, null, timeOut);
    }

    private static ZKPool getZkPool0(String zkAddress, String zkUser, String zkPasswd, String serviceId, int timeOut) throws Exception {
        if (zkPool.exist(zkAddress, zkUser, serviceId)) {
            return zkPool;
        }
        ZKClient client = null;
        try {
            if (	zkUser!=null&&!"".equals(zkUser.trim())&&
        			zkPasswd!=null&&!"".equals(zkPasswd.trim())) {
                client = new ZKClient(zkAddress, timeOut, new String[]{ConfigCenterConstants.ZKAuthSchema.DIGEST, getAuthInfo(zkUser, zkPasswd)});
                client.addAuth(ConfigCenterConstants.ZKAuthSchema.DIGEST, getAuthInfo(zkUser, zkPasswd));
            } else {
                client = new ZKClient(zkAddress, timeOut, new String[]{});
            }
        } catch (Exception e) {
        		LOGGER.error("",e);
        }
        zkPool.addZKClient(zkAddress, zkUser, serviceId, client);
        return zkPool;
    }

    private static void validateParam(String zkAddress, String zkUser, String zkPasswd) {
//        Assert.notNull(zkAddress, ResourceUtil.getMessage(BundleKeyConstant.CONFIG_ADDRESS_IS_NULL));
//        Assert.notNull(zkUser, ResourceUtil.getMessage(BundleKeyConstant.USER_NAME_IS_NULL));
//        Assert.notNull(zkPasswd, ResourceUtil.getMessage(BundleKeyConstant.PASSWD_IS_NULL));
    }

    private static void validateParam(String zkAddress, String zkUser, String zkPasswd, String serviceId) {
//        Assert.notNull(zkAddress, ResourceUtil.getMessage(BundleKeyConstant.CONFIG_ADDRESS_IS_NULL));
//        Assert.notNull(zkUser, ResourceUtil.getMessage(BundleKeyConstant.USER_NAME_IS_NULL));
//        Assert.notNull(zkPasswd, ResourceUtil.getMessage(BundleKeyConstant.PASSWD_IS_NULL));
//        Assert.notNull(serviceId, ResourceUtil.getMessage(BundleKeyConstant.SERVICEID_IS_NULL));
    }

    private static String getAuthInfo(String zkUser, String zkPasswd) {
        return zkUser + ":" + zkPasswd;
    }



}
