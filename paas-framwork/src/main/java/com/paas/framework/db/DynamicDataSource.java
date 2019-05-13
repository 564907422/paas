package com.paas.framework.db;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

import java.util.List;
import java.util.Random;

/**
 * 多数据源切换
 */
public class DynamicDataSource extends AbstractRoutingDataSource {

    private static final ThreadLocal<String> dataSourceKey = new InheritableThreadLocal<String>();
    private static List<String> slaveDataSourceNames;
    private static String masterDataSourceName;
    private static String defaultDataSourceName;
    private static Random random = new Random();

    public static void setDataSourceKey(String dataSource) {
        dataSourceKey.set(dataSource);
    }

    @Override
    protected Object determineCurrentLookupKey() {
        return dataSourceKey.get();
    }

    public static String getSlaveDataSource() {
        return slaveDataSourceNames.get(random.nextInt(slaveDataSourceNames.size()));
    }


    public static void setSlaveDataSourceNames(List<String> slaveDataSourceNames) {
        DynamicDataSource.slaveDataSourceNames = slaveDataSourceNames;
    }

    public static String getDefaultDataSourceName() {
        return defaultDataSourceName;
    }

    public static void setDefaultDataSourceName(String defaultDataSourceName) {
        DynamicDataSource.defaultDataSourceName = defaultDataSourceName;
    }

    public static String getMasterDataSourceName() {
        return masterDataSourceName;
    }

    public static void setMasterDataSourceName(String masterDataSourceName) {
        DynamicDataSource.masterDataSourceName = masterDataSourceName;
    }
}
