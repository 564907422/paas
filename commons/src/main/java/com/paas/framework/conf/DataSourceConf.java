package com.paas.framework.conf;

import com.baidu.disconf.client.common.annotations.DisconfFile;
import com.baidu.disconf.client.common.annotations.DisconfFileItem;
import org.springframework.stereotype.Service;

@Service
@DisconfFile(filename = "jdbc.properties")
public class DataSourceConf {

	private String defaultDataSource;
	private String defaultDataSourceName;
	
    @DisconfFileItem(name = "defaultDataSource", associateField = "defaultDataSource")
	public String getDefaultDataSource() {
		return defaultDataSource;
	}
	public void setDefaultDataSource(String defaultDataSource) {
		this.defaultDataSource = defaultDataSource;
	}
	
    @DisconfFileItem(name = "default.datasource.name", associateField = "defaultDataSourceName")
	public String getDefaultDataSourceName() {
		return defaultDataSourceName;
	}
	public void setDefaultDataSourceName(String defaultDataSourceName) {
		this.defaultDataSourceName = defaultDataSourceName;
	}
	
	
}
