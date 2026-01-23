package com.github.litesql.jdbc.ha;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

public class HADataSource implements DataSource {
	
	private String url;
	
	private Properties properties;
	
	private transient PrintWriter logger;
	
	private int loginTimeout = 1;	

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Properties getProperties() {
		if(properties == null) {
			properties = new Properties();
		}
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException("getParentLogger");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isInstance(this);
	}

	@Override
	@SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return (T) this;
    }

	@Override
	public Connection getConnection() throws SQLException {
		return HADriver.getInstance().connect(this.url, this.properties);		
	}

	@Override
	public Connection getConnection(String user, String pass) throws SQLException {
		Properties props = (Properties) this.getProperties().clone();
		props.put("password", pass);
		return getConnection();
	}

	@Override
	public PrintWriter getLogWriter() throws SQLException {
		return this.logger;
	}

	@Override
	public int getLoginTimeout() throws SQLException {
		return this.loginTimeout;
	}

	@Override
	public void setLogWriter(PrintWriter out) throws SQLException {
		this.logger = out;
	}

	@Override
	public void setLoginTimeout(int seconds) throws SQLException {
		this.loginTimeout = seconds;
	}
	
	public void setPassword(String token) {
		if (this.properties == null) {
			this.properties = new Properties();
		}
		properties.put("password", token);
	}

}
