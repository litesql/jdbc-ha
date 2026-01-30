package com.github.litesql.jdbc.ha;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

public class HADataSource implements DataSource {

	private String url;

	private final Driver driver = new HADriver();

	private final Properties properties = new Properties();

	private transient PrintWriter logger;

	private int loginTimeout;

	public HADataSource() {
		try {
			setLoginTimeout(30);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Properties getProperties() {
		return properties;
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
		return driver.connect(this.url, this.properties);
	}

	@Override
	public Connection getConnection(String user, String pass) throws SQLException {
		Properties props = (Properties) this.getProperties().clone();
		props.put(HAConstants.CONNECTION_PROPERTY_PASSWORD, pass);
		return driver.connect(this.url, props);
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
		properties.put(HAConstants.CONNECTION_PROPERTY_LOGIN_TIMEOUT, String.valueOf(seconds));
	}
	
	public void setEnableSSL(boolean enable) {
		properties.put(HAConstants.CONNECTION_PROPERTY_ENABLE_SSL, String.valueOf(enable));	
	}

	public void setPassword(String token) {
		properties.put(HAConstants.CONNECTION_PROPERTY_PASSWORD, token);
	}

	public void setEmbeddedReplicasDir(String embeddedReplicasDir) {
		properties.put(HAConstants.CONNECTION_PROPERTY_EMBEDDED_REPLICAS_DIR, embeddedReplicasDir);
	}
	
	public void setReplicationURL(String url) {
		properties.put(HAConstants.CONNECTION_PROPERTY_REPLICATION_URL, url);	
	}
	
	public void setReplicationStream(String stream) {
		properties.put(HAConstants.CONNECTION_PROPERTY_REPLICATION_STREAM, stream);	
	}
	
	public void setReplicationDurable(String durable) {
		properties.put(HAConstants.CONNECTION_PROPERTY_REPLICATION_DURABLE, durable);	
	}

}
