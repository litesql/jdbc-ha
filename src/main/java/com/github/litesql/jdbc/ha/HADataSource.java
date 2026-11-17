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

	private final HADriver driver = new HADriver();

	private final Properties properties = new Properties();

	private transient PrintWriter logger;

	private int loginTimeout;

	public HADataSource() {
		this.loginTimeout = HAConstants.DEFAULT_LOGIN_TIMEOUT;
		this.properties.setProperty(HAConstants.CONNECTION_PROPERTY_LOGIN_TIMEOUT,
				String.valueOf(HAConstants.DEFAULT_LOGIN_TIMEOUT));
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
		return driver.getParentLogger();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface != null && iface.isInstance(this);
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (isWrapperFor(iface)) {
			return iface.cast(this);
		}
		throw new SQLException("Not a wrapper for " + (iface != null ? iface.getName() : "null"));
	}

	@Override
	public Connection getConnection() throws SQLException {
		if (this.url == null || this.url.isEmpty()) {
			throw new HAException("DataSource URL must not be null or empty");
		}
		Connection conn = driver.connect(this.url, this.properties);
		if (conn == null) {
			throw new HAException("Invalid connection URL: " + this.url + ".\nExpected URL formats: "
					+ HAConstants.CONNECTION_URL_EXAMPLES);
		}
		return conn;
	}

	@Override
	public Connection getConnection(String user, String pass) throws SQLException {
		if (this.url == null || this.url.isEmpty()) {
			throw new HAException("DataSource URL must not be null or empty");
		}
		Properties props = (Properties) this.getProperties().clone();
		if (pass != null) {
			props.setProperty(HAConstants.CONNECTION_PROPERTY_PASSWORD, pass);
		} else {
			props.remove(HAConstants.CONNECTION_PROPERTY_PASSWORD);
		}
		Connection conn = driver.connect(this.url, props);
		if (conn == null) {
			throw new HAException("Invalid connection URL: " + this.url + ".\nExpected URL formats: "
					+ HAConstants.CONNECTION_URL_EXAMPLES);
		}
		return conn;
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
		if (seconds < 0) {
			throw new HAException("Login timeout must not be negative: " + seconds);
		}
		this.loginTimeout = seconds;
		properties.setProperty(HAConstants.CONNECTION_PROPERTY_LOGIN_TIMEOUT, String.valueOf(seconds));
	}

	public boolean isDisableSSL() {
		return Boolean.parseBoolean(properties.getProperty(HAConstants.CONNECTION_PROPERTY_DISABLE_SSL, "false"));
	}

	public void setDisableSSL(boolean b) {
		properties.setProperty(HAConstants.CONNECTION_PROPERTY_DISABLE_SSL, String.valueOf(b));
	}

	public String getPassword() {
		return properties.getProperty(HAConstants.CONNECTION_PROPERTY_PASSWORD);
	}

	public void setPassword(String token) {
		setOrRemoveProperty(HAConstants.CONNECTION_PROPERTY_PASSWORD, token);
	}

	public String getEmbeddedReplicasDir() {
		return properties.getProperty(HAConstants.CONNECTION_PROPERTY_EMBEDDED_REPLICAS_DIR);
	}

	public void setEmbeddedReplicasDir(String embeddedReplicasDir) {
		setOrRemoveProperty(HAConstants.CONNECTION_PROPERTY_EMBEDDED_REPLICAS_DIR, embeddedReplicasDir);
	}

	public String getReplicationURL() {
		return properties.getProperty(HAConstants.CONNECTION_PROPERTY_REPLICATION_URL);
	}

	public void setReplicationURL(String url) {
		setOrRemoveProperty(HAConstants.CONNECTION_PROPERTY_REPLICATION_URL, url);
	}

	public String getReplicationStream() {
		return properties.getProperty(HAConstants.CONNECTION_PROPERTY_REPLICATION_STREAM);
	}

	public void setReplicationStream(String stream) {
		setOrRemoveProperty(HAConstants.CONNECTION_PROPERTY_REPLICATION_STREAM, stream);
	}

	public String getReplicationDurable() {
		return properties.getProperty(HAConstants.CONNECTION_PROPERTY_REPLICATION_DURABLE);
	}

	public void setReplicationDurable(String durable) {
		setOrRemoveProperty(HAConstants.CONNECTION_PROPERTY_REPLICATION_DURABLE, durable);
	}

	public int getTimeout() {
		String val = properties.getProperty(HAConstants.CONNECTION_PROPERTY_TIMEOUT);
		return val != null ? Integer.parseInt(val) : HAConstants.DEFAULT_TIMEOUT;
	}

	public void setTimeout(int seconds) {
		properties.setProperty(HAConstants.CONNECTION_PROPERTY_TIMEOUT, String.valueOf(seconds));
	}

	private void setOrRemoveProperty(String key, String value) {
		if (value == null) {
			properties.remove(key);
		} else {
			properties.setProperty(key, value);
		}
	}

}
