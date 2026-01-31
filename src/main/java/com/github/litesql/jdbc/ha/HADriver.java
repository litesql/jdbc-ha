package com.github.litesql.jdbc.ha;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HADriver implements Driver {

	static {
		Logger logger = Logger.getLogger("com.github.litesql.jdbc.driver.ha");
		parentLogger = logger;
		try {
			DriverManager.registerDriver(new HADriver());
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Could not register driver", e);
		}

		try {
			InetAddress localHost = InetAddress.getLocalHost();
			hostname = localHost.getHostName();
		} catch (UnknownHostException e) {
			logger.log(Level.WARNING, "Could not get hostname", e);
		}

	}

	private static final java.util.logging.Logger parentLogger;

	private static String hostname = null;

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		String targetUrl = HAUtils.validateAndFormatUrl(url);		

		Map<String, Object> props = new LinkedHashMap<>();
		if (info != null) {
			for (Enumeration<?> pne = info.propertyNames(); pne.hasMoreElements();) {
				String propName = (String) pne.nextElement();
				props.put(propName, info.get(propName));
			}
		}

		try {
			Map<String, String> queryParam = parseURLOptions(targetUrl);
			props.putAll(queryParam);
		} catch (Exception e) {
		}

		String replicationDir = (String) props.getOrDefault(HAConstants.CONNECTION_PROPERTY_EMBEDDED_REPLICAS_DIR,
				System.getProperty("java.io.tmpdir"));
		String replicationURL = (String) props.get(HAConstants.CONNECTION_PROPERTY_REPLICATION_URL);
		String replicationStream = (String) props.getOrDefault(HAConstants.CONNECTION_PROPERTY_REPLICATION_STREAM,
				"ha_replication");
		String replicationDurable = (String) props.getOrDefault(HAConstants.CONNECTION_PROPERTY_REPLICATION_DURABLE,
				hostname);

		if (replicationURL != null && !replicationURL.isEmpty() && replicationDurable != null
				&& !replicationDurable.isEmpty()) {
			HAEmbeddedReplicasManager.load(replicationDir, replicationURL, replicationStream, replicationDurable);
		}
		
		int queryTimeout = 60;
		Object timeoutObj = props.get(HAConstants.CONNECTION_PROPERTY_TIMEOUT);
		if (timeoutObj != null) {
			try {
				queryTimeout = Integer.parseInt(timeoutObj.toString());
			} catch (NumberFormatException nfe) {
			}
		}
		
		return new HAConnection(this, targetUrl, queryTimeout, props);
	}

	private Map<String, String> parseURLOptions(String urlString)
			throws MalformedURLException, UnsupportedEncodingException {
		URL url = new URL(urlString);
		String queryString = url.getQuery();

		Map<String, String> queryParams = new HashMap<>();
		if (queryString != null) {
			String[] params = queryString.split("&");
			for (String param : params) {
				String[] keyValue = param.split("=");
				if (keyValue.length == 2) {
					String key = URLDecoder.decode(keyValue[0], "UTF-8");
					String value = URLDecoder.decode(keyValue[1], "UTF-8");
					queryParams.put(key, value);
				}
			}
		}

		return queryParams;

	}

	@Override
	public boolean acceptsURL(String url) {
		return HAConstants.CONNECTION_URL_PATTERN.matcher(url).matches();
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
		return new DriverPropertyInfo[] {

		};
	}

	@Override
	public int getMajorVersion() {
		return HAConstants.DRIVER_VERSION_MAJOR;
	}

	@Override
	public int getMinorVersion() {
		return HAConstants.DRIVER_VERSION_MINOR;
	}

	public int getMicroVersion() {
		return HAConstants.DRIVER_VERSION_MICRO;
	}

	@Override
	public boolean jdbcCompliant() {
		return true;
	}

	@Override
	public Logger getParentLogger() {
		return parentLogger;
	}

	public String getDriverName() {
		return HAConstants.DRIVER_NAME;
	}

	public String getFullVersion() {
		return getMajorVersion() + "." + getMinorVersion() + "." + getMicroVersion() + " (" + HAConstants.DRIVER_INFO
				+ ")";
	}
}
