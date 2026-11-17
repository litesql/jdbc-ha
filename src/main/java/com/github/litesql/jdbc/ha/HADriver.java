package com.github.litesql.jdbc.ha;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
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

	private static final Logger parentLogger = Logger.getLogger("com.github.litesql.jdbc.driver.ha");

	private static String hostname = null;

	static {
		try {
			DriverManager.registerDriver(new HADriver());
		} catch (Exception e) {
			parentLogger.log(Level.SEVERE, "Could not register driver", e);
		}

		try {
			InetAddress localHost = InetAddress.getLocalHost();
			hostname = localHost.getHostName();
		} catch (UnknownHostException e) {
			parentLogger.log(Level.WARNING, "Could not get hostname", e);
		}
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if (!acceptsURL(url)) {
			return null;
		}
		String targetUrl = HAUtils.validateAndFormatUrl(url);

		Map<String, Object> props = new LinkedHashMap<>();
		if (info != null) {
			for (Enumeration<?> pne = info.propertyNames(); pne.hasMoreElements();) {
				String propName = String.valueOf(pne.nextElement());
				Object val = info.get(propName);
				if (val == null) {
					val = info.getProperty(propName);
				}
				if (val != null) {
					props.put(propName, val);
				}
			}
			for (Map.Entry<Object, Object> entry : info.entrySet()) {
				if (entry.getKey() != null && entry.getValue() != null) {
					props.put(String.valueOf(entry.getKey()), entry.getValue());
				}
			}
		}

		try {
			Map<String, String> queryParam = parseURLOptions(targetUrl);
			props.putAll(queryParam);
		} catch (MalformedURLException e) {
			throw new HAException("Invalid connection URL: " + url, e);
		}

		String replicationDir = toStringOrDefault(props.get(HAConstants.CONNECTION_PROPERTY_EMBEDDED_REPLICAS_DIR),
				System.getProperty("java.io.tmpdir"));
		String replicationURL = toStringOrDefault(props.get(HAConstants.CONNECTION_PROPERTY_REPLICATION_URL), null);
		String replicationStream = toStringOrDefault(props.get(HAConstants.CONNECTION_PROPERTY_REPLICATION_STREAM),
				HAConstants.DEFAULT_REPLICATION_STREAM);
		String replicationDurable = toStringOrDefault(props.get(HAConstants.CONNECTION_PROPERTY_REPLICATION_DURABLE),
				hostname);

		if (replicationURL != null && !replicationURL.isEmpty() && replicationDurable != null
				&& !replicationDurable.isEmpty()) {
			HAEmbeddedReplicasManager.load(replicationDir, replicationURL, replicationStream, replicationDurable);
		}

		int queryTimeout = HAConstants.DEFAULT_TIMEOUT;
		Object timeoutObj = props.get(HAConstants.CONNECTION_PROPERTY_TIMEOUT);
		if (timeoutObj != null && !timeoutObj.toString().trim().isEmpty()) {
			try {
				queryTimeout = Integer.parseInt(timeoutObj.toString().trim());
				if (queryTimeout < 0) {
					throw new HAException("Connection timeout must not be negative: " + queryTimeout);
				}
			} catch (NumberFormatException nfe) {
				throw new HAException("Invalid timeout value: " + timeoutObj, nfe);
			}
		}

		return new HAConnection(this, targetUrl, queryTimeout, props);
	}

	private static String toStringOrDefault(Object val, String defaultVal) {
		return val != null ? val.toString() : defaultVal;
	}

	private Map<String, String> parseURLOptions(String urlString) throws MalformedURLException {
		URL url = new URL(urlString);
		String queryString = url.getQuery();

		Map<String, String> queryParams = new HashMap<>();
		if (queryString != null && !queryString.isEmpty()) {
			String[] params = queryString.split("&");
			for (String param : params) {
				if (param.isEmpty()) {
					continue;
				}
				int eqIdx = param.indexOf('=');
				if (eqIdx > 0) {
					String key = URLDecoder.decode(param.substring(0, eqIdx), StandardCharsets.UTF_8);
					String value = URLDecoder.decode(param.substring(eqIdx + 1), StandardCharsets.UTF_8);
					queryParams.put(key, value);
				} else if (eqIdx < 0) {
					String key = URLDecoder.decode(param, StandardCharsets.UTF_8);
					queryParams.put(key, "");
				}
			}
		}

		return queryParams;
	}

	@Override
	public boolean acceptsURL(String url) {
		if (url == null || url.isEmpty()) {
			return false;
		}
		return HAConstants.CONNECTION_URL_PATTERN.matcher(url).matches();
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
		Properties effective = new Properties();
		if (info != null) {
			effective.putAll(info);
		}
		if (url != null && acceptsURL(url)) {
			try {
				String formatted = HAUtils.validateAndFormatUrl(url);
				effective.putAll(parseURLOptions(formatted));
			} catch (Exception ignored) {
			}
		}

		DriverPropertyInfo passwordProp = createProperty(HAConstants.CONNECTION_PROPERTY_PASSWORD,
				effective.getProperty(HAConstants.CONNECTION_PROPERTY_PASSWORD, ""),
				"Authentication token for the LiteSQL HA server");
		DriverPropertyInfo disableSslProp = createProperty(HAConstants.CONNECTION_PROPERTY_DISABLE_SSL,
				effective.getProperty(HAConstants.CONNECTION_PROPERTY_DISABLE_SSL, "false"),
				"Use plaintext gRPC connection without TLS");
		disableSslProp.choices = new String[] { "true", "false" };

		DriverPropertyInfo timeoutProp = createProperty(HAConstants.CONNECTION_PROPERTY_TIMEOUT,
				effective.getProperty(HAConstants.CONNECTION_PROPERTY_TIMEOUT,
						String.valueOf(HAConstants.DEFAULT_TIMEOUT)),
				"Default query timeout in seconds (0 means no limit)");
		DriverPropertyInfo embeddedDirProp = createProperty(HAConstants.CONNECTION_PROPERTY_EMBEDDED_REPLICAS_DIR,
				effective.getProperty(HAConstants.CONNECTION_PROPERTY_EMBEDDED_REPLICAS_DIR,
						System.getProperty("java.io.tmpdir")),
				"Local directory containing downloaded SQLite embedded replicas");
		DriverPropertyInfo repUrlProp = createProperty(HAConstants.CONNECTION_PROPERTY_REPLICATION_URL,
				effective.getProperty(HAConstants.CONNECTION_PROPERTY_REPLICATION_URL, ""),
				"NATS server URL for real-time replica synchronization");
		DriverPropertyInfo repStreamProp = createProperty(HAConstants.CONNECTION_PROPERTY_REPLICATION_STREAM,
				effective.getProperty(HAConstants.CONNECTION_PROPERTY_REPLICATION_STREAM,
						HAConstants.DEFAULT_REPLICATION_STREAM),
				"NATS JetStream stream name for replication");
		DriverPropertyInfo repDurableProp = createProperty(HAConstants.CONNECTION_PROPERTY_REPLICATION_DURABLE,
				effective.getProperty(HAConstants.CONNECTION_PROPERTY_REPLICATION_DURABLE,
						hostname != null ? hostname : ""),
				"Unique NATS durable consumer name for this replica node");

		return new DriverPropertyInfo[] { passwordProp, disableSslProp, timeoutProp, embeddedDirProp, repUrlProp,
				repStreamProp, repDurableProp };
	}

	private static DriverPropertyInfo createProperty(String name, String value, String description) {
		DriverPropertyInfo dpi = new DriverPropertyInfo(name, value);
		dpi.description = description;
		dpi.required = false;
		return dpi;
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
		return false;
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
