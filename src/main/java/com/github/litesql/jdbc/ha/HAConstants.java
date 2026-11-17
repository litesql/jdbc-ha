package com.github.litesql.jdbc.ha;

import java.util.regex.Pattern;

public class HAConstants {

	public static final String CONNECTION_URL_EXAMPLES = "jdbc:litesql:ha:<server-url>, litesql://<server-url>, litesqls://<server-url>, https://<server-url>";
	public static final Pattern CONNECTION_URL_PATTERN = Pattern
			.compile("(jdbc:litesql:ha:|litesqls?://|https://)(.+)");

	public static final int DEFAULT_TIMEOUT = 60;
	public static final int DEFAULT_LOGIN_TIMEOUT = 30;
	public static final String DEFAULT_REPLICATION_STREAM = "ha_replication";

	public static final int DRIVER_VERSION_MAJOR = 1;
	public static final int DRIVER_VERSION_MINOR = 3;
	public static final int DRIVER_VERSION_MICRO = 0;

	public static final String DRIVER_NAME = "LiteSQL HA";
	public static final String DRIVER_INFO = "LiteSQL HA JDBC driver";

	public static final String DEFAULT_ISO_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

	public static final String CONNECTION_PROPERTY_PASSWORD = "password";
	public static final String CONNECTION_PROPERTY_DISABLE_SSL = "disableSSL";
	public static final String CONNECTION_PROPERTY_EMBEDDED_REPLICAS_DIR = "embeddedReplicasDir";
	public static final String CONNECTION_PROPERTY_TIMEOUT = "timeout";
	public static final String CONNECTION_PROPERTY_LOGIN_TIMEOUT = "loginTimeout";
	public static final String CONNECTION_PROPERTY_REPLICATION_URL = "replicationURL";
	public static final String CONNECTION_PROPERTY_REPLICATION_STREAM = "replicationStream";
	public static final String CONNECTION_PROPERTY_REPLICATION_DURABLE = "replicationDurable";
}
