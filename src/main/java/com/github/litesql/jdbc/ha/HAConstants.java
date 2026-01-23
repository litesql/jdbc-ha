package com.github.litesql.jdbc.ha;

import java.util.regex.Pattern;

public class HAConstants {

    public static final String CONNECTION_URL_EXAMPLES = "jdbc:litesql:ha:<server-url>, litesql://";
    public static final Pattern CONNECTION_URL_PATTERN = Pattern.compile("(jdbc:litesql:ha:|litesql://)(.+)");

    public static final int DRIVER_VERSION_MAJOR = 1;
    public static final int DRIVER_VERSION_MINOR = 0;
    public static final int DRIVER_VERSION_MICRO = 12;

    public static final String DRIVER_NAME = "LiteSQL HA";
    public static final String DRIVER_INFO = "LiteSQL HA JDBC driver";

    public static final String DEFAULT_ISO_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
}
