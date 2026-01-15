package com.github.litesql.jdbc.ha;


import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HADriver implements Driver {
	
	private static HADriver instance; 

    static {
        Logger logger = Logger.getLogger("com.github.litesql.jdbc.driver.ha");
        parentLogger = logger;
        try {
            DriverManager.registerDriver(getInstance());
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Could not register driver", e);
        }
    }

    private static final java.util.logging.Logger parentLogger;
    
    static synchronized HADriver getInstance() {
    	if (instance == null) {
    		instance = new HADriver();
    	}
    	return instance;
    }
    
    private HADriver() {
    	
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        String targetUrl = HAUtils.validateAndFormatUrl(url);

        Map<String, Object> props = new LinkedHashMap<>();
        for (Enumeration<?> pne = info.propertyNames(); pne.hasMoreElements(); ) {
            String propName = (String) pne.nextElement();
            props.put(propName, info.get(propName));
        }
        return new HAConnection(this, targetUrl, props);
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
        return getMajorVersion() + "." + getMinorVersion() + "." + getMicroVersion() +
               " (" + HAConstants.DRIVER_INFO + ")";
    }
}
