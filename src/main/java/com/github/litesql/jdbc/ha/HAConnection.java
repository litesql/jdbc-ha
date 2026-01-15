package com.github.litesql.jdbc.ha;

import java.io.IOException;
import java.net.URL;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jkiss.code.NotNull;

import com.dbeaver.jdbc.model.AbstractJdbcConnection;
import com.github.litesql.jdbc.ha.client.HAClient;
import com.github.litesql.jdbc.ha.client.HAExecutionResult;

public class HAConnection extends AbstractJdbcConnection {

	@NotNull
	private final HADriver driver;
	@NotNull
	private final HAClient client;
	@NotNull
	private final String url;
	@NotNull
	private final Map<String, Object> driverProperties;

	private boolean autoCommit;

	private HADatabaseMetaData databaseMetaData;
	
	private int queryTimeout;

	private Logger logger = Logger.getLogger("com.github.litesql.jdbc.driver.ha");

	public HAConnection(@NotNull HADriver driver, @NotNull String url, @NotNull Map<String, Object> driverProperties)
			throws SQLException {
		this.driver = driver;
		this.url = url;
		this.driverProperties = driverProperties;
		this.autoCommit = true;
		this.queryTimeout = 60;

		try {
			this.client = new HAClient(new URL(url));
		} catch (IOException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public void commit() throws SQLException {
		getClient().executeUpdate("COMMIT", null, this.queryTimeout);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return this.autoCommit;
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		try {
			getClient().executeQuery("SELECT 1", null, timeout);
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	@Override
	public void rollback() throws SQLException {
		getClient().executeUpdate("ROLLBACK", null, this.queryTimeout);
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		if (autoCommit == this.autoCommit) {
			return;
		}
		if (autoCommit) {
			this.commit();
		} else {
			getClient().executeUpdate("BEGIN", null, this.queryTimeout);
		}
		this.autoCommit = autoCommit;
	}

	@Override
	public void endRequest() throws SQLException {
		if (!this.autoCommit) {
			this.autoCommit = true;
			getClient().executeUpdate("COMMIT", null, this.queryTimeout);
		}
		try {
			getClient().executeUpdate("PRAGMA query_only = 0", null, this.queryTimeout);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Could not set PRAGMA query_only", e);
		}
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		HAExecutionResult result = getClient().executeQuery("PRAGMA query_only", null, this.queryTimeout);
		return result.getRows().get(0)[0].equals(1);
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		if (readOnly) {
			getClient().executeUpdate("PRAGMA query_only = 1", null, this.queryTimeout);
		} else {
			getClient().executeUpdate("PRAGMA query_only = 0", null, this.queryTimeout);
		}
	}

	/**
	 * Obtain transport client
	 */
	public HAClient getClient() {
		return client;
	}

	@NotNull
	public String getUrl() {
		return url;
	}

	@NotNull
	public Map<String, Object> getDriverProperties() {
		return driverProperties;
	}

	@NotNull
	public HADriver getDriver() {
		return driver;
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return new HAStatement(this);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		return prepareStatementImpl(sql);
	}

	@NotNull
	private HAPreparedStatement prepareStatementImpl(String sql) throws SQLException {
		return new HAPreparedStatement(this, sql);
	}

	@Override
	public void close() throws SQLException {
		client.close();
	}

	@Override
	public boolean isClosed() {
		return false;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		if (databaseMetaData == null) {
			databaseMetaData = new HADatabaseMetaData(this);
		}
		return databaseMetaData;
	}

}
