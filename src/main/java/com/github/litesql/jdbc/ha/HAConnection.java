package com.github.litesql.jdbc.ha;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jkiss.code.NotNull;
import org.jkiss.utils.CommonUtils;

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

	private HAEmbeddedReplicasManager.ReplicaConn embeddedReplicaManager;
	private Connection embeddedReplica;

	private volatile boolean closed;
	private boolean autoCommit;
	private boolean readOnlyState;

	private HADatabaseMetaData databaseMetaData;

	private final int queryTimeout;

	private final Logger logger = Logger.getLogger("com.github.litesql.jdbc.driver.ha");

	public HAConnection(@NotNull HADriver driver, @NotNull String url, int queryTimeout,
			@NotNull Map<String, Object> driverProperties) throws SQLException {
		this.driver = driver;
		this.url = url;
		this.driverProperties = driverProperties;
		this.autoCommit = true;
		this.readOnlyState = false;
		this.queryTimeout = queryTimeout;
		this.closed = false;

		try {
			String token = CommonUtils.toString(driverProperties.get(HAConstants.CONNECTION_PROPERTY_PASSWORD), null);
			boolean disableSSL = CommonUtils
					.toBoolean(driverProperties.get(HAConstants.CONNECTION_PROPERTY_DISABLE_SSL));
			this.client = new HAClient(new URL(url), token, disableSSL);
			refreshEmbeddedReplica(this.client.getReplicationID());
		} catch (IOException e) {
			throw new HAException("Failed to initialize HA connection", e);
		}
	}

	private void refreshEmbeddedReplica(String replicationId) throws SQLException {
		if (this.embeddedReplica != null) {
			try {
				this.embeddedReplica.close();
			} catch (SQLException ignored) {
			}
			this.embeddedReplica = null;
		}
		this.embeddedReplicaManager = HAEmbeddedReplicasManager.getReplica(replicationId);
		if (this.embeddedReplicaManager != null) {
			this.embeddedReplica = this.embeddedReplicaManager.createConn();
		}
	}

	void checkClosed() throws SQLException {
		if (this.closed) {
			throw new HAException("Connection is closed");
		}
	}

	@Override
	public void commit() throws SQLException {
		checkClosed();
		if (this.autoCommit) {
			throw new HAException("Cannot commit when autoCommit is enabled");
		}
		getClient().executeUpdate("COMMIT", null, this.queryTimeout);
		getClient().executeUpdate("BEGIN", null, this.queryTimeout);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		checkClosed();
		return this.autoCommit;
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		if (timeout < 0) {
			throw new HAException("Timeout must not be negative: " + timeout);
		}
		if (this.closed) {
			return false;
		}
		try {
			getClient().executeQuery("SELECT 1", null, timeout > 0 ? timeout : 5);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public void rollback() throws SQLException {
		checkClosed();
		if (this.autoCommit) {
			throw new HAException("Cannot rollback when autoCommit is enabled");
		}
		getClient().executeUpdate("ROLLBACK", null, this.queryTimeout);
		getClient().executeUpdate("BEGIN", null, this.queryTimeout);
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		checkClosed();
		if (autoCommit == this.autoCommit) {
			return;
		}
		if (autoCommit) {
			getClient().executeUpdate("COMMIT", null, this.queryTimeout);
		} else {
			getClient().executeUpdate("BEGIN", null, this.queryTimeout);
		}
		this.autoCommit = autoCommit;
	}

	@Override
	public void beginRequest() throws SQLException {
		checkClosed();
	}

	@Override
	public void endRequest() throws SQLException {
		if (this.closed) {
			return;
		}
		if (!this.autoCommit) {
			try {
				getClient().executeUpdate("ROLLBACK", null, this.queryTimeout);
			} finally {
				this.autoCommit = true;
			}
		}
		if (this.readOnlyState) {
			try {
				getClient().executeUpdate("PRAGMA query_only = 0", null, this.queryTimeout);
				this.readOnlyState = false;
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Could not reset PRAGMA query_only", e);
			}
		}
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		checkClosed();
		HAExecutionResult result = getClient().executeQuery("PRAGMA query_only", null, this.queryTimeout);
		if (result.getRows().isEmpty() || result.getRows().get(0).length == 0) {
			return this.readOnlyState;
		}
		Object val = result.getRows().get(0)[0];
		return CommonUtils.toInt(val) != 0 || Boolean.TRUE.equals(val);
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		checkClosed();
		if (readOnly) {
			getClient().executeUpdate("PRAGMA query_only = 1", null, this.queryTimeout);
		} else {
			getClient().executeUpdate("PRAGMA query_only = 0", null, this.queryTimeout);
		}
		this.readOnlyState = readOnly;
	}

	/**
	 * Obtain transport client
	 */
	public HAClient getClient() {
		return client;
	}

	protected Connection getEmbeddedReplica() {
		return this.embeddedReplica;
	}

	protected boolean canUseEmbeddedReplica() {
		if (!this.autoCommit || this.embeddedReplica == null || this.embeddedReplicaManager == null) {
			return false;
		}
		return this.embeddedReplicaManager.txseq >= getClient().getTxseq();
	}

	protected boolean isReplicaUpdated() {
		if (this.embeddedReplicaManager == null) {
			return false;
		}
		return this.embeddedReplicaManager.txseq >= getClient().getTxseq();
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
		checkClosed();
		return new HAStatement(this, this.queryTimeout);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		checkClosed();
		return prepareStatementImpl(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		checkClosed();
		return prepareStatementImpl(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		checkClosed();
		return prepareStatementImpl(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		checkClosed();
		return prepareStatementImpl(sql);
	}

	@NotNull
	private HAPreparedStatement prepareStatementImpl(String sql) throws SQLException {
		if (sql == null) {
			throw new HAException("SQL statement must not be null");
		}
		return new HAPreparedStatement(this, sql, this.queryTimeout);
	}

	@Override
	public synchronized void close() throws SQLException {
		if (this.closed) {
			return;
		}
		this.closed = true;
		SQLException replicaErr = null;
		if (embeddedReplica != null) {
			try {
				embeddedReplica.close();
			} catch (SQLException e) {
				replicaErr = e;
			} finally {
				embeddedReplica = null;
			}
		}
		client.close();
		if (replicaErr != null) {
			throw replicaErr;
		}
	}

	@Override
	public boolean isClosed() {
		return this.closed;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		checkClosed();
		if (databaseMetaData == null) {
			databaseMetaData = new HADatabaseMetaData(this);
		}
		return databaseMetaData;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		checkClosed();
		if (catalog == null || catalog.isEmpty()) {
			return;
		}
		setCatalogInternal(catalog);
	}

	void setCatalogInternal(String catalog) throws SQLException {
		String normalized = catalog != null ? catalog : "";
		if (Objects.equals(normalized, getClient().getReplicationID())) {
			return;
		}
		getClient().setReplicationID(normalized);
		refreshEmbeddedReplica(normalized);
	}

	@Override
	public String getCatalog() throws SQLException {
		checkClosed();
		return getClient().getReplicationID();
	}

}
