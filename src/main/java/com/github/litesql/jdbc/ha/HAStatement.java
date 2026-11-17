package com.github.litesql.jdbc.ha;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;

import com.dbeaver.jdbc.model.AbstractJdbcStatement;
import com.github.litesql.jdbc.ha.client.Converter;
import com.github.litesql.jdbc.ha.client.HAExecutionResult;

public class HAStatement extends AbstractJdbcStatement<HAConnection> {

	private final HAConnection connection;

	protected String queryText;
	protected Map<Object, Object> parameters = new LinkedHashMap<>();
	private final List<String> sqlBatch = new ArrayList<>();

	protected HAExecutionResult executionResult;
	protected long updateCount = -1L;
	protected HAResultSet resultSet;

	protected int queryTimeout;
	private boolean closed;
	private boolean closeOnCompletion;

	public HAStatement(@NotNull HAConnection connection, int queryTimeout) throws SQLException {
		super(connection);
		this.connection = connection;
		this.queryTimeout = queryTimeout;
		this.closed = false;
	}

	protected void checkClosed() throws SQLException {
		if (isClosed()) {
			throw new HAException("Statement is closed");
		}
	}

	protected void resetExecutionState() {
		if (this.resultSet != null) {
			this.resultSet.closeInternal();
			this.resultSet = null;
		}
		this.executionResult = null;
		this.updateCount = -1L;
	}

	private static boolean hasOnlyIndexedParameters(Map<Object, Object> params) {
		if (params == null || params.isEmpty()) {
			return true;
		}
		for (Object key : params.keySet()) {
			if (!(key instanceof Number)) {
				return false;
			}
		}
		return true;
	}

	protected boolean shouldUseEmbeddedReplica(String sql, Map<Object, Object> params) {
		return this.connection.canUseEmbeddedReplica() && HAUtils.isSelectQuery(sql)
				&& hasOnlyIndexedParameters(params);
	}

	private HAExecutionResult executeQueryOnConnection(Connection conn, String sql, Map<Object, Object> params)
			throws SQLException {
		if (params == null || params.isEmpty()) {
			try (Statement stmt = conn.createStatement()) {
				configureLocalStatement(stmt);
				try (ResultSet rs = stmt.executeQuery(sql)) {
					return readLocalResultSet(rs);
				}
			}
		} else {
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				configureLocalStatement(pstmt);
				for (Map.Entry<Object, Object> entry : params.entrySet()) {
					int index = ((Number) entry.getKey()).intValue();
					Object val = Converter.unwrapForLocalJdbc(entry.getValue());
					if (val == null) {
						pstmt.setObject(index, null);
					} else {
						pstmt.setObject(index, val);
					}
				}
				try (ResultSet rs = pstmt.executeQuery()) {
					return readLocalResultSet(rs);
				}
			}
		}
	}

	private void configureLocalStatement(Statement stmt) throws SQLException {
		if (this.queryTimeout > 0) {
			stmt.setQueryTimeout(this.queryTimeout);
		}
		if (this.maxRows > 0 && this.maxRows <= Integer.MAX_VALUE) {
			stmt.setMaxRows((int) this.maxRows);
		}
	}

	private HAExecutionResult readLocalResultSet(ResultSet rs) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		List<String> columns = new ArrayList<>(columnCount);
		for (int i = 1; i <= columnCount; i++) {
			String label = rsmd.getColumnLabel(i);
			columns.add(label != null && !label.isEmpty() ? label : rsmd.getColumnName(i));
		}
		List<Object[]> rows = new ArrayList<>();
		while (rs.next()) {
			Object[] row = new Object[columnCount];
			for (int i = 1; i <= columnCount; i++) {
				row[i - 1] = rs.getObject(i);
			}
			rows.add(row);
			if (this.maxRows > 0 && rows.size() >= this.maxRows) {
				break;
			}
		}
		return new HAExecutionResult(columns, rows);
	}

	private HAExecutionResult applyMaxRows(HAExecutionResult res) {
		if (res != null && res.hasResultSet() && this.maxRows > 0 && res.getRows().size() > this.maxRows) {
			return new HAExecutionResult(res.getColumns(),
					new ArrayList<>(res.getRows().subList(0, (int) this.maxRows)));
		}
		return res;
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		checkClosed();
		resetExecutionState();
		if (shouldUseEmbeddedReplica(sql, parameters)) {
			executionResult = executeQueryOnConnection(this.connection.getEmbeddedReplica(), sql, parameters);
		} else {
			executionResult = applyMaxRows(connection.getClient().executeQuery(sql, parameters, this.queryTimeout));
		}
		this.resultSet = new HAResultSet(this, executionResult);
		return this.resultSet;
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		return executeQuery(queryText);
	}

	@Override
	protected boolean execute(@NotNull String sql, @Nullable int[] columnIndexes, @Nullable String[] columnNames,
			int autoGeneratedKeys) throws SQLException {
		checkClosed();
		resetExecutionState();
		if (shouldUseEmbeddedReplica(sql, parameters)) {
			executionResult = executeQueryOnConnection(this.connection.getEmbeddedReplica(), sql, parameters);
		} else {
			executionResult = applyMaxRows(connection.getClient().execute(sql, parameters, this.queryTimeout));
		}
		if (executionResult.hasResultSet()) {
			this.updateCount = -1L;
			return true;
		}
		this.updateCount = executionResult.getRowsAffected();
		return false;
	}

	@Override
	public boolean execute() throws SQLException {
		return execute(queryText, null, null, Statement.NO_GENERATED_KEYS);
	}

	@Override
	protected int executeUpdate(@NotNull String sql, @Nullable int[] columnIndexes, @Nullable String[] columnNames,
			int autoGeneratedKeys) throws SQLException {
		return (int) executeLargeUpdateInternal(sql, parameters);
	}

	@Override
	public int executeUpdate() throws SQLException {
		return (int) executeLargeUpdateInternal(queryText, parameters);
	}

	protected long executeLargeUpdateInternal(String sql, Map<Object, Object> params) throws SQLException {
		checkClosed();
		resetExecutionState();
		this.executionResult = connection.getClient().executeUpdateWithResult(sql, params, this.queryTimeout);
		this.updateCount = this.executionResult.getRowsAffected();
		return this.updateCount;
	}

	@Override
	public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
		return executeLargeUpdateInternal(sql, parameters);
	}

	@Override
	public long executeLargeUpdate(String sql) throws SQLException {
		return executeLargeUpdateInternal(sql, parameters);
	}

	@Override
	public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		return executeLargeUpdateInternal(sql, parameters);
	}

	@Override
	public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
		return executeLargeUpdateInternal(sql, parameters);
	}

	@Override
	public long executeLargeUpdate() throws SQLException {
		return executeLargeUpdateInternal(queryText, parameters);
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		checkClosed();
		if (sql == null) {
			throw new HAException("Batch SQL must not be null");
		}
		this.sqlBatch.add(sql);
	}

	@Override
	public void clearBatch() throws SQLException {
		checkClosed();
		this.sqlBatch.clear();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		long[] largeCounts = executeLargeBatch();
		int[] counts = new int[largeCounts.length];
		for (int i = 0; i < largeCounts.length; i++) {
			counts[i] = (int) largeCounts[i];
		}
		return counts;
	}

	@Override
	public long[] executeLargeBatch() throws SQLException {
		checkClosed();
		List<String> batchCopy = new ArrayList<>(this.sqlBatch);
		this.sqlBatch.clear();
		long[] counts = new long[batchCopy.size()];
		for (int i = 0; i < batchCopy.size(); i++) {
			try {
				counts[i] = executeLargeUpdateInternal(batchCopy.get(i), null);
			} catch (SQLException e) {
				int[] partial = new int[i];
				for (int j = 0; j < i; j++) {
					partial[j] = (int) counts[j];
				}
				throw new BatchUpdateException(e.getMessage(), e.getSQLState(), e.getErrorCode(), partial, e);
			}
		}
		return counts;
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		checkClosed();
		List<String> cols = Collections.singletonList("last_insert_rowid()");
		List<Object[]> rows;
		if (executionResult != null && !executionResult.hasResultSet() && executionResult.getLastInsertId() != 0L) {
			rows = Collections.singletonList(new Object[] { executionResult.getLastInsertId() });
		} else {
			rows = Collections.emptyList();
		}
		return new HAResultSet(this, new HAExecutionResult(cols, rows));
	}

	@Override
	public void close() throws SQLException {
		if (this.closed) {
			return;
		}
		this.closed = true;
		resetExecutionState();
		this.parameters.clear();
		this.sqlBatch.clear();
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		checkClosed();
		this.closeOnCompletion = true;
	}

	@Override
	public boolean isCloseOnCompletion() {
		return this.closeOnCompletion;
	}

	void onResultSetClosed() {
		if (this.closeOnCompletion) {
			try {
				close();
			} catch (SQLException ignored) {
			}
		}
	}

	@Override
	public void cancel() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		checkClosed();
		if (executionResult == null || !executionResult.hasResultSet()) {
			return null;
		}
		if (resultSet == null) {
			resultSet = new HAResultSet(this, executionResult);
		}
		return resultSet;
	}

	@Override
	public int getUpdateCount() throws SQLException {
		checkClosed();
		return (int) this.updateCount;
	}

	@Override
	public long getLargeUpdateCount() throws SQLException {
		checkClosed();
		return this.updateCount;
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		checkClosed();
		return this.queryTimeout;
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		checkClosed();
		if (seconds < 0) {
			throw new HAException("Query timeout must not be negative: " + seconds);
		}
		this.queryTimeout = seconds;
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		return getMoreResults(Statement.CLOSE_CURRENT_RESULT);
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		checkClosed();
		if (current == Statement.CLOSE_CURRENT_RESULT || current == Statement.CLOSE_ALL_RESULTS) {
			if (this.resultSet != null) {
				this.resultSet.closeInternal();
				this.resultSet = null;
			}
		}
		this.executionResult = null;
		this.updateCount = -1L;
		return false;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		checkClosed();
		if (direction != ResultSet.FETCH_FORWARD && direction != ResultSet.FETCH_REVERSE
				&& direction != ResultSet.FETCH_UNKNOWN) {
			throw new HAException("Invalid fetch direction: " + direction);
		}
		super.setFetchDirection(direction);
	}

	@Override
	public int getFetchDirection() throws SQLException {
		checkClosed();
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return this.closed || this.connection.isClosed();
	}

}
