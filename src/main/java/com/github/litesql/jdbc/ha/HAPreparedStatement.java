/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2024 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.litesql.jdbc.ha;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jkiss.code.NotNull;

import com.github.litesql.jdbc.ha.client.HAReaderInput;
import com.github.litesql.jdbc.ha.client.HAStreamInput;

public class HAPreparedStatement extends HAStatement implements PreparedStatement {

	private final List<Map<Object, Object>> parameterBatch = new ArrayList<>();

	public HAPreparedStatement(@NotNull HAConnection connection, String sql, int queryTimeout) throws SQLException {
		super(connection, queryTimeout);
		this.queryText = sql;
	}

	protected void addParameter(int parameterIndex, Object value) throws SQLException {
		checkClosed();
		if (parameterIndex < 1) {
			throw new HAException("Parameter index must be >= 1: " + parameterIndex);
		}
		this.parameters.put(parameterIndex, value);
	}

	public void setNamedParameter(String parameterName, Object value) throws SQLException {
		checkClosed();
		if (parameterName == null || parameterName.isEmpty()) {
			throw new HAException("Parameter name must not be empty");
		}
		this.parameters.put(parameterName, value);
	}

	protected void addParameter(String parameterName, Object value) throws SQLException {
		setNamedParameter(parameterName, value);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		this.addParameter(parameterIndex, null);
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		this.addParameter(parameterIndex,
				x == null ? null : new HAStreamInput(x, length, StandardCharsets.US_ASCII));
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		this.addParameter(parameterIndex, x == null ? null : new HAStreamInput(x, length, StandardCharsets.UTF_8));
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		this.addParameter(parameterIndex, x == null ? null : new HAStreamInput(x, length));
	}

	@Override
	public void clearParameters() throws SQLException {
		checkClosed();
		this.parameters.clear();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void addBatch() throws SQLException {
		checkClosed();
		this.parameterBatch.add(new LinkedHashMap<>(this.parameters));
	}

	@Override
	public void clearBatch() throws SQLException {
		checkClosed();
		this.parameterBatch.clear();
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
		List<Map<Object, Object>> batchCopy = new ArrayList<>(this.parameterBatch);
		this.parameterBatch.clear();
		long[] counts = new long[batchCopy.size()];
		for (int i = 0; i < batchCopy.size(); i++) {
			try {
				counts[i] = executeLargeUpdateInternal(this.queryText, batchCopy.get(i));
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
	public void close() throws SQLException {
		this.parameterBatch.clear();
		super.close();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		this.addParameter(parameterIndex, reader == null ? null : new HAReaderInput(reader, length));
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		checkClosed();
		ResultSet rs = getResultSet();
		return rs != null ? rs.getMetaData() : null;
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		if (x == null || cal == null) {
			this.addParameter(parameterIndex, x);
			return;
		}
		this.addParameter(parameterIndex, new Date(adjustToCalendar(x.getTime(), cal)));
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		if (x == null || cal == null) {
			this.addParameter(parameterIndex, x);
			return;
		}
		this.addParameter(parameterIndex, new Time(adjustToCalendar(x.getTime(), cal)));
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		if (x == null || cal == null) {
			this.addParameter(parameterIndex, x);
			return;
		}
		Timestamp adjusted = new Timestamp(adjustToCalendar(x.getTime(), cal));
		adjusted.setNanos(x.getNanos());
		this.addParameter(parameterIndex, adjusted);
	}

	private static long adjustToCalendar(long millis, Calendar targetCal) {
		Calendar defaultCal = Calendar.getInstance();
		int defaultOffset = defaultCal.getTimeZone().getOffset(millis);
		int targetOffset = targetCal.getTimeZone().getOffset(millis);
		return millis + (defaultOffset - targetOffset);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		this.addParameter(parameterIndex, null);
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		throw new SQLFeatureNotSupportedException("Parameters metadata not supported");
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		this.addParameter(parameterIndex, value);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		this.addParameter(parameterIndex, value == null ? null : new HAReaderInput(value, length));
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		this.addParameter(parameterIndex, value);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		this.addParameter(parameterIndex, reader == null ? null : new HAReaderInput(reader, length));
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		this.addParameter(parameterIndex, inputStream == null ? null : new HAStreamInput(inputStream, length));
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		this.addParameter(parameterIndex, reader == null ? null : new HAReaderInput(reader, length));
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		this.addParameter(parameterIndex, xmlObject);
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		this.addParameter(parameterIndex, x);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		this.addParameter(parameterIndex,
				x == null ? null : new HAStreamInput(x, length, StandardCharsets.US_ASCII));
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		this.addParameter(parameterIndex, x == null ? null : new HAStreamInput(x, length));
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		this.addParameter(parameterIndex, reader == null ? null : new HAReaderInput(reader, length));
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		this.addParameter(parameterIndex, x == null ? null : new HAStreamInput(x, -1, StandardCharsets.US_ASCII));
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		this.addParameter(parameterIndex, x == null ? null : new HAStreamInput(x, -1));
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		this.addParameter(parameterIndex, reader == null ? null : new HAReaderInput(reader, -1));
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		this.addParameter(parameterIndex, value == null ? null : new HAReaderInput(value, -1));
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		this.addParameter(parameterIndex, reader == null ? null : new HAReaderInput(reader, -1));
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		this.addParameter(parameterIndex, inputStream == null ? null : new HAStreamInput(inputStream, -1));
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		this.addParameter(parameterIndex, reader == null ? null : new HAReaderInput(reader, -1));
	}
}
