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

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.Date;
import java.util.List;

import org.jkiss.code.NotNull;
import org.jkiss.utils.CommonUtils;

import com.dbeaver.jdbc.model.AbstractJdbcResultSetMetaData;
import com.github.litesql.jdbc.ha.client.HAExecutionResult;

public class HAResultSetMetaData extends AbstractJdbcResultSetMetaData<HAStatement> {

	private enum ResultColumnDataType {
		BOOLEAN, INTEGER, BIGINT, REAL, DOUBLE, BLOB, TIMESTAMP, STRING
	}

	@NotNull
	private final HAResultSet resultSet;

	public HAResultSetMetaData(@NotNull HAResultSet resultSet) throws SQLException {
		super(resultSet.getStatement());
		this.resultSet = resultSet;
	}

	private List<String> getColumnsValidated(int column) throws SQLException {
		HAExecutionResult result = resultSet.getResult();
		if (result == null) {
			throw new HAException("ResultSet has no metadata");
		}
		List<String> columns = result.getColumns();
		if (column < 1 || column > columns.size()) {
			throw new HAException("Column index out of bounds: " + column + "/" + columns.size());
		}
		return columns;
	}

	private ResultColumnDataType getDataTypeFromData(int column) throws SQLException {
		getColumnsValidated(column);
		HAExecutionResult result = resultSet.getResult();
		if (result != null && !CommonUtils.isEmpty(result.getRows())) {
			List<Object[]> rows = result.getRows();
			int scanLimit = Math.min(rows.size(), 100);
			for (int i = 0; i < scanLimit; i++) {
				Object[] row = rows.get(i);
				if (column - 1 >= row.length) {
					continue;
				}
				Object val = row[column - 1];
				if (val == null) {
					continue;
				}
				if (val instanceof Boolean) {
					return ResultColumnDataType.BOOLEAN;
				}
				if (val instanceof Integer || val instanceof Short || val instanceof Byte) {
					return ResultColumnDataType.INTEGER;
				}
				if (val instanceof Long) {
					return ResultColumnDataType.BIGINT;
				}
				if (val instanceof Float) {
					return ResultColumnDataType.REAL;
				}
				if (val instanceof Double || val instanceof BigDecimal || val instanceof Number) {
					return ResultColumnDataType.DOUBLE;
				}
				if (val instanceof byte[]) {
					return ResultColumnDataType.BLOB;
				}
				if (val instanceof Instant || val instanceof Date) {
					return ResultColumnDataType.TIMESTAMP;
				}
				return ResultColumnDataType.STRING;
			}
		}
		return ResultColumnDataType.STRING;
	}

	@Override
	public int getColumnCount() throws SQLException {
		return resultSet.getResult() == null ? 0 : resultSet.getResult().getColumns().size();
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		getColumnsValidated(column);
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		return getDataTypeFromData(column) == ResultColumnDataType.STRING;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		getColumnsValidated(column);
		return true;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		getColumnsValidated(column);
		return false;
	}

	@Override
	public int isNullable(int column) throws SQLException {
		getColumnsValidated(column);
		return columnNullable;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		switch (getDataTypeFromData(column)) {
		case INTEGER:
		case BIGINT:
		case REAL:
		case DOUBLE:
			return true;
		default:
			return false;
		}
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		getColumnsValidated(column);
		return 0;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		return getColumnsValidated(column).get(column - 1);
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return getColumnsValidated(column).get(column - 1);
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		getColumnsValidated(column);
		return 0;
	}

	@Override
	public int getScale(int column) throws SQLException {
		getColumnsValidated(column);
		return 0;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		getColumnsValidated(column);
		return "";
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		getColumnsValidated(column);
		return "";
	}

	@Override
	public String getTableName(int column) throws SQLException {
		getColumnsValidated(column);
		return "";
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		switch (getDataTypeFromData(column)) {
		case BOOLEAN:
			return Types.BOOLEAN;
		case INTEGER:
			return Types.INTEGER;
		case BIGINT:
			return Types.BIGINT;
		case REAL:
			return Types.REAL;
		case DOUBLE:
			return Types.DOUBLE;
		case BLOB:
			return Types.BLOB;
		case TIMESTAMP:
			return Types.TIMESTAMP;
		case STRING:
		default:
			return Types.VARCHAR;
		}
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		switch (getDataTypeFromData(column)) {
		case BOOLEAN:
			return "BOOLEAN";
		case INTEGER:
		case BIGINT:
			return "INTEGER";
		case REAL:
		case DOUBLE:
			return "REAL";
		case BLOB:
			return "BLOB";
		case TIMESTAMP:
			return "TIMESTAMP";
		case STRING:
		default:
			return "VARCHAR";
		}
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		switch (getDataTypeFromData(column)) {
		case BOOLEAN:
			return Boolean.class.getName();
		case INTEGER:
			return Integer.class.getName();
		case BIGINT:
			return Long.class.getName();
		case REAL:
			return Float.class.getName();
		case DOUBLE:
			return Double.class.getName();
		case BLOB:
			return byte[].class.getName();
		case TIMESTAMP:
			return Timestamp.class.getName();
		case STRING:
		default:
			return String.class.getName();
		}
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		getColumnsValidated(column);
		return false;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		getColumnsValidated(column);
		return true;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		getColumnsValidated(column);
		return false;
	}

}
