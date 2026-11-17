package com.github.litesql.jdbc.ha.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HAExecutionResult {

	private final List<String> columns;
	private final List<Object[]> rows;
	private final long rowsAffected;
	private final long lastInsertId;
	private final boolean hasResultSet;

	public HAExecutionResult(List<String> columns, List<Object[]> rows) {
		this.columns = columns != null ? columns : new ArrayList<>();
		this.rows = rows != null ? rows : new ArrayList<>();
		this.rowsAffected = 0L;
		this.lastInsertId = 0L;
		this.hasResultSet = true;
	}

	public HAExecutionResult(long rowsAffected) {
		this(rowsAffected, 0L);
	}

	public HAExecutionResult(long rowsAffected, long lastInsertId) {
		this.columns = Collections.emptyList();
		this.rows = Collections.emptyList();
		this.rowsAffected = rowsAffected;
		this.lastInsertId = lastInsertId;
		this.hasResultSet = false;
	}

	public List<String> getColumns() {
		return columns;
	}

	public List<Object[]> getRows() {
		return rows;
	}

	public long getRowsAffected() {
		return this.rowsAffected;
	}

	public long getLastInsertId() {
		return this.lastInsertId;
	}

	public boolean hasResultSet() {
		return this.hasResultSet;
	}
}
