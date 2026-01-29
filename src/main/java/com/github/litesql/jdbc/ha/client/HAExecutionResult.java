package com.github.litesql.jdbc.ha.client;

import java.util.ArrayList;
import java.util.List;

public class HAExecutionResult {
	
	private List<String> columns = new ArrayList<>();
    private List<Object[]> rows = new ArrayList<>();
    private long rowsAffected;
	
	public HAExecutionResult(List<String> columns, List<Object[]> rows) {
		this.columns = columns;
		this.rows = rows;
	}
	
	public HAExecutionResult(long rowsAffected) {
		this.rowsAffected = rowsAffected;		
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
}
