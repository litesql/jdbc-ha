package com.github.litesql.jdbc.driver.test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class HADriverTest {
	/**
	 * Runs simple select query in HA
	 *
	 * @param args mvn exec:java "-Dexec.args=database-url"
	 */
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		String serverUrl = "http://localhost:5001/chinook.db";
		try {
			if (args.length > 0) {
				serverUrl = args[0];
			}
			try (Connection connection = DriverManager.getConnection("jdbc:litesql:ha:" + serverUrl, null, null)) {
				DatabaseMetaData metaData = connection.getMetaData();
				System.out.println("Driver: " + metaData.getDriverName());
				System.out.println(
						"Database: " + metaData.getDatabaseProductName() + " " + metaData.getDatabaseProductVersion());

				System.out.println("Query:");
				try (Statement dbStat = connection.createStatement()) {
					try (ResultSet dbResult = dbStat.executeQuery("select rowid, name from Artist")) {
						printResultSet(dbResult);
					}
				}
				try (Statement dbStat = connection.createStatement()) {
					try (ResultSet dbResult = dbStat.executeQuery("select * from PRAGMA_TABLE_INFO('Artist')")) {
						printResultSet(dbResult);
					}
				}

				System.out.println("Tables:");
				try (ResultSet tables = metaData.getTables(null, null, null, null)) {
					while (tables.next()) {
						String tableName = tables.getString("TABLE_NAME");
						System.out.println("\t- " + tableName);
						try (ResultSet columns = metaData.getColumns(null, null, tableName, null)) {
							while (columns.next()) {
								System.out.println("\t\t- " + columns.getString("COLUMN_NAME") + " "
										+ columns.getString("TYPE_NAME"));
							}
						}

					}
				}

			}
		} finally {
			System.out.println("Finished (" + (System.currentTimeMillis() - startTime) + "ms)");
		}
	}

	private static void printResultSet(ResultSet dbResults) throws SQLException {
		ResultSetMetaData rsmd = dbResults.getMetaData();
		System.out.print("|");
		for (int i = 0; i < rsmd.getColumnCount(); i++) {
			System.out.print(rsmd.getColumnLabel(i + 1) + " " + rsmd.getColumnTypeName(i + 1) + "|");
		}
		System.out.println();

		while (dbResults.next()) {
			System.out.print("|");
			for (int i = 0; i < rsmd.getColumnCount(); i++) {
				Object value = dbResults.getObject(i + 1);
				System.out.print(value + "|");
			}
			System.out.println();
		}
	}
}
