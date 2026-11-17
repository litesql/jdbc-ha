package com.github.litesql.jdbc.ha;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jkiss.code.NotNull;
import org.jkiss.utils.CommonUtils;

import com.dbeaver.jdbc.model.AbstractJdbcDatabaseMetaData;

/**
 * Queries related to keys and indexes were adapted from Xerial SQLite driver
 * (https://github.com/xerial/sqlite-jdbc)
 */
public class HADatabaseMetaData extends AbstractJdbcDatabaseMetaData<HAConnection> {

	private static final String PRODUCT_NAME = "SQLite HA";

	private final HAConnection connection;

	private volatile String serverVersion;

	@FunctionalInterface
	private interface SqlSupplier<T> {
		T get() throws SQLException;
	}

	public HADatabaseMetaData(@NotNull HAConnection connection) {
		super(connection, connection.getUrl());
		this.connection = connection;
	}

	private String readServerVersion() {
		String ver = this.serverVersion;
		if (ver != null) {
			return ver;
		}
		synchronized (this) {
			if (this.serverVersion == null) {
				try (ResultSet rs = executeQuery("SELECT sqlite_version()")) {
					if (rs.next()) {
						String v = rs.getString(1);
						if (v != null && !v.isEmpty()) {
							this.serverVersion = v;
							return v;
						}
					}
				} catch (SQLException ignored) {
				}
				this.serverVersion = "3.0.0";
			}
			return this.serverVersion;
		}
	}

	private <T> T executeInCatalog(String catalog, SqlSupplier<T> supplier) throws SQLException {
		String previousCatalog = connection.getCatalog();
		boolean switched = false;
		if (catalog != null && !catalog.isEmpty() && !catalog.equals(previousCatalog)) {
			connection.setCatalogInternal(catalog);
			switched = true;
		}
		try {
			return supplier.get();
		} finally {
			if (switched) {
				connection.setCatalogInternal(previousCatalog);
			}
		}
	}

	private String resolveEffectiveCatalog(String catalog) throws SQLException {
		if (catalog != null && !catalog.isEmpty()) {
			return catalog;
		}
		String current = connection.getCatalog();
		return (current != null && !current.isEmpty()) ? current : null;
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return PRODUCT_NAME;
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		return readServerVersion();
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		String ver = readServerVersion();
		String[] parts = ver.split("\\.");
		try {
			return parts.length > 0 ? Integer.parseInt(parts[0]) : 3;
		} catch (NumberFormatException e) {
			return 3;
		}
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		String ver = readServerVersion();
		String[] parts = ver.split("\\.");
		try {
			return parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
		} catch (NumberFormatException e) {
			return 0;
		}
	}

	@Override
	public String getDriverName() {
		return connection.getDriver().getDriverName();
	}

	@Override
	public String getDriverVersion() {
		return connection.getDriver().getFullVersion();
	}

	@Override
	public int getDriverMajorVersion() {
		return connection.getDriver().getMajorVersion();
	}

	@Override
	public int getDriverMinorVersion() {
		return connection.getDriver().getMinorVersion();
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
			throws SQLException {
		verifySchemaParameters(catalog, schemaPattern);
		return executeInCatalog(catalog, () -> {
			String effectiveCat = resolveEffectiveCatalog(catalog);
			StringBuilder sql = new StringBuilder();
			sql.append("SELECT ").append(HAUtils.quote(effectiveCat)).append(" AS TABLE_CAT, ")
					.append("NULL AS TABLE_SCHEM, ").append("name AS TABLE_NAME, ")
					.append("CASE WHEN type = 'table' AND name LIKE 'sqlite_%' THEN 'SYSTEM TABLE' ")
					.append("ELSE UPPER(type) END AS TABLE_TYPE, ")
					.append("NULL AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, ")
					.append("NULL AS SELF_REFERENCING_COL_NAME, NULL AS REF_GENERATION ")
					.append("FROM sqlite_master WHERE ");

			if (types == null || types.length == 0) {
				sql.append("type IN ('table', 'view')");
			} else {
				List<String> typeClauses = new ArrayList<>();
				for (String t : types) {
					if (t == null) {
						continue;
					}
					String upper = t.trim().toUpperCase(Locale.ENGLISH);
					if ("TABLE".equals(upper)) {
						typeClauses.add("(type = 'table' AND name NOT LIKE 'sqlite_%')");
					} else if ("SYSTEM TABLE".equals(upper)) {
						typeClauses.add("(type = 'table' AND name LIKE 'sqlite_%')");
					} else if ("VIEW".equals(upper)) {
						typeClauses.add("(type = 'view')");
					} else {
						typeClauses.add("(UPPER(type) = " + HAUtils.quote(upper) + ")");
					}
				}
				if (typeClauses.isEmpty()) {
					sql.append("1 = 0");
				} else {
					sql.append("(").append(String.join(" OR ", typeClauses)).append(")");
				}
			}

			if (!CommonUtils.isEmpty(tableNamePattern) && !"%".equals(tableNamePattern)) {
				sql.append(" AND name LIKE ").append(HAUtils.quote(tableNamePattern));
			}

			sql.append(" ORDER BY TABLE_TYPE, TABLE_SCHEM, TABLE_NAME");
			return executeQuery(sql.toString());
		});
	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
			throws SQLException {
		verifySchemaParameters(catalog, schemaPattern);
		return executeInCatalog(catalog, () -> {
			String effectiveCat = resolveEffectiveCatalog(catalog);
			String tblFilter = (!CommonUtils.isEmpty(tableNamePattern) && !"%".equals(tableNamePattern))
					? " AND name LIKE " + HAUtils.quote(tableNamePattern)
					: "";
			String colFilter = (!CommonUtils.isEmpty(columnNamePattern) && !"%".equals(columnNamePattern))
					? " WHERE pti.name LIKE " + HAUtils.quote(columnNamePattern)
					: "";

			String dataTypeCase = "CASE " + "WHEN UPPER(pti.type) LIKE '%INT%' THEN " + Types.INTEGER + " "
					+ "WHEN UPPER(pti.type) LIKE '%CHAR%' OR UPPER(pti.type) LIKE '%CLOB%' OR UPPER(pti.type) LIKE '%TEXT%' THEN "
					+ Types.VARCHAR + " " + "WHEN UPPER(pti.type) LIKE '%BLOB%' THEN " + Types.BLOB + " "
					+ "WHEN UPPER(pti.type) LIKE '%REAL%' OR UPPER(pti.type) LIKE '%FLOA%' OR UPPER(pti.type) LIKE '%DOUB%' THEN "
					+ Types.DOUBLE + " " + "WHEN UPPER(pti.type) LIKE '%BOOL%' THEN " + Types.BOOLEAN + " "
					+ "WHEN UPPER(pti.type) LIKE '%DATE%' OR UPPER(pti.type) LIKE '%TIME%' THEN " + Types.TIMESTAMP
					+ " " + "WHEN pti.type = '' OR pti.type IS NULL THEN " + Types.VARCHAR + " " + "ELSE "
					+ Types.NUMERIC + " END";

			String sql = "WITH all_tables AS (SELECT name AS tn FROM sqlite_master WHERE type IN ('table', 'view')"
					+ tblFilter + ") " + "SELECT " + HAUtils.quote(effectiveCat) + " AS TABLE_CAT, "
					+ "NULL AS TABLE_SCHEM, " + "at.tn AS TABLE_NAME, " + "pti.name AS COLUMN_NAME, " + dataTypeCase
					+ " AS DATA_TYPE, " + "pti.type AS TYPE_NAME, " + "0 AS COLUMN_SIZE, " + "0 AS BUFFER_LENGTH, "
					+ "0 AS DECIMAL_DIGITS, " + "10 AS NUM_PREC_RADIX, "
					+ "CASE WHEN pti.\"notnull\" = 1 OR pti.pk > 0 THEN " + columnNoNulls + " ELSE " + columnNullable
					+ " END AS NULLABLE, " + "NULL AS REMARKS, " + "pti.dflt_value AS COLUMN_DEF, "
					+ "0 AS SQL_DATA_TYPE, " + "0 AS SQL_DATETIME_SUB, " + "0 AS CHAR_OCTET_LENGTH, "
					+ "(pti.cid + 1) AS ORDINAL_POSITION, "
					+ "CASE WHEN pti.\"notnull\" = 1 OR pti.pk > 0 THEN 'NO' ELSE 'YES' END AS IS_NULLABLE, "
					+ "NULL AS SCOPE_CATALOG, " + "NULL AS SCOPE_SCHEMA, " + "NULL AS SCOPE_TABLE, "
					+ "NULL AS SOURCE_DATA_TYPE, " + "'' AS IS_AUTOINCREMENT, " + "'' AS IS_GENERATEDCOLUMN "
					+ "FROM all_tables at INNER JOIN pragma_table_info(at.tn) pti" + colFilter
					+ " ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION";
			return executeQuery(sql);
		});
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String tableName) throws SQLException {
		return executeInCatalog(catalog, () -> {
			String effectiveCat = resolveEffectiveCatalog(catalog);
			PrimaryKeyFinder pkFinder = new PrimaryKeyFinder(connection, tableName);
			String[] columns = pkFinder.getColumns();

			StringBuilder sql = new StringBuilder();
			sql.append("select ").append(HAUtils.quote(effectiveCat)).append(" as TABLE_CAT, null as TABLE_SCHEM, ")
					.append(HAUtils.quote(tableName))
					.append(" as TABLE_NAME, cn as COLUMN_NAME, ks as KEY_SEQ, pk as PK_NAME from (");

			if (columns == null || columns.length == 0) {
				sql.append("select null as cn, null as pk, 0 as ks) limit 0;");
				return executeQuery(sql.toString());
			}

			String quotedPkName = HAUtils.quote(pkFinder.getName());

			for (int i = 0; i < columns.length; i++) {
				if (i > 0) {
					sql.append(" union all ");
				}
				sql.append("select ").append(quotedPkName).append(" as pk, ")
						.append(HAUtils.quote(HAUtils.unquote(columns[i]).trim())).append(" as cn, ").append(i + 1)
						.append(" as ks");
			}

			sql.append(") order by cn;");
			return executeQuery(sql.toString());
		});
	}

	private static class IndexInfo {
		final String indexName;
		final int uniqueFlag;

		IndexInfo(String indexName, int uniqueFlag) {
			this.indexName = indexName;
			this.uniqueFlag = uniqueFlag;
		}
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
			throws SQLException {
		return executeInCatalog(catalog, () -> {
			String effectiveCat = resolveEffectiveCatalog(catalog);
			StringBuilder sql = new StringBuilder();

			sql.append("select ").append(HAUtils.quote(effectiveCat)).append(" as TABLE_CAT, null as TABLE_SCHEM, ")
					.append(HAUtils.quote(table))
					.append(" as TABLE_NAME, un as NON_UNIQUE, null as INDEX_QUALIFIER, n as INDEX_NAME, ")
					.append(DatabaseMetaData.tableIndexOther).append(" as TYPE, op as ORDINAL_POSITION, ")
					.append("cn as COLUMN_NAME, null as ASC_OR_DESC, 0 as CARDINALITY, 0 as PAGES, null as FILTER_CONDITION from (");

			List<IndexInfo> indexList = new ArrayList<>();
			if (table != null && !table.isEmpty()) {
				try (ResultSet rs = executeQuery("pragma index_list(" + HAUtils.quote(table) + ")")) {
					while (rs.next()) {
						int isUnique = rs.getInt(3);
						if (unique && isUnique == 0) {
							continue;
						}
						indexList.add(new IndexInfo(rs.getString(2), isUnique));
					}
				}
			}

			List<String> unionAll = new ArrayList<>();
			for (IndexInfo currentIndex : indexList) {
				String indexName = currentIndex.indexName;
				try (ResultSet rs = executeQuery("pragma index_info(" + HAUtils.quote(indexName) + ")")) {
					while (rs.next()) {
						StringBuilder sqlRow = new StringBuilder();
						String colName = rs.getString(3);
						sqlRow.append("select ").append(1 - currentIndex.uniqueFlag).append(" as un, ")
								.append(HAUtils.quote(indexName)).append(" as n, ").append(rs.getInt(1) + 1)
								.append(" as op, ").append(HAUtils.quote(colName)).append(" as cn");
						unionAll.add(sqlRow.toString());
					}
				}
			}

			if (unionAll.isEmpty()) {
				sql.append("select null as un, null as n, null as op, null as cn) limit 0;");
				return executeQuery(sql.toString());
			}

			sql.append(String.join(" union all ", unionAll))
					.append(") ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION;");
			return executeQuery(sql.toString());
		});
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
		return executeInCatalog(catalog, () -> {
			String effectiveCat = resolveEffectiveCatalog(catalog);
			StringBuilder sql = new StringBuilder();

			sql.append("select ").append(HAUtils.quote(effectiveCat)).append(" as PKTABLE_CAT, ")
					.append("NULL as PKTABLE_SCHEM, ").append("ptn as PKTABLE_NAME, pcn as PKCOLUMN_NAME, ")
					.append(HAUtils.quote(effectiveCat)).append(" as FKTABLE_CAT, ").append(HAUtils.quote(schema))
					.append(" as FKTABLE_SCHEM, ").append(HAUtils.quote(table)).append(" as FKTABLE_NAME, ")
					.append("fcn as FKCOLUMN_NAME, ks as KEY_SEQ, ur as UPDATE_RULE, dr as DELETE_RULE, fkn as FK_NAME, pkn as PK_NAME, ")
					.append(DatabaseMetaData.importedKeyInitiallyDeferred).append(" as DEFERRABILITY from (");

			try (ResultSet rs = executeQuery("pragma foreign_key_list(" + HAUtils.quote(table) + ")")) {
				final ImportedKeyFinder impFkFinder = new ImportedKeyFinder(connection, table);
				List<ImportedKeyFinder.ForeignKey> fkNames = impFkFinder.getFkList();

				int i = 0;
				for (; rs.next(); i++) {
					int keySeq = rs.getInt(2) + 1;
					int keyId = rs.getInt(1);
					String PKTabName = rs.getString(3);
					String FKColName = rs.getString(4);
					String PKColName = rs.getString(5);

					String pkName = null;
					try {
						PrimaryKeyFinder pkFinder = new PrimaryKeyFinder(connection, PKTabName);
						pkName = pkFinder.getName();
						if (PKColName == null && pkFinder.getColumns() != null && pkFinder.getColumns().length > 0) {
							PKColName = pkFinder.getColumns()[0];
						}
					} catch (SQLException ignored) {
					}

					String updateRule = rs.getString(6);
					String deleteRule = rs.getString(7);

					if (i > 0) {
						sql.append(" union all ");
					}

					String fkName = null;
					if (fkNames.size() > keyId) {
						fkName = fkNames.get(keyId).getFkName();
					}

					sql.append("select ").append(keySeq).append(" as ks, ").append(HAUtils.quote(PKTabName))
							.append(" as ptn, ").append(HAUtils.quote(FKColName)).append(" as fcn, ")
							.append(HAUtils.quote(PKColName)).append(" as pcn, ").append("case ")
							.append(HAUtils.quote(updateRule)).append(" when 'NO ACTION' then ")
							.append(DatabaseMetaData.importedKeyNoAction).append(" when 'CASCADE' then ")
							.append(DatabaseMetaData.importedKeyCascade).append(" when 'RESTRICT' then ")
							.append(DatabaseMetaData.importedKeyRestrict).append(" when 'SET NULL' then ")
							.append(DatabaseMetaData.importedKeySetNull).append(" when 'SET DEFAULT' then ")
							.append(DatabaseMetaData.importedKeySetDefault).append(" else ")
							.append(DatabaseMetaData.importedKeyNoAction).append(" end as ur, ").append("case ")
							.append(HAUtils.quote(deleteRule)).append(" when 'NO ACTION' then ")
							.append(DatabaseMetaData.importedKeyNoAction).append(" when 'CASCADE' then ")
							.append(DatabaseMetaData.importedKeyCascade).append(" when 'RESTRICT' then ")
							.append(DatabaseMetaData.importedKeyRestrict).append(" when 'SET NULL' then ")
							.append(DatabaseMetaData.importedKeySetNull).append(" when 'SET DEFAULT' then ")
							.append(DatabaseMetaData.importedKeySetDefault).append(" else ")
							.append(DatabaseMetaData.importedKeyNoAction).append(" end as dr, ")
							.append(fkName == null ? "''" : HAUtils.quote(fkName)).append(" as fkn, ")
							.append(pkName == null ? "''" : HAUtils.quote(pkName)).append(" as pkn");
				}
				if (i == 0) {
					sql.append("select -1 as ks, '' as ptn, '' as fcn, '' as pcn, ")
							.append(DatabaseMetaData.importedKeyNoAction).append(" as ur, ")
							.append(DatabaseMetaData.importedKeyNoAction)
							.append(" as dr, '' as fkn, '' as pkn) limit 0;");
				} else {
					sql.append(") ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ");
				}
			}
			return executeQuery(sql.toString());
		});
	}

	private static final Map<String, Integer> RULE_MAP = new HashMap<>();

	static {
		RULE_MAP.put("NO ACTION", DatabaseMetaData.importedKeyNoAction);
		RULE_MAP.put("CASCADE", DatabaseMetaData.importedKeyCascade);
		RULE_MAP.put("RESTRICT", DatabaseMetaData.importedKeyRestrict);
		RULE_MAP.put("SET NULL", DatabaseMetaData.importedKeySetNull);
		RULE_MAP.put("SET DEFAULT", DatabaseMetaData.importedKeySetDefault);
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
		return executeInCatalog(catalog, () -> {
			String effectiveCat = resolveEffectiveCatalog(catalog);
			PrimaryKeyFinder pkFinder = new PrimaryKeyFinder(connection, table);
			String[] pkColumns = pkFinder.getColumns();

			String quotedCatalog = HAUtils.quote(effectiveCat);
			String quotedSchema = HAUtils.quote(schema);

			StringBuilder exportedKeysQuery = new StringBuilder();

			String target = table;
			int count = 0;
			if (pkColumns != null) {
				ArrayList<String> tableList = new ArrayList<>();
				try (ResultSet rs = executeQuery("select name from sqlite_schema where type = 'table'")) {
					while (rs.next()) {
						String tblname = rs.getString(1);
						if (tblname != null) {
							tableList.add(tblname);
							if (tblname.equalsIgnoreCase(table)) {
								target = tblname;
							}
						}
					}
				}

				for (String tbl : tableList) {
					final ImportedKeyFinder impFkFinder = new ImportedKeyFinder(connection, tbl);
					List<ImportedKeyFinder.ForeignKey> fkNames = impFkFinder.getFkList();

					for (ImportedKeyFinder.ForeignKey foreignKey : fkNames) {
						String PKTabName = foreignKey.pkTableName;

						if (PKTabName == null || !PKTabName.equalsIgnoreCase(target)) {
							continue;
						}

						for (int j = 0; j < foreignKey.fkColNames.size(); j++) {
							int keySeq = j + 1;
							String pkColName = foreignKey.pkColNames.get(j);
							if (pkColName == null && pkColumns.length > j) {
								pkColName = pkColumns[j];
							}
							pkColName = (pkColName == null) ? "" : pkColName;
							String fkColName = foreignKey.fkColNames.get(j);
							fkColName = (fkColName == null) ? "" : fkColName;

							boolean usePkName = false;
							for (String pkColumn : pkColumns) {
								if (pkColumn != null && pkColumn.equalsIgnoreCase(pkColName)) {
									usePkName = true;
									break;
								}
							}
							String pkName = (usePkName && pkFinder.getName() != null) ? pkFinder.getName() : "";

							int ur = RULE_MAP.getOrDefault(foreignKey.onUpdate, DatabaseMetaData.importedKeyNoAction);
							int dr = RULE_MAP.getOrDefault(foreignKey.onDelete, DatabaseMetaData.importedKeyNoAction);

							exportedKeysQuery.append(count > 0 ? " union all select " : "select ").append(keySeq)
									.append(" as ks, ").append(HAUtils.quote(tbl)).append(" as fkt, ")
									.append(HAUtils.quote(fkColName)).append(" as fcn, ")
									.append(HAUtils.quote(pkColName)).append(" as pcn, ").append(HAUtils.quote(pkName))
									.append(" as pkn, ").append(ur).append(" as ur, ").append(dr).append(" as dr, ");

							String fkName = foreignKey.getFkName();
							if (fkName != null) {
								exportedKeysQuery.append(HAUtils.quote(fkName)).append(" as fkn");
							} else {
								exportedKeysQuery.append("'' as fkn");
							}

							count++;
						}
					}
				}
			}

			boolean hasImportedKey = (count > 0);
			StringBuilder sql = new StringBuilder(512);
			sql.append("select ").append(quotedCatalog).append(" as PKTABLE_CAT, ").append(quotedSchema)
					.append(" as PKTABLE_SCHEM, ").append(HAUtils.quote(target)).append(" as PKTABLE_NAME, ")
					.append(hasImportedKey ? "pcn" : "''").append(" as PKCOLUMN_NAME, ").append(quotedCatalog)
					.append(" as FKTABLE_CAT, ").append(quotedSchema).append(" as FKTABLE_SCHEM, ")
					.append(hasImportedKey ? "fkt" : "''").append(" as FKTABLE_NAME, ")
					.append(hasImportedKey ? "fcn" : "''").append(" as FKCOLUMN_NAME, ")
					.append(hasImportedKey ? "ks" : "-1").append(" as KEY_SEQ, ").append(hasImportedKey ? "ur" : "3")
					.append(" as UPDATE_RULE, ").append(hasImportedKey ? "dr" : "3").append(" as DELETE_RULE, ")
					.append(hasImportedKey ? "fkn" : "''").append(" as FK_NAME, ").append(hasImportedKey ? "pkn" : "''")
					.append(" as PK_NAME, ").append(DatabaseMetaData.importedKeyInitiallyDeferred)
					.append(" as DEFERRABILITY ");

			if (hasImportedKey) {
				sql.append("from (").append(exportedKeysQuery)
						.append(") ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ");
			} else {
				sql.append("limit 0");
			}
			return executeQuery(sql.toString());
		});
	}

	@Override
	public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
			String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
		if (parentTable == null) {
			return getExportedKeys(parentCatalog, parentSchema, null);
		}
		if (foreignTable == null) {
			return getImportedKeys(parentCatalog, parentSchema, parentTable);
		}
		return executeInCatalog(parentCatalog, () -> {
			String effectiveParentCat = resolveEffectiveCatalog(parentCatalog);
			String effectiveForeignCat = (foreignCatalog != null && !foreignCatalog.isEmpty()) ? foreignCatalog
					: effectiveParentCat;
			String query = "select " + HAUtils.quote(effectiveParentCat) + " as PKTABLE_CAT, "
					+ HAUtils.quote(parentSchema) + " as PKTABLE_SCHEM, " + HAUtils.quote(parentTable)
					+ " as PKTABLE_NAME, '' as PKCOLUMN_NAME, " + HAUtils.quote(effectiveForeignCat)
					+ " as FKTABLE_CAT, " + HAUtils.quote(foreignSchema) + " as FKTABLE_SCHEM, "
					+ HAUtils.quote(foreignTable)
					+ " as FKTABLE_NAME, '' as FKCOLUMN_NAME, -1 as KEY_SEQ, 3 as UPDATE_RULE, 3 as DELETE_RULE, '' as FK_NAME, '' as PK_NAME, "
					+ DatabaseMetaData.importedKeyInitiallyDeferred + " as DEFERRABILITY limit 0";
			return executeQuery(query);
		});
	}

	private static void verifySchemaParameters(String catalog, String schemaPattern) throws SQLException {
		if (CommonUtils.isEmpty(schemaPattern) || "%".equals(schemaPattern)
				|| "main".equalsIgnoreCase(schemaPattern)) {
			return;
		}
		throw new HAException("Schemas are not supported: " + schemaPattern);
	}

	private ResultSet executeQuery(String query) throws SQLException {
		return HAUtils.executeQuery(connection, query);
	}

	/**
	 * Parses the sqlite_schema table for a table's primary key.
	 */
	static class PrimaryKeyFinder {
		protected static final Pattern PK_UNNAMED_PATTERN = Pattern.compile(".*PRIMARY\\s+KEY\\s*\\((.*?)\\).*",
				Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

		protected static final Pattern PK_NAMED_PATTERN = Pattern.compile(
				".*CONSTRAINT\\s*(.*?)\\s*PRIMARY\\s+KEY\\s*\\((.*?)\\).*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

		String table;
		String pkName = null;
		String[] pkColumns = null;

		public PrimaryKeyFinder(Connection connection, String table) throws SQLException {
			this.table = table;

			if ("sqlite_schema".equals(table) || "sqlite_master".equals(table)) {
				return;
			}

			if (table == null || table.trim().isEmpty()) {
				throw new HAException("Invalid table name: '" + this.table + "'");
			}

			try (ResultSet rs = HAUtils.executeQuery(connection,
					"select sql from sqlite_schema where name like " + HAUtils.quote(table)
							+ " and type in ('table', 'view')")) {

				if (!rs.next()) {
					throw new HAException("Table not found: '" + table + "'");
				}

				String createSql = rs.getString(1);
				if (createSql != null) {
					Matcher matcher = PK_NAMED_PATTERN.matcher(createSql);
					if (matcher.find()) {
						pkName = HAUtils.unquote(matcher.group(1));
						pkColumns = matcher.group(2).split(",");
					} else {
						matcher = PK_UNNAMED_PATTERN.matcher(createSql);
						if (matcher.find()) {
							pkColumns = matcher.group(1).split(",");
						}
					}
				}

				if (pkColumns == null) {
					List<String> cols = new ArrayList<>();
					try (ResultSet rs2 = HAUtils.executeQuery(connection,
							"pragma table_info(" + HAUtils.quote(table) + ")")) {
						while (rs2.next()) {
							if (rs2.getInt(6) > 0 || rs2.getBoolean(6)) {
								cols.add(rs2.getString(2));
							}
						}
					}
					if (!cols.isEmpty()) {
						pkColumns = cols.toArray(new String[0]);
					}
				}

				if (pkColumns != null) {
					for (int i = 0; i < pkColumns.length; i++) {
						pkColumns[i] = HAUtils.unquote(pkColumns[i]);
					}
				}
			}
		}

		public String getName() {
			return pkName;
		}

		public String[] getColumns() {
			return pkColumns;
		}
	}

	/**
	 * Parses foreign key definitions from SQLite pragma and schema DDL.
	 */
	static class ImportedKeyFinder {

		private static final Pattern FK_NAMED_PATTERN = Pattern.compile(
				"CONSTRAINT\\s*\"?([A-Za-z_][A-Za-z\\d_]*)?\"?\\s*FOREIGN\\s+KEY\\s*\\((.*?)\\)",
				Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

		private final Connection conn;
		private final List<ForeignKey> fkList = new ArrayList<>();

		public ImportedKeyFinder(Connection connection, String table) throws SQLException {
			this.conn = connection;
			if (table == null || table.trim().isEmpty()) {
				throw new HAException("Invalid table name: '" + table + "'");
			}

			List<String> fkNames = getForeignKeyNames(table);

			try (ResultSet rs = HAUtils.executeQuery(connection,
					"pragma foreign_key_list(" + HAUtils.quote(table) + ")")) {

				int prevFkId = -1;
				int count = 0;
				ForeignKey fk = null;
				while (rs.next()) {
					int fkId = rs.getInt(1);
					String pkTableName = rs.getString(3);
					String fkColName = rs.getString(4);
					String pkColName = rs.getString(5);
					String onUpdate = rs.getString(6);
					String onDelete = rs.getString(7);
					String match = rs.getString(8);

					String fkName = null;
					if (fkNames.size() > count) {
						fkName = fkNames.get(count);
					}

					if (fkId != prevFkId) {
						fk = new ForeignKey(fkName, pkTableName, table, onUpdate, onDelete, match);
						fkList.add(fk);
						prevFkId = fkId;
						count++;
					}
					if (fk != null) {
						fk.addColumnMapping(fkColName, pkColName);
					}
				}
			}
		}

		private List<String> getForeignKeyNames(String tbl) throws SQLException {
			List<String> fkNames = new ArrayList<>();
			if (tbl == null) {
				return fkNames;
			}
			try (ResultSet rs = HAUtils.executeQuery(conn,
					"select sql from sqlite_schema where name like " + HAUtils.quote(tbl))) {

				if (rs.next()) {
					String createSql = rs.getString(1);
					if (createSql != null) {
						Matcher matcher = FK_NAMED_PATTERN.matcher(createSql);
						while (matcher.find()) {
							fkNames.add(matcher.group(1));
						}
					}
				}
			}
			Collections.reverse(fkNames);
			return fkNames;
		}

		public List<ForeignKey> getFkList() {
			return fkList;
		}

		static class ForeignKey {

			private final String fkName;
			private final String pkTableName;
			private final String fkTableName;
			private final List<String> fkColNames = new ArrayList<>();
			private final List<String> pkColNames = new ArrayList<>();
			private final String onUpdate;
			private final String onDelete;
			private final String match;

			ForeignKey(String fkName, String pkTableName, String fkTableName, String onUpdate, String onDelete,
					String match) {
				this.fkName = fkName;
				this.pkTableName = pkTableName;
				this.fkTableName = fkTableName;
				this.onUpdate = onUpdate;
				this.onDelete = onDelete;
				this.match = match;
			}

			public String getFkName() {
				return fkName;
			}

			public String getMatch() {
				return match;
			}

			void addColumnMapping(String fkColName, String pkColName) {
				fkColNames.add(fkColName);
				pkColNames.add(pkColName);
			}

			@Override
			public String toString() {
				return "ForeignKey [fkName=" + fkName + ", pkTableName=" + pkTableName + ", fkTableName=" + fkTableName
						+ ", pkColNames=" + pkColNames + ", fkColNames=" + fkColNames + "]";
			}
		}
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		return true;
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		HAStatement stmt = new HAStatement(connection, HAConstants.DEFAULT_TIMEOUT);
		try {
			stmt.closeOnCompletion();
			return new HAResultSet(stmt, connection.getClient().getReplicationIDsResultSet());
		} catch (Exception e) {
			stmt.close();
			if (e instanceof SQLException) {
				throw (SQLException) e;
			}
			throw new HAException("Failed to fetch catalogs", e);
		}
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		return false;
	}

}
