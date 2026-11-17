package com.github.litesql.jdbc.driver.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.litesql.jdbc.ha.HAConnection;
import com.github.litesql.jdbc.ha.HADataSource;
import com.github.litesql.jdbc.ha.HADriver;
import com.github.litesql.jdbc.ha.HAUtils;
import com.github.litesql.jdbc.ha.client.HAClient;

public class HADriverTest {

	@TempDir
	Path tempDir;

	private MockHAServer server;
	private String jdbcUrl;

	@BeforeEach
	void setUp() throws Exception {
		Class.forName(HADriver.class.getName());
		server = new MockHAServer(tempDir.resolve("server-dbs"));
		jdbcUrl = "jdbc:litesql:ha:http://127.0.0.1:" + server.getPort() + "/test.db";
	}

	@AfterEach
	void tearDown() {
		if (server != null) {
			server.close();
		}
	}

	@Test
	void testDriverCoexistenceAndUrlParsing() throws Exception {
		HADriver driver = new HADriver();
		assertFalse(driver.acceptsURL(null));
		assertFalse(driver.acceptsURL(""));
		assertFalse(driver.acceptsURL("jdbc:sqlite::memory:"));
		assertFalse(driver.acceptsURL("jdbc:postgresql://localhost:5432/mydb"));

		assertNull(driver.connect(null, null));
		assertNull(driver.connect("jdbc:sqlite::memory:", null));
		assertNull(driver.connect("jdbc:postgresql://localhost:5432/mydb", null));

		// Ensure standard SQLite DriverManager connections still work alongside HADriver
		try (Connection sqliteConn = DriverManager.getConnection("jdbc:sqlite::memory:");
				Statement st = sqliteConn.createStatement();
				ResultSet rs = st.executeQuery("SELECT 42")) {
			assertTrue(rs.next());
			assertEquals(42, rs.getInt(1));
		}

		assertTrue(driver.acceptsURL("litesql://localhost:8080/db"));
		assertTrue(driver.acceptsURL("litesqls://localhost:8443/db"));
		assertTrue(driver.acceptsURL("jdbc:litesql:ha:http://localhost:8080/db"));
		assertTrue(driver.acceptsURL("https://example.com/db"));

		assertEquals("http://localhost:8080/db", HAUtils.validateAndFormatUrl("litesql://localhost:8080/db"));
		assertEquals("https://localhost:8443/db", HAUtils.validateAndFormatUrl("litesqls://localhost:8443/db"));
		assertEquals("https://example.com/db", HAUtils.validateAndFormatUrl("https://example.com/db"));

		DriverPropertyInfo[] propInfos = driver.getPropertyInfo("litesql://localhost:8080/db?timeout=15", null);
		assertTrue(propInfos.length >= 7);
	}

	@Test
	void testUtilsEscapingQuotingAndSafePaths() throws Exception {
		// Regression test for indexOf('\'') == 1 bug
		assertEquals("O''Reilly", HAUtils.escape("O'Reilly"));
		assertEquals("a''b''c", HAUtils.escape("a'b'c"));
		assertEquals("normal", HAUtils.escape("normal"));
		assertNull(HAUtils.escape(null));

		assertEquals("NULL", HAUtils.quote(null));
		assertEquals("'O''Reilly'", HAUtils.quote("O'Reilly"));

		assertTrue(HAUtils.isSelectQuery("/* leading comment */\n-- line comment\nSELECT * FROM t"));
		assertTrue(HAUtils.isSelectQuery("WITH cte AS (SELECT 1) SELECT * FROM cte"));
		assertFalse(HAUtils.isSelectQuery("WITH cte AS (SELECT 1) DELETE FROM t WHERE id IN (SELECT * FROM cte)"));

		assertThrows(IOException.class, () -> HAUtils.resolveSafePath(tempDir.toString(), "../outside.db"));

		// Verify writeFileChunk truncates larger existing file when offset == 0
		Path targetFile = tempDir.resolve("chunked.db");
		Files.write(targetFile, new byte[500]);
		HAUtils.writeFileChunk(tempDir.toString(), "chunked.db", new byte[] { 1, 2, 3, 4 }, 0);
		assertEquals(4L, Files.size(targetFile));
	}

	@Test
	void testDataSourceAndAuthentication() throws Exception {
		HADataSource ds = new HADataSource();
		ds.setUrl("litesql://127.0.0.1:" + server.getPort() + "/test.db");
		ds.setPassword("secret-token-123");
		ds.setTimeout(30);

		assertTrue(ds.isWrapperFor(DataSource.class));
		assertTrue(ds.isWrapperFor(HADataSource.class));
		assertFalse(ds.isWrapperFor(String.class));
		assertNotNull(ds.unwrap(DataSource.class));
		assertThrows(SQLException.class, () -> ds.unwrap(String.class));
		assertNotNull(ds.getParentLogger());

		try (Connection conn = ds.getConnection();
				Statement st = conn.createStatement();
				ResultSet rs = st.executeQuery("SELECT 1")) {
			assertTrue(rs.next());
			assertEquals(1, rs.getInt(1));
			assertEquals("secret-token-123", server.getLastAuthToken());
		}

		// Null password handling (previously threw NullPointerException on Hashtable)
		ds.setPassword(null);
		try (Connection conn = ds.getConnection("user", null)) {
			assertTrue(conn.isValid(5));
		}
	}

	@Test
	void testStatementReuseExecuteContractGeneratedKeysAndBatch() throws Exception {
		try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
			try (Statement st = conn.createStatement()) {
				// DDL via execute() must return false and null ResultSet
				assertFalse(st.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, qty INT)"));
				assertNull(st.getResultSet());
			}

			// PreparedStatement.executeUpdate() + getGeneratedKeys()
			try (PreparedStatement ps = conn.prepareStatement("INSERT INTO items (name, qty) VALUES (?, ?)")) {
				ps.setString(1, "apple");
				ps.setInt(2, 10);
				assertEquals(1, ps.executeUpdate());
				try (ResultSet keys = ps.getGeneratedKeys()) {
					assertTrue(keys.next());
					assertEquals(1L, keys.getLong(1));
				}

				ps.setString(1, "banana");
				ps.setInt(2, 20);
				assertEquals(1, ps.executeUpdate());
				try (ResultSet keys = ps.getGeneratedKeys()) {
					assertTrue(keys.next());
					assertEquals(2L, keys.getLong(1));
				}
			}

			// PreparedStatement reuse across multiple executeQuery() calls
			try (PreparedStatement ps = conn.prepareStatement("SELECT name, qty FROM items WHERE id = ?")) {
				ps.setInt(1, 1);
				try (ResultSet rs1 = ps.executeQuery()) {
					assertTrue(rs1.next());
					assertEquals("apple", rs1.getString("name"));
					assertEquals(10, rs1.getInt("qty"));
					assertFalse(rs1.next());
				}

				ps.setInt(1, 2);
				try (ResultSet rs2 = ps.executeQuery()) {
					assertTrue(rs2.next());
					assertEquals("banana", rs2.getString("name"));
					assertEquals(20, rs2.getInt("qty"));
					assertFalse(rs2.next());
				}
			}

			// Statement.execute() for SELECT vs UPDATE
			try (Statement st = conn.createStatement()) {
				assertTrue(st.execute("SELECT COUNT(*) FROM items"));
				assertEquals(-1, st.getUpdateCount());
				try (ResultSet rs = st.getResultSet()) {
					assertNotNull(rs);
					assertTrue(rs.next());
					assertEquals(2, rs.getInt(1));
				}

				assertFalse(st.execute("UPDATE items SET qty = qty + 5"));
				assertEquals(2, st.getUpdateCount());
				assertNull(st.getResultSet());
			}

			// PreparedStatement batching
			try (PreparedStatement ps = conn.prepareStatement("INSERT INTO items (name, qty) VALUES (?, ?)")) {
				ps.setString(1, "cherry");
				ps.setInt(2, 30);
				ps.addBatch();

				ps.setString(1, "date");
				ps.setInt(2, 40);
				ps.addBatch();

				int[] counts = ps.executeBatch();
				assertArrayEquals(new int[] { 1, 1 }, counts);
			}

			// Statement batching
			try (Statement st = conn.createStatement()) {
				st.addBatch("UPDATE items SET qty = 99 WHERE name = 'cherry'");
				st.addBatch("DELETE FROM items WHERE name = 'date'");
				int[] counts = st.executeBatch();
				assertArrayEquals(new int[] { 1, 1 }, counts);
			}
		}
	}

	@Test
	void testAllDataTypesNullsBlobsStreamsAndTimestamps() throws Exception {
		try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
			try (Statement st = conn.createStatement()) {
				st.execute("CREATE TABLE types_test ("
						+ "id INT, str_col TEXT, blob_col BLOB, short_col INT, byte_col INT, "
						+ "bd_col REAL, bool_col BOOLEAN, ts_col TEXT, ascii_col TEXT, reader_col TEXT)");
			}

			byte[] binaryPayload = new byte[] { 0x00, 0x01, 0x7F, (byte) 0x80, (byte) 0xFF };
			Timestamp sampleTs = Timestamp.from(Instant.parse("2026-11-17T10:15:30.123Z"));

			try (PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO types_test VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
				// Row 1: NULLs
				ps.setInt(1, 1);
				ps.setNull(2, Types.VARCHAR);
				ps.setNull(3, Types.BLOB);
				ps.setNull(4, Types.SMALLINT);
				ps.setNull(5, Types.TINYINT);
				ps.setNull(6, Types.DECIMAL);
				ps.setNull(7, Types.BOOLEAN);
				ps.setNull(8, Types.TIMESTAMP);
				ps.setNull(9, Types.VARCHAR);
				ps.setNull(10, Types.VARCHAR);
				assertEquals(1, ps.executeUpdate());

				// Row 2: Rich types & bounded streams
				ps.setInt(1, 2);
				ps.setString(2, "hello");
				ps.setBinaryStream(3, new ByteArrayInputStream(new byte[] { 0x00, 0x01, 0x7F, (byte) 0x80, (byte) 0xFF, 0x55, 0x66 }), 5);
				ps.setShort(4, (short) 32000);
				ps.setByte(5, (byte) 42);
				ps.setBigDecimal(6, new BigDecimal("123.45"));
				ps.setBoolean(7, true);
				ps.setTimestamp(8, sampleTs);
				ps.setAsciiStream(9, new ByteArrayInputStream("abcdefg".getBytes(StandardCharsets.US_ASCII)), 4);
				ps.setCharacterStream(10, new StringReader("xyz12345"), 3);
				assertEquals(1, ps.executeUpdate());
			}

			try (Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery("SELECT * FROM types_test ORDER BY id")) {
				// Metadata scans past NULLs in row 1 to infer types from row 2
				ResultSetMetaData rsmd = rs.getMetaData();
				assertEquals(Types.BLOB, rsmd.getColumnType(3));
				assertEquals(0, rsmd.getPrecision(1));
				assertEquals(0, rsmd.getScale(1));

				// Row 1: verify SQL NULL returns null for getString (not empty string!)
				assertTrue(rs.next());
				assertEquals(1, rs.getInt("id"));
				assertFalse(rs.wasNull());

				assertNull(rs.getString("str_col"));
				assertTrue(rs.wasNull());
				assertNull(rs.getBytes("blob_col"));
				assertNull(rs.getBigDecimal("bd_col"));
				assertNull(rs.getTimestamp("ts_col"));
				assertNull(rs.getDate("ts_col"));
				assertNull(rs.getTime("ts_col"));

				// Row 2: verify actual values
				assertTrue(rs.next());
				assertEquals("hello", rs.getString("str_col"));
				assertArrayEquals(binaryPayload, rs.getBytes("blob_col"));
				try (InputStream binStream = rs.getBinaryStream("blob_col")) {
					assertArrayEquals(binaryPayload, binStream.readAllBytes());
				}
				assertEquals((short) 32000, rs.getShort("short_col"));
				assertEquals((byte) 42, rs.getByte("byte_col"));
				assertEquals(new BigDecimal("123.5"), rs.getBigDecimal("bd_col", 1));
				assertTrue(rs.getBoolean("bool_col"));
				assertEquals(sampleTs.toInstant(), rs.getTimestamp("ts_col").toInstant());
				assertEquals(LocalDate.of(2026, 11, 17), rs.getObject("ts_col", LocalDate.class));
				assertEquals("abcd", rs.getString("ascii_col"));
				assertEquals("xyz", rs.getString("reader_col"));
			}

			// Protobuf google.protobuf.Timestamp (Instant) -> getTimestamp / getDate / getTime
			try (Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery("SELECT PROTO_TIMESTAMP_TEST")) {
				assertTrue(rs.next());
				Timestamp ts = rs.getTimestamp(1);
				assertEquals(Instant.parse("2026-11-17T12:34:56.789Z"), ts.toInstant());
				Date d = rs.getDate(1);
				assertNotNull(d);
				Time t = rs.getTime(1);
				assertNotNull(t);
			}

			// Duplicate column names: findColumn must return the FIRST column index (1)
			try (Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery("SELECT 111 AS dup_col, 222 AS dup_col")) {
				assertEquals(1, rs.findColumn("dup_col"));
				assertTrue(rs.next());
				assertEquals(111, rs.getInt("dup_col"));
			}
		}
	}

	@Test
	void testTransactionsReadOnlyAndPoolRequestLifecycle() throws Exception {
		try (HAConnection conn = (HAConnection) DriverManager.getConnection(jdbcUrl)) {
			try (Statement st = conn.createStatement()) {
				st.execute("CREATE TABLE tx_test (id INT PRIMARY KEY, v TEXT)");
			}

			// Read-only flag (Long 1L vs Integer 1 regression test)
			assertFalse(conn.isReadOnly());
			conn.setReadOnly(true);
			assertTrue(conn.isReadOnly());
			conn.setReadOnly(false);
			assertFalse(conn.isReadOnly());

			// Explicit transaction rollback & commit
			conn.setAutoCommit(false);
			try (Statement st = conn.createStatement()) {
				st.executeUpdate("INSERT INTO tx_test VALUES (1, 'rolled_back')");
			}
			conn.rollback();

			try (Statement st = conn.createStatement()) {
				st.executeUpdate("INSERT INTO tx_test VALUES (2, 'committed')");
			}
			conn.commit();
			conn.setAutoCommit(true);

			try (Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery("SELECT id, v FROM tx_test")) {
				assertTrue(rs.next());
				assertEquals(2, rs.getInt(1));
				assertEquals("committed", rs.getString(2));
				assertFalse(rs.next());
			}

			// Pool lifecycle: beginRequest() + uncommitted work + endRequest() must ROLLBACK, not COMMIT
			conn.beginRequest();
			assertTrue(conn.getAutoCommit());
			conn.setAutoCommit(false);
			try (Statement st = conn.createStatement()) {
				st.executeUpdate("INSERT INTO tx_test VALUES (3, 'leaked_uncommitted')");
			}
			conn.endRequest();
			assertTrue(conn.getAutoCommit());

			try (Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM tx_test WHERE id = 3")) {
				assertTrue(rs.next());
				assertEquals(0, rs.getInt(1));
			}
		}
	}

	@Test
	void testQueryTimeoutAndStreamRecovery() throws Exception {
		server.setArtificialDelayMs(2500L);
		try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
			try (Statement st = conn.createStatement()) {
				st.setQueryTimeout(1);
				assertThrows(SQLTimeoutException.class, () -> st.executeQuery("SELECT 'SLOW_QUERY'"));
			}

			// Subsequent fast query on the SAME connection must recover and return its own result
			server.setArtificialDelayMs(0L);
			try (Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery("SELECT 'RECOVERED_OK'")) {
				assertTrue(rs.next());
				assertEquals("RECOVERED_OK", rs.getString(1));
			}
		}
	}

	@Test
	void testDatabaseMetaDataCatalogsDownloadsAndEscaping() throws Exception {
		try (HAConnection conn = (HAConnection) DriverManager.getConnection(jdbcUrl)) {
			try (Statement st = conn.createStatement()) {
				st.execute("CREATE TABLE parent_tbl (id INTEGER PRIMARY KEY, code TEXT UNIQUE NOT NULL)");
				st.execute("CREATE TABLE child_tbl (id INTEGER PRIMARY KEY, parent_id INT DEFAULT 1, "
						+ "CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent_tbl(id) ON DELETE CASCADE)");
				st.execute("CREATE INDEX idx_child_parent ON child_tbl(parent_id)");
				st.execute("CREATE VIEW v_parent AS SELECT id, code FROM parent_tbl");
				st.execute("CREATE TABLE \"O'Reilly\" (id INTEGER PRIMARY KEY, title TEXT)");
			}

			DatabaseMetaData meta = conn.getMetaData();
			assertEquals("SQLite HA", meta.getDatabaseProductName());
			assertTrue(meta.getDatabaseProductVersion().startsWith("3."));
			assertEquals(3, meta.getDatabaseMajorVersion());
			assertTrue(meta.getDatabaseMinorVersion() > 0);

			// getCatalogs() & getReplicationIDs() (previously threw NPE/CANCELLED on null request)
			Set<String> catalogs = new HashSet<>();
			try (ResultSet rs = meta.getCatalogs()) {
				while (rs.next()) {
					catalogs.add(rs.getString("TABLE_CAT"));
				}
			}
			assertTrue(catalogs.containsAll(Arrays.asList("test.db", "other.db")));

			// Querying metadata of another catalog must NOT mutate active connection catalog
			assertEquals("test.db", conn.getCatalog());
			try (ResultSet rs = meta.getTables("other.db", "%", "%", new String[] { "TABLE" })) {
				assertFalse(rs.next());
			}
			assertEquals("test.db", conn.getCatalog());

			// Views & tableNamePattern filtering
			List<String> views = new ArrayList<>();
			try (ResultSet rs = meta.getTables(null, "%", "v_%", new String[] { "VIEW" })) {
				while (rs.next()) {
					views.add(rs.getString("TABLE_NAME"));
					assertEquals("VIEW", rs.getString("TABLE_TYPE"));
				}
			}
			assertEquals(List.of("v_parent"), views);

			// getColumns: 24 JDBC columns, NUM_PREC_RADIX, 1-based ORDINAL_POSITION, SQLite type affinity
			try (ResultSet rs = meta.getColumns(null, "%", "child_tbl", "%")) {
				assertEquals(24, rs.getMetaData().getColumnCount());
				assertTrue(rs.next());
				assertEquals("id", rs.getString("COLUMN_NAME"));
				assertEquals(Types.INTEGER, rs.getInt("DATA_TYPE"));
				assertEquals(10, rs.getInt("NUM_PREC_RADIX"));
				assertEquals(1, rs.getInt("ORDINAL_POSITION"));
				assertEquals("NO", rs.getString("IS_NULLABLE"));

				assertTrue(rs.next());
				assertEquals("parent_id", rs.getString("COLUMN_NAME"));
				assertEquals(Types.INTEGER, rs.getInt("DATA_TYPE"));
				assertEquals(2, rs.getInt("ORDINAL_POSITION"));
				assertEquals("1", rs.getString("COLUMN_DEF"));
				assertEquals("YES", rs.getString("IS_NULLABLE"));
				assertFalse(rs.next());
			}

			// Escaping table names with apostrophe at index 1 ("O'Reilly")
			try (ResultSet pkRs = meta.getPrimaryKeys(null, null, "O'Reilly")) {
				assertTrue(pkRs.next());
				assertEquals("id", pkRs.getString("COLUMN_NAME"));
			}

			// getIndexInfo respecting unique = true vs unique = false
			List<String> uniqueIndexes = new ArrayList<>();
			try (ResultSet idxRs = meta.getIndexInfo(null, null, "child_tbl", true, false)) {
				while (idxRs.next()) {
					uniqueIndexes.add(idxRs.getString("INDEX_NAME"));
				}
			}
			assertTrue(uniqueIndexes.isEmpty());

			List<String> allIndexes = new ArrayList<>();
			try (ResultSet idxRs = meta.getIndexInfo(null, null, "child_tbl", false, false)) {
				while (idxRs.next()) {
					allIndexes.add(idxRs.getString("INDEX_NAME"));
				}
			}
			assertEquals(List.of("idx_child_parent"), allIndexes);

			// Foreign keys (imported & exported)
			try (ResultSet fkRs = meta.getImportedKeys(null, null, "child_tbl")) {
				assertTrue(fkRs.next());
				assertEquals("parent_tbl", fkRs.getString("PKTABLE_NAME"));
				assertEquals("id", fkRs.getString("PKCOLUMN_NAME"));
				assertEquals("parent_id", fkRs.getString("FKCOLUMN_NAME"));
				assertEquals(DatabaseMetaData.importedKeyCascade, fkRs.getInt("DELETE_RULE"));
			}
			try (ResultSet ekRs = meta.getExportedKeys(null, null, "parent_tbl")) {
				assertTrue(ekRs.next());
				assertEquals("child_tbl", ekRs.getString("FKTABLE_NAME"));
			}

			// Atomic replica & snapshot downloads (truncating larger pre-existing files)
			Path downloadDir = tempDir.resolve("downloads");
			Files.createDirectories(downloadDir);
			Path existingLarge = downloadDir.resolve("test.db");
			byte[] hugeDummy = new byte[200_000];
			Arrays.fill(hugeDummy, (byte) 'X');
			Files.write(existingLarge, hugeDummy);

			HAClient client = conn.getClient();
			client.downloadAllCurrentReplicas(downloadDir.toString(), true);
			assertTrue(Files.size(existingLarge) < hugeDummy.length);

			Path snapDir = tempDir.resolve("snapshots");
			client.downloadAllLatestSnapshots(snapDir.toString(), true);
			assertTrue(Files.exists(snapDir.resolve("test.db")));
			assertTrue(Files.exists(snapDir.resolve("other.db")));
		}
	}

	/**
	 * Manual CLI entry point preserved for ad-hoc testing against an external HA server.
	 */
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		String serverUrl = args.length > 0 ? args[0] : "http://localhost:5001/chinook.db";
		try (Connection connection = DriverManager.getConnection("jdbc:litesql:ha:" + serverUrl, null, null)) {
			DatabaseMetaData metaData = connection.getMetaData();
			System.out.println("Driver: " + metaData.getDriverName());
			System.out.println(
					"Database: " + metaData.getDatabaseProductName() + " " + metaData.getDatabaseProductVersion());
			try (ResultSet tables = metaData.getTables(null, null, null, null)) {
				while (tables.next()) {
					String tableName = tables.getString("TABLE_NAME");
					System.out.println("\t- " + tableName);
				}
			}
		} finally {
			System.out.println("Finished (" + (System.currentTimeMillis() - startTime) + "ms)");
		}
	}
}
