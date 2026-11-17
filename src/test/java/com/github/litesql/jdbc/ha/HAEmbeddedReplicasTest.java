package com.github.litesql.jdbc.ha;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.litesql.jdbc.driver.test.MockHAServer;

public class HAEmbeddedReplicasTest {

	@TempDir
	Path tempDir;

	private MockHAServer server;

	@BeforeEach
	void setUp() throws Exception {
		Class.forName(HADriver.class.getName());
		server = new MockHAServer(tempDir.resolve("remote-dbs"));
	}

	@AfterEach
	void tearDown() {
		HAEmbeddedReplicasManager.unregisterReplica("replica.db");
		if (server != null) {
			server.close();
		}
	}

	@Test
	void testEmbeddedReplicaPreparedStatementParameterBindingAndTransactionBypass() throws Exception {
		// Seed remote DB with value 'FROM_REMOTE'
		Path remoteDb = server.registerDatabase("replica.db");
		try (Connection rc = DriverManager.getConnection("jdbc:sqlite:" + remoteDb.toAbsolutePath());
				Statement st = rc.createStatement()) {
			st.execute("CREATE TABLE users (id INT PRIMARY KEY, source TEXT)");
			st.execute("INSERT INTO users VALUES (1, 'FROM_REMOTE'), (2, 'OTHER_REMOTE')");
		}

		// Create local embedded replica SQLite file with value 'FROM_LOCAL_REPLICA' and high txseq
		Path localReplicaPath = tempDir.resolve("local-replica.db");
		String localDsn = "jdbc:sqlite:" + localReplicaPath.toAbsolutePath();
		try (Connection lc = DriverManager.getConnection(localDsn);
				Statement st = lc.createStatement()) {
			st.execute("PRAGMA journal_mode=WAL");
			st.execute("CREATE TABLE users (id INT PRIMARY KEY, source TEXT)");
			st.execute("INSERT INTO users VALUES (1, 'FROM_LOCAL_REPLICA'), (2, 'OTHER_LOCAL')");
		}

		HAEmbeddedReplicasManager.registerReplica("replica.db",
				new HAEmbeddedReplicasManager.ReplicaConn(localDsn, null, 1_000L));

		String url = "jdbc:litesql:ha:http://127.0.0.1:" + server.getPort() + "/replica.db";
		try (Connection conn = DriverManager.getConnection(url)) {
			// 1. In autoCommit=true, PreparedStatement SELECT with '?' MUST route to local replica AND bind '?'
			try (PreparedStatement ps = conn.prepareStatement("SELECT source FROM users WHERE id = ?")) {
				ps.setInt(1, 1);
				try (ResultSet rs = ps.executeQuery()) {
					assertTrue(rs.next());
					assertEquals("FROM_LOCAL_REPLICA", rs.getString(1));
					assertFalse(rs.next());
				}

				ps.setInt(1, 2);
				try (ResultSet rs = ps.executeQuery()) {
					assertTrue(rs.next());
					assertEquals("OTHER_LOCAL", rs.getString(1));
					assertFalse(rs.next());
				}
			}

			// 2. Inside a transaction (autoCommit=false), SELECT MUST bypass replica and read from primary server
			conn.setAutoCommit(false);
			try (PreparedStatement ps = conn.prepareStatement("SELECT source FROM users WHERE id = ?")) {
				ps.setInt(1, 1);
				try (ResultSet rs = ps.executeQuery()) {
					assertTrue(rs.next());
					assertEquals("FROM_REMOTE", rs.getString(1));
				}
			}
			conn.rollback();
			conn.setAutoCommit(true);
		}
	}
}
