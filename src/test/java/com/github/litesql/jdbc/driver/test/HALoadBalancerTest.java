package com.github.litesql.jdbc.driver.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.litesql.jdbc.ha.HAConnection;
import com.github.litesql.jdbc.ha.HAConstants;
import com.github.litesql.jdbc.ha.HADataSource;
import com.github.litesql.jdbc.ha.HADriver;
import com.github.litesql.jdbc.ha.HAException;
import com.github.litesql.jdbc.ha.HAUtils;
import com.github.litesql.jdbc.ha.client.HAConnectionUrl;

public class HALoadBalancerTest {

	@TempDir
	Path tempDir;

	private final List<MockHAServer> servers = new ArrayList<>();

	@BeforeEach
	void setUp() throws Exception {
		Class.forName(HADriver.class.getName());
	}

	@AfterEach
	void tearDown() {
		for (MockHAServer s : servers) {
			s.close();
		}
		servers.clear();
	}

	private MockHAServer startNode(String nodeName) throws Exception {
		MockHAServer server = new MockHAServer(tempDir.resolve(nodeName));
		servers.add(server);
		Path dbPath = server.registerDatabase("cluster.db");
		try (Connection sqlite = DriverManager.getConnection("jdbc:sqlite:" + dbPath.toAbsolutePath());
				Statement st = sqlite.createStatement()) {
			st.execute("CREATE TABLE node_info (node_name TEXT)");
			st.execute("INSERT INTO node_info VALUES ('" + nodeName + "')");
		}
		return server;
	}

	private static String queryNodeName(Connection conn) throws SQLException {
		try (Statement st = conn.createStatement();
				ResultSet rs = st.executeQuery("SELECT node_name FROM node_info")) {
			assertTrue(rs.next());
			return rs.getString(1);
		}
	}

	@Test
	void testUrlParsingMultiHostDnsAndIpv6() throws Exception {
		HADriver driver = new HADriver();

		assertTrue(driver.acceptsURL("litesql://node1:8080,node2:8080/mydb"));
		assertTrue(driver.acceptsURL("litesqls://node1:8443,node2:8443/mydb"));
		assertTrue(driver.acceptsURL("jdbc:litesql:ha:http://[::1]:5001,[::1]:5002/mydb"));
		assertTrue(driver.acceptsURL("dns:///my-service:8080/mydb"));
		assertTrue(driver.acceptsURL("jdbc:litesql:ha:dns:///my-service:8080/mydb"));

		assertEquals("http://node1:8080,node2:8081,node3/mydb?loadBalancerPolicy=round_robin",
				HAUtils.validateAndFormatUrl(
						"litesql://node1:8080,node2:8081,node3/mydb?loadBalancerPolicy=round_robin"));

		HAConnectionUrl multi = HAConnectionUrl
				.parse("litesql://node1:8080,node2:8081,node3/mydb?loadBalancerPolicy=pick_first");
		assertEquals(3, multi.getEndpoints().size());
		assertEquals("node1:8080", multi.getEndpoints().get(0).toHostPort());
		assertEquals("node2:8081", multi.getEndpoints().get(1).toHostPort());
		assertEquals("node3:80", multi.getEndpoints().get(2).toHostPort());
		assertEquals("mydb", multi.getReplicationId());
		assertEquals("pick_first", multi.getQueryParams().get("loadBalancerPolicy"));
		assertEquals("litesql-cluster:///node1:8080,node2:8081,node3:80", multi.toGrpcTarget());

		HAConnectionUrl ipv6 = HAConnectionUrl.parse("jdbc:litesql:ha:https://[::1]:8443,[::1]/secure.db");
		assertEquals(2, ipv6.getEndpoints().size());
		assertEquals("[::1]:8443", ipv6.getEndpoints().get(0).toHostPort());
		assertEquals("[::1]:443", ipv6.getEndpoints().get(1).toHostPort());
		assertEquals("litesql-cluster:///%5B::1%5D:8443,%5B::1%5D:443", ipv6.toGrpcTarget());

		HAConnectionUrl dnsCustomAuth = HAConnectionUrl
				.parse("jdbc:litesql:ha:dns://8.8.8.8:53/my-svc:9000/app.db?disableSSL=true");
		assertEquals("dns", dnsCustomAuth.getScheme());
		assertEquals("8.8.8.8:53", dnsCustomAuth.getDnsAuthority());
		assertEquals("app.db", dnsCustomAuth.getReplicationId());
		assertEquals("dns://8.8.8.8:53/my-svc:9000", dnsCustomAuth.toGrpcTarget());

		DriverPropertyInfo[] propInfos = driver
				.getPropertyInfo("litesql://node1:8080,node2:8080/db?loadBalancerPolicy=pick_first", null);
		boolean foundLbProp = false;
		for (DriverPropertyInfo dpi : propInfos) {
			if (HAConstants.CONNECTION_PROPERTY_LOAD_BALANCER_POLICY.equals(dpi.name)) {
				foundLbProp = true;
				assertEquals("pick_first", dpi.value);
			}
		}
		assertTrue(foundLbProp);

		// Invalid URLs and invalid loadBalancerPolicy must throw HAException (SQLException)
		assertThrows(HAException.class, () -> HAUtils.validateAndFormatUrl("litesql://host1:8080,/db"));
		assertThrows(HAException.class, () -> HAUtils.validateAndFormatUrl("litesql://,host2:8080/db"));
		assertThrows(HAException.class, () -> HAUtils.validateAndFormatUrl("litesql://host1:99999/db"));
		assertThrows(HAException.class, () -> HAUtils.validateAndFormatUrl("litesql://::1:8080/db"));
		assertThrows(SQLException.class, () -> DriverManager
				.getConnection("litesql://127.0.0.1:8080/db?loadBalancerPolicy=invalid_policy_xyz"));
	}

	@Test
	void testRoundRobinLoadBalancingAndSharedChannel() throws Exception {
		MockHAServer s1 = startNode("node-1");
		MockHAServer s2 = startNode("node-2");
		MockHAServer s3 = startNode("node-3");

		String clusterUrl = String.format("litesql://127.0.0.1:%d,127.0.0.1:%d,127.0.0.1:%d/cluster.db", s1.getPort(),
				s2.getPort(), s3.getPort());

		HADataSource ds = new HADataSource();
		ds.setUrl(clusterUrl);
		assertEquals("round_robin", ds.getLoadBalancerPolicy());

		List<HAConnection> openConns = new ArrayList<>();
		Map<String, Integer> distribution = new HashMap<>();
		try {
			for (int i = 0; i < 6; i++) {
				HAConnection conn = (HAConnection) ds.getConnection();
				openConns.add(conn);
			}

			// Verify all connections to the same cluster share the underlying ManagedChannel
			for (int i = 1; i < openConns.size(); i++) {
				assertSame(openConns.get(0).getClient().getChannel(), openConns.get(i).getClient().getChannel());
			}

			for (HAConnection conn : openConns) {
				String firstNode = queryNodeName(conn);
				// Session stream must remain sticky to the same server across multiple queries on the same Connection
				String secondNode = queryNodeName(conn);
				assertEquals(firstNode, secondNode);
				distribution.merge(firstNode, 1, Integer::sum);
			}
		} finally {
			for (HAConnection c : openConns) {
				c.close();
			}
		}

		// Every server in the 3-node cluster must receive 2 of the 6 connections
		assertEquals(Set.of("node-1", "node-2", "node-3"), distribution.keySet());
		assertEquals(2, distribution.get("node-1"));
		assertEquals(2, distribution.get("node-2"));
		assertEquals(2, distribution.get("node-3"));
	}

	@Test
	void testPickFirstPolicyAndFailover() throws Exception {
		MockHAServer s1 = startNode("node-1");
		MockHAServer s2 = startNode("node-2");

		HADataSource ds = new HADataSource();
		ds.setUrl(String.format("litesql://127.0.0.1:%d,127.0.0.1:%d/cluster.db", s1.getPort(), s2.getPort()));
		ds.setLoadBalancerPolicy("pick_first");
		assertEquals("pick_first", ds.getLoadBalancerPolicy());

		try (Connection c1 = ds.getConnection(); Connection c2 = ds.getConnection()) {
			assertEquals("node-1", queryNodeName(c1));
			assertEquals("node-1", queryNodeName(c2));
			assertEquals(0L, s2.getOpenedStreamCount());

			// Shut down primary node-1; existing read connection and new connections must fail over to node-2
			s1.close();

			assertEquals("node-2", queryNodeName(c1));
			try (Connection c3 = ds.getConnection()) {
				assertEquals("node-2", queryNodeName(c3));
			}
		}
	}

	@Test
	void testNodeFailoverActiveStreamReadOnlyAndTransactionSafety() throws Exception {
		MockHAServer s1 = startNode("node-1");
		MockHAServer s2 = startNode("node-2");

		String url = String.format("litesql://127.0.0.1:%d,127.0.0.1:%d/cluster.db", s1.getPort(), s2.getPort());

		// Find one connection routed to node-1 for read-only testing, and one routed to node-1 for transaction testing
		List<Connection> extras = new ArrayList<>();
		Connection connReadOnly = null;
		Connection connTx = null;
		try {
			for (int i = 0; i < 8 && (connReadOnly == null || connTx == null); i++) {
				Connection candidate = DriverManager.getConnection(url);
				if ("node-1".equals(queryNodeName(candidate))) {
					if (connReadOnly == null) {
						connReadOnly = candidate;
					} else {
						connTx = candidate;
					}
				} else {
					extras.add(candidate);
				}
			}
			assertTrue(connReadOnly != null && connTx != null, "Expected connections routed to node-1");

			// 1. Configure read-only session on node-1
			connReadOnly.setReadOnly(true);
			assertTrue(connReadOnly.isReadOnly());

			// 2. Start explicit transaction with uncommitted write on node-1
			connTx.setAutoCommit(false);
			try (Statement st = connTx.createStatement()) {
				st.executeUpdate("INSERT INTO node_info VALUES ('uncommitted-on-node-1')");
			}

			// 3. Crash node-1!
			s1.close();

			// 4. Read-only connection in autoCommit=true must transparently reconnect to node-2 AND preserve query_only=1
			assertEquals("node-2", queryNodeName(connReadOnly));
			assertTrue(connReadOnly.isReadOnly());
			final Connection roRef = connReadOnly;
			assertThrows(SQLException.class, () -> {
				try (Statement st = roRef.createStatement()) {
					st.executeUpdate("INSERT INTO node_info VALUES ('should-be-blocked-by-read-only')");
				}
			});

			// 5. Transaction connection must NOT silently hide aborted transaction; commit/statement must fail
			final Connection txRef = connTx;
			assertThrows(SQLException.class, () -> {
				try (Statement st = txRef.createStatement()) {
					st.executeQuery("SELECT node_name FROM node_info");
				}
			});
			assertThrows(SQLException.class, txRef::commit);

			// 6. Calling rollback() must cleanly recover connTx onto healthy node-2 for new transactions!
			connTx.rollback();
			assertEquals("node-2", queryNodeName(connTx));
			try (Statement st = connTx.createStatement()) {
				st.executeUpdate("INSERT INTO node_info VALUES ('committed-on-node-2')");
			}
			connTx.commit();
			connTx.setAutoCommit(true);

			Set<String> rowsOnNode2 = new HashSet<>();
			try (Statement st = connTx.createStatement();
					ResultSet rs = st.executeQuery("SELECT node_name FROM node_info")) {
				while (rs.next()) {
					rowsOnNode2.add(rs.getString(1));
				}
			}
			assertEquals(new HashSet<>(Arrays.asList("node-2", "committed-on-node-2")), rowsOnNode2);
		} finally {
			for (Connection c : extras) {
				c.close();
			}
			if (connReadOnly != null) {
				connReadOnly.close();
			}
			if (connTx != null) {
				connTx.close();
			}
		}
	}

	@Test
	void testDnsTargetAndPartialDnsFailureResilience() throws Exception {
		MockHAServer s1 = startNode("dns-node");

		// Direct dns:/// scheme and jdbc:litesql:ha:dns:/// scheme
		try (Connection c1 = DriverManager
				.getConnection("dns:///127.0.0.1:" + s1.getPort() + "/cluster.db?disableSSL=true");
				Connection c2 = DriverManager
						.getConnection("jdbc:litesql:ha:dns:///localhost:" + s1.getPort() + "/cluster.db?disableSSL=true")) {
			assertEquals("dns-node", queryNodeName(c1));
			assertEquals("dns-node", queryNodeName(c2));
		}

		// Partial DNS failure in multi-host URL must still resolve and connect to healthy endpoints
		String partialFailureUrl = "litesql://nonexistent-node.invalid:9999,127.0.0.1:" + s1.getPort() + "/cluster.db";
		try (Connection conn = DriverManager.getConnection(partialFailureUrl)) {
			assertEquals("dns-node", queryNodeName(conn));
		}
	}
}
