package com.github.litesql.jdbc.driver.test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.github.litesql.jdbc.ha.client.Converter;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;

import build.buf.gen.sql.v1.Data;
import build.buf.gen.sql.v1.DatabaseServiceGrpc;
import build.buf.gen.sql.v1.DownloadRequest;
import build.buf.gen.sql.v1.DownloadResponse;
import build.buf.gen.sql.v1.LatestSnapshotRequest;
import build.buf.gen.sql.v1.LatestSnapshotResponse;
import build.buf.gen.sql.v1.NamedValue;
import build.buf.gen.sql.v1.QueryRequest;
import build.buf.gen.sql.v1.QueryResponse;
import build.buf.gen.sql.v1.QueryType;
import build.buf.gen.sql.v1.ReplicationIDsRequest;
import build.buf.gen.sql.v1.ReplicationIDsResponse;
import build.buf.gen.sql.v1.Row;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.stub.StreamObserver;

/**
 * In-process / loopback gRPC test server implementing LiteSQL HA DatabaseService
 * backed by real SQLite files.
 */
public class MockHAServer implements AutoCloseable {

	private static final Metadata.Key<String> AUTH_HEADER = Metadata.Key.of("Authorization",
			Metadata.ASCII_STRING_MARSHALLER);
	private static final Context.Key<String> AUTH_CTX_KEY = Context.key("auth-token");

	private final Path dbDir;
	private final Map<String, Path> databases = new ConcurrentHashMap<>();
	private final AtomicLong txseq = new AtomicLong(1L);
	private final AtomicReference<String> lastAuthToken = new AtomicReference<>("");
	private volatile long artificialDelayMs = 0L;
	private final Server server;

	public MockHAServer(Path dbDir) throws IOException {
		this.dbDir = dbDir;
		Files.createDirectories(dbDir);
		registerDatabase("test.db");
		registerDatabase("other.db");

		ServerInterceptor authInterceptor = new ServerInterceptor() {
			@Override
			public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
					ServerCallHandler<ReqT, RespT> next) {
				String token = headers.get(AUTH_HEADER);
				if (token != null) {
					lastAuthToken.set(token);
				}
				Context ctx = Context.current().withValue(AUTH_CTX_KEY, token != null ? token : "");
				return Contexts.interceptCall(ctx, call, headers, next);
			}
		};

		this.server = Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
				.addService(new DatabaseServiceImpl()).intercept(authInterceptor).build().start();
	}

	public Path registerDatabase(String name) {
		Path dbPath = dbDir.resolve(name);
		databases.put(name, dbPath);
		try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + dbPath.toAbsolutePath());
				Statement s = c.createStatement()) {
			s.execute("PRAGMA journal_mode=WAL");
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return dbPath;
	}

	public int getPort() {
		return server.getPort();
	}

	public String getLastAuthToken() {
		return lastAuthToken.get();
	}

	public void setArtificialDelayMs(long delayMs) {
		this.artificialDelayMs = delayMs;
	}

	public long getCurrentTxSeq() {
		return txseq.get();
	}

	@Override
	public void close() {
		server.shutdownNow();
	}

	private class DatabaseServiceImpl extends DatabaseServiceGrpc.DatabaseServiceImplBase {

		@Override
		public StreamObserver<QueryRequest> query(StreamObserver<QueryResponse> responseObserver) {
			final Map<String, Connection> sessionConns = new ConcurrentHashMap<>();

			return new StreamObserver<QueryRequest>() {
				@Override
				public void onNext(QueryRequest request) {
					try {
						long delay = artificialDelayMs;
						if (delay > 0 && request.getSql().contains("SLOW_QUERY")) {
							Thread.sleep(delay);
						}

						if (request.getType() == QueryType.QUERY_TYPE_PING) {
							responseObserver.onNext(QueryResponse.newBuilder().setTxseq(txseq.get()).build());
							return;
						}

						String repId = request.getReplicationId().isEmpty() ? "test.db" : request.getReplicationId();
						Connection conn = getOrCreateConn(sessionConns, repId);
						QueryResponse response = executeOnSqlite(conn, request);
						responseObserver.onNext(response);
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
					} catch (Exception e) {
						responseObserver.onNext(QueryResponse.newBuilder()
								.setError(e.getMessage() != null ? e.getMessage() : e.toString())
								.setTxseq(txseq.get()).build());
					}
				}

				@Override
				public void onError(Throwable t) {
					closeAll();
				}

				@Override
				public void onCompleted() {
					closeAll();
					responseObserver.onCompleted();
				}

				private void closeAll() {
					for (Connection c : sessionConns.values()) {
						try {
							c.close();
						} catch (SQLException ignored) {
						}
					}
					sessionConns.clear();
				}
			};
		}

		private Connection getOrCreateConn(Map<String, Connection> sessionConns, String repId) throws SQLException {
			Connection existing = sessionConns.get(repId);
			if (existing != null && !existing.isClosed()) {
				return existing;
			}
			Path dbPath = databases.computeIfAbsent(repId, k -> dbDir.resolve(k));
			Connection c = DriverManager.getConnection("jdbc:sqlite:" + dbPath.toAbsolutePath());
			sessionConns.put(repId, c);
			return c;
		}

		private QueryResponse executeOnSqlite(Connection conn, QueryRequest req) throws Exception {
			String sql = req.getSql().trim();
			if ("SELECT PROTO_TIMESTAMP_TEST".equalsIgnoreCase(sql)) {
				Instant sampleInstant = Instant.parse("2026-11-17T12:34:56.789Z");
				Row row = Row.newBuilder().addValues(Converter.toAny(sampleInstant)).build();
				Data data = Data.newBuilder().addColumns("ts_col").addRows(row).build();
				return QueryResponse.newBuilder().setResultSet(data).setTxseq(txseq.get()).build();
			}

			List<NamedValue> params = req.getParamsList();
			if (req.getType() == QueryType.QUERY_TYPE_EXEC_QUERY) {
				try (PreparedStatement ps = conn.prepareStatement(sql)) {
					bindParams(ps, params);
					try (ResultSet rs = ps.executeQuery()) {
						return QueryResponse.newBuilder().setResultSet(toProtoData(rs)).setTxseq(txseq.get()).build();
					}
				}
			} else if (req.getType() == QueryType.QUERY_TYPE_EXEC_UPDATE) {
				return runUpdate(conn, sql, params);
			} else {
				try (PreparedStatement ps = conn.prepareStatement(sql)) {
					bindParams(ps, params);
					boolean hasRs = ps.execute();
					if (hasRs) {
						try (ResultSet rs = ps.getResultSet()) {
							return QueryResponse.newBuilder().setResultSet(toProtoData(rs)).setTxseq(txseq.get())
									.build();
						}
					} else {
						int affected = ps.getUpdateCount();
						long lastId = fetchLastInsertId(conn);
						long seq = txseq.incrementAndGet();
						return QueryResponse.newBuilder().setRowsAffected(Math.max(0, affected))
								.setLastInsertId(lastId).setTxseq(seq).build();
					}
				}
			}
		}

		private QueryResponse runUpdate(Connection conn, String sql, List<NamedValue> params) throws Exception {
			if (params.isEmpty()) {
				try (Statement st = conn.createStatement()) {
					int affected = st.executeUpdate(sql);
					long lastId = fetchLastInsertId(conn);
					long seq = txseq.incrementAndGet();
					return QueryResponse.newBuilder().setRowsAffected(affected).setLastInsertId(lastId).setTxseq(seq)
							.build();
				}
			}
			try (PreparedStatement ps = conn.prepareStatement(sql)) {
				bindParams(ps, params);
				int affected = ps.executeUpdate();
				long lastId = fetchLastInsertId(conn);
				long seq = txseq.incrementAndGet();
				return QueryResponse.newBuilder().setRowsAffected(affected).setLastInsertId(lastId).setTxseq(seq)
						.build();
			}
		}

		private long fetchLastInsertId(Connection conn) {
			try (Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery("SELECT last_insert_rowid()")) {
				if (rs.next()) {
					return rs.getLong(1);
				}
			} catch (SQLException ignored) {
			}
			return 0L;
		}

		private void bindParams(PreparedStatement ps, List<NamedValue> params) throws Exception {
			for (NamedValue nv : params) {
				Object val = Converter.fromAny(nv.getValue());
				int idx = (int) nv.getOrdinal();
				if (val == null) {
					ps.setObject(idx, null);
				} else if (val instanceof Instant) {
					ps.setString(idx, val.toString());
				} else {
					ps.setObject(idx, val);
				}
			}
		}

		private Data toProtoData(ResultSet rs) throws Exception {
			ResultSetMetaData rsmd = rs.getMetaData();
			int colCount = rsmd.getColumnCount();
			List<String> cols = new ArrayList<>(colCount);
			for (int i = 1; i <= colCount; i++) {
				String label = rsmd.getColumnLabel(i);
				cols.add(label != null ? label : rsmd.getColumnName(i));
			}
			Data.Builder dataBuilder = Data.newBuilder().addAllColumns(cols);
			while (rs.next()) {
				Row.Builder rowBuilder = Row.newBuilder();
				for (int i = 1; i <= colCount; i++) {
					Object obj = rs.getObject(i);
					Any anyVal = Converter.toAny(obj);
					rowBuilder.addValues(anyVal);
				}
				dataBuilder.addRows(rowBuilder.build());
			}
			return dataBuilder.build();
		}

		@Override
		public void replicationIDs(ReplicationIDsRequest request,
				StreamObserver<ReplicationIDsResponse> responseObserver) {
			ReplicationIDsResponse.Builder builder = ReplicationIDsResponse.newBuilder();
			builder.addAllReplicationId(databases.keySet());
			responseObserver.onNext(builder.build());
			responseObserver.onCompleted();
		}

		@Override
		public void download(DownloadRequest request, StreamObserver<DownloadResponse> responseObserver) {
			String repId = request.getReplicationId().isEmpty() ? "test.db" : request.getReplicationId();
			Path path = databases.get(repId);
			try {
				byte[] bytes = (path != null && Files.exists(path)) ? Files.readAllBytes(path)
						: "sqlite-replica-bytes".getBytes();
				int chunkSize = 1024;
				for (int offset = 0; offset < bytes.length; offset += chunkSize) {
					int len = Math.min(chunkSize, bytes.length - offset);
					responseObserver.onNext(DownloadResponse.newBuilder()
							.setData(ByteString.copyFrom(bytes, offset, len)).build());
				}
				responseObserver.onCompleted();
			} catch (IOException e) {
				responseObserver.onError(e);
			}
		}

		@Override
		public void latestSnapshot(LatestSnapshotRequest request,
				StreamObserver<LatestSnapshotResponse> responseObserver) {
			String repId = request.getReplicationId().isEmpty() ? "test.db" : request.getReplicationId();
			Path path = databases.get(repId);
			try {
				byte[] bytes = (path != null && Files.exists(path)) ? Files.readAllBytes(path)
						: "sqlite-snapshot-bytes".getBytes();
				responseObserver
						.onNext(LatestSnapshotResponse.newBuilder().setData(ByteString.copyFrom(bytes)).build());
				responseObserver.onCompleted();
			} catch (IOException e) {
				responseObserver.onError(e);
			}
		}
	}
}
