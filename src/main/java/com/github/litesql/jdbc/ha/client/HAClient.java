package com.github.litesql.jdbc.ha.client;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.litesql.jdbc.ha.HAConstants;
import com.github.litesql.jdbc.ha.HAException;
import com.github.litesql.jdbc.ha.HAUtils;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;

import build.buf.gen.sql.v1.DatabaseServiceGrpc;
import build.buf.gen.sql.v1.DatabaseServiceGrpc.DatabaseServiceBlockingV2Stub;
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
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.BlockingClientCall;

/**
 * The entry point to HA Database client API.
 */
public class HAClient implements AutoCloseable {

	private static final Logger LOGGER = Logger.getLogger(HAClient.class.getName());

	private volatile String replicationID;

	private final ManagedChannel channel;
	private final HAChannelManager.Lease channelLease;
	private final DatabaseServiceBlockingV2Stub stub;
	private BlockingClientCall<QueryRequest, QueryResponse> queryCall;

	private final ScheduledExecutorService executor;
	private volatile boolean closed = false;
	private volatile long txseq;

	private volatile boolean inTransaction = false;
	private volatile boolean streamHasTransaction = false;
	private volatile boolean readOnlySession = false;

	public HAClient(URL url, String token, boolean disableSSL) {
		this(url != null ? url.toString() : null, token, disableSSL, null);
	}

	public HAClient(URL url, String token, boolean disableSSL, String loadBalancerPolicy) {
		this(url != null ? url.toString() : null, token, disableSSL, loadBalancerPolicy);
	}

	public HAClient(String url, String token, boolean disableSSL) {
		this(url, token, disableSSL, null);
	}

	public HAClient(String url, String token, boolean disableSSL, String loadBalancerPolicy) {
		this(initFromUrl(url, disableSSL, loadBalancerPolicy), token);
	}

	public HAClient(ManagedChannel channel, String replicationID, String token) {
		this(null, Objects.requireNonNull(channel, "channel"), replicationID, token);
	}

	private static final class UrlChannelInit {
		final HAChannelManager.Lease lease;
		final String replicationId;

		UrlChannelInit(HAChannelManager.Lease lease, String replicationId) {
			this.lease = lease;
			this.replicationId = replicationId;
		}
	}

	private static UrlChannelInit initFromUrl(String url, boolean disableSSL, String loadBalancerPolicy) {
		HAConnectionUrl parsed = HAConnectionUrl.parse(url);
		boolean effectivePlaintext = disableSSL || parsed.isPlaintextByScheme()
				|| Boolean.parseBoolean(parsed.getQueryParams().get(HAConstants.CONNECTION_PROPERTY_DISABLE_SSL));
		String effectivePolicy = (loadBalancerPolicy != null && !loadBalancerPolicy.trim().isEmpty())
				? loadBalancerPolicy
				: parsed.getQueryParams().getOrDefault(HAConstants.CONNECTION_PROPERTY_LOAD_BALANCER_POLICY,
						HAConstants.DEFAULT_LOAD_BALANCER_POLICY);
		HAChannelManager.Lease lease = HAChannelManager.acquire(parsed.toGrpcTarget(), effectivePlaintext,
				effectivePolicy);
		return new UrlChannelInit(lease, parsed.getReplicationId());
	}

	private HAClient(UrlChannelInit init, String token) {
		this(init.lease, init.lease.getChannel(), init.replicationId, token);
	}

	private HAClient(HAChannelManager.Lease channelLease, ManagedChannel channel, String replicationID, String token) {
		this.channelLease = channelLease;
		this.channel = channel;
		this.replicationID = replicationID != null ? replicationID : "";

		CallCredentials credentials = new Credentials(token);
		this.stub = DatabaseServiceGrpc.newBlockingV2Stub(channel).withCallCredentials(credentials);

		this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "ha-client-ping");
			t.setDaemon(true);
			return t;
		});

		Runnable pingTask = () -> {
			if (this.closed || !hasOpenQueryStream()) {
				return;
			}
			try {
				send("SELECT 1", null, QueryType.QUERY_TYPE_PING, 5);
			} catch (SQLException e) {
				if (!this.closed) {
					LOGGER.log(Level.FINE, "Keepalive ping failed", e);
				}
			}
		};
		this.executor.scheduleWithFixedDelay(pingTask, 25, 25, TimeUnit.SECONDS);
	}

	private synchronized boolean hasOpenQueryStream() {
		return this.queryCall != null;
	}

	public ManagedChannel getChannel() {
		return this.channel;
	}

	public void setInTransaction(boolean inTransaction) {
		this.inTransaction = inTransaction;
	}

	public boolean isInTransaction() {
		return this.inTransaction;
	}

	public synchronized void markStreamTransactionStarted() {
		this.streamHasTransaction = true;
	}

	public synchronized void markStreamTransactionEnded() {
		this.streamHasTransaction = false;
	}

	public synchronized boolean hasActiveStreamTransaction() {
		return this.queryCall != null && this.streamHasTransaction;
	}

	public void setReadOnlySession(boolean readOnlySession) {
		this.readOnlySession = readOnlySession;
	}

	public boolean isReadOnlySession() {
		return this.readOnlySession;
	}

	public void downloadCurrentReplica(String dir, String replicationID, boolean override)
			throws StatusException, InterruptedException {
		try {
			Path target = HAUtils.resolveSafePath(dir, replicationID);
			if (!override && Files.exists(target)) {
				return;
			}
			Path parent = target.getParent();
			if (parent != null) {
				Files.createDirectories(parent);
			}
			Path tempFile = Files.createTempFile(parent, target.getFileName().toString() + ".", ".part");
			boolean success = false;
			BlockingClientCall<?, DownloadResponse> call = this.stub
					.download(DownloadRequest.newBuilder().setReplicationId(replicationID).build());
			try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(tempFile))) {
				while (call.hasNext()) {
					DownloadResponse response = call.read();
					response.getData().writeTo(out);
				}
				out.flush();
				success = true;
			} finally {
				if (!success) {
					call.cancel("Download aborted", null);
					Files.deleteIfExists(tempFile);
				}
			}
			Files.move(tempFile, target, StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			throw new RuntimeException("Failed to download current replica " + replicationID, e);
		}
	}

	public void downloadAllCurrentReplicas(String dir, boolean override) throws StatusException, InterruptedException {
		ReplicationIDsResponse response = this.stub.replicationIDs(ReplicationIDsRequest.getDefaultInstance());
		for (int i = 0; i < response.getReplicationIdCount(); i++) {
			downloadCurrentReplica(dir, response.getReplicationId(i), override);
		}
	}

	public void downloadLatestSnapshot(String dir, String replicationID, boolean override)
			throws StatusException, InterruptedException {
		try {
			Path target = HAUtils.resolveSafePath(dir, replicationID);
			if (!override && Files.exists(target)) {
				return;
			}
			Path parent = target.getParent();
			if (parent != null) {
				Files.createDirectories(parent);
			}
			Path tempFile = Files.createTempFile(parent, target.getFileName().toString() + ".", ".part");
			boolean success = false;
			BlockingClientCall<?, LatestSnapshotResponse> call = this.stub
					.latestSnapshot(LatestSnapshotRequest.newBuilder().setReplicationId(replicationID).build());
			try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(tempFile))) {
				while (call.hasNext()) {
					LatestSnapshotResponse response = call.read();
					response.getData().writeTo(out);
				}
				out.flush();
				success = true;
			} finally {
				if (!success) {
					call.cancel("Snapshot download aborted", null);
					Files.deleteIfExists(tempFile);
				}
			}
			Files.move(tempFile, target, StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			throw new RuntimeException("Failed to download latest snapshot " + replicationID, e);
		}
	}

	public void downloadAllLatestSnapshots(String dir, boolean override) throws StatusException, InterruptedException {
		ReplicationIDsResponse response = this.stub.replicationIDs(ReplicationIDsRequest.getDefaultInstance());
		for (int i = 0; i < response.getReplicationIdCount(); i++) {
			downloadLatestSnapshot(dir, response.getReplicationId(i), override);
		}
	}

	public List<String> getReplicationIDs() throws StatusException {
		ReplicationIDsResponse response = this.stub.replicationIDs(ReplicationIDsRequest.getDefaultInstance());
		return new ArrayList<>(response.getReplicationIdList());
	}

	public HAExecutionResult getReplicationIDsResultSet() throws StatusException {
		ReplicationIDsResponse response = this.stub.replicationIDs(ReplicationIDsRequest.getDefaultInstance());
		List<Object[]> rows = new ArrayList<>(response.getReplicationIdCount());
		for (int i = 0; i < response.getReplicationIdCount(); i++) {
			rows.add(new Object[] { response.getReplicationId(i) });
		}

		List<String> columns = new ArrayList<>(1);
		columns.add("TABLE_CAT");

		return new HAExecutionResult(columns, rows);
	}

	/**
	 * Execute a single SQL query returning a result set.
	 */
	public HAExecutionResult executeQuery(String stmt, Map<Object, Object> parameters, int timeout)
			throws SQLException {
		QueryResponse response = send(stmt, parameters, QueryType.QUERY_TYPE_EXEC_QUERY, timeout);
		return parseResultSet(response);
	}

	public int executeUpdate(String stmt, Map<Object, Object> parameters, int timeout) throws SQLException {
		return (int) executeUpdateWithResult(stmt, parameters, timeout).getRowsAffected();
	}

	public HAExecutionResult executeUpdateWithResult(String stmt, Map<Object, Object> parameters, int timeout)
			throws SQLException {
		QueryResponse response = send(stmt, parameters, QueryType.QUERY_TYPE_EXEC_UPDATE, timeout);
		return new HAExecutionResult(response.getRowsAffected(), response.getLastInsertId());
	}

	public HAExecutionResult execute(String stmt, Map<Object, Object> parameters, int timeout) throws SQLException {
		QueryResponse response = send(stmt, parameters, QueryType.QUERY_TYPE_UNSPECIFIED, timeout);

		if (!response.hasResultSet() || response.getResultSet().getColumnsCount() == 0) {
			return new HAExecutionResult(response.getRowsAffected(), response.getLastInsertId());
		}
		return parseResultSet(response);
	}

	private HAExecutionResult parseResultSet(QueryResponse response) throws SQLException {
		List<String> columns = new ArrayList<>(response.getResultSet().getColumnsList());
		List<Object[]> rows = new ArrayList<>(response.getResultSet().getRowsCount());
		try {
			for (Row rowAny : response.getResultSet().getRowsList()) {
				Object[] row = new Object[rowAny.getValuesCount()];
				for (int i = 0; i < rowAny.getValuesCount(); i++) {
					row[i] = Converter.fromAny(rowAny.getValues(i));
				}
				rows.add(row);
			}
		} catch (InvalidProtocolBufferException e) {
			throw new HAException("Failed to decode result set row", e);
		}
		return new HAExecutionResult(columns, rows);
	}

	private synchronized QueryResponse send(String sql, Map<Object, Object> parameters, QueryType type, int timeout)
			throws SQLException {
		return sendInternal(sql, parameters, type, timeout, true);
	}

	private QueryResponse sendInternal(String sql, Map<Object, Object> parameters, QueryType type, int timeout,
			boolean allowReconnectRetry) throws SQLException {
		if (this.closed) {
			throw new HAException("Connection is closed");
		}

		boolean reusedStream = (this.queryCall != null);
		if (this.queryCall == null) {
			this.queryCall = this.stub.query();
			this.streamHasTransaction = false;
			if (this.readOnlySession && !isPragmaQueryOnlyStatement(sql)) {
				applyReadOnlyPragmaOnNewStream(timeout);
			}
		}

		QueryRequest request = buildQueryRequest(sql, parameters, type);
		QueryResponse response;
		try {
			response = writeAndRead(request, timeout);
		} catch (TimeoutException e) {
			resetQueryCall("Query timed out", e);
			throw new SQLTimeoutException("Query timed out after " + timeout + " seconds", e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			resetQueryCall("Query interrupted", e);
			throw new HAException("Query interrupted", e);
		} catch (StatusException e) {
			resetQueryCall("gRPC stream failed", e);
			if (allowReconnectRetry && reusedStream && !this.inTransaction && isIdempotentQueryType(type)
					&& isRetryableStreamDisconnect(e)) {
				return sendInternal(sql, parameters, type, timeout, false);
			}
			throw new HAException("gRPC error: " + e.getMessage(), e);
		}

		if (response == null) {
			resetQueryCall("Stream closed by server", null);
			if (allowReconnectRetry && reusedStream && !this.inTransaction && isIdempotentQueryType(type)) {
				return sendInternal(sql, parameters, type, timeout, false);
			}
			throw new HAException("Query stream closed unexpectedly by server");
		}

		if (response.getTxseq() > 0) {
			this.txseq = response.getTxseq();
		}
		if (!response.getError().isEmpty()) {
			throw new HAException(response.getError());
		}

		return response;
	}

	private void applyReadOnlyPragmaOnNewStream(int timeout) throws SQLException {
		QueryRequest pragmaReq = buildQueryRequest("PRAGMA query_only = 1", null, QueryType.QUERY_TYPE_EXEC_UPDATE);
		try {
			QueryResponse resp = writeAndRead(pragmaReq, timeout);
			if (resp == null) {
				resetQueryCall("Stream closed during read-only initialization", null);
				throw new HAException("Query stream closed unexpectedly by server");
			}
			if (!resp.getError().isEmpty()) {
				throw new HAException(resp.getError());
			}
		} catch (TimeoutException e) {
			resetQueryCall("Read-only initialization timed out", e);
			throw new SQLTimeoutException("Query timed out after " + timeout + " seconds", e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			resetQueryCall("Read-only initialization interrupted", e);
			throw new HAException("Query interrupted", e);
		} catch (StatusException e) {
			resetQueryCall("Read-only initialization failed", e);
			throw new HAException("gRPC error: " + e.getMessage(), e);
		}
	}

	private QueryResponse writeAndRead(QueryRequest request, int timeout)
			throws StatusException, InterruptedException, TimeoutException {
		if (timeout > 0) {
			this.queryCall.write(request, timeout, TimeUnit.SECONDS);
			return this.queryCall.read(timeout, TimeUnit.SECONDS);
		}
		this.queryCall.write(request);
		return this.queryCall.read();
	}

	private QueryRequest buildQueryRequest(String sql, Map<Object, Object> parameters, QueryType type)
			throws SQLException {
		String repId = this.replicationID != null ? this.replicationID : "";
		QueryRequest.Builder builder = QueryRequest.newBuilder().setReplicationId(repId)
				.setSql(sql != null ? sql : "").setType(type);

		if (parameters != null && !parameters.isEmpty()) {
			List<NamedValue> namedValues = new ArrayList<>(parameters.size());
			long counter = 1L;
			for (Map.Entry<Object, Object> entry : parameters.entrySet()) {
				Object key = entry.getKey();
				Any anyValue = Converter.toAny(entry.getValue());
				NamedValue.Builder nvBuilder = NamedValue.newBuilder().setValue(anyValue);
				if (key instanceof Number) {
					nvBuilder.setOrdinal(((Number) key).longValue());
				} else {
					String name = String.valueOf(key);
					if (name.startsWith(":") || name.startsWith("@") || name.startsWith("$")) {
						name = name.substring(1);
					}
					nvBuilder.setName(name).setOrdinal(counter);
				}
				counter++;
				namedValues.add(nvBuilder.build());
			}
			builder.addAllParams(namedValues);
		}
		return builder.build();
	}

	private boolean isRetryableStreamDisconnect(StatusException e) {
		if (this.closed || Thread.currentThread().isInterrupted()) {
			return false;
		}
		Status.Code code = e.getStatus().getCode();
		return code == Status.Code.UNAVAILABLE || code == Status.Code.CANCELLED;
	}

	private static boolean isIdempotentQueryType(QueryType type) {
		return type == QueryType.QUERY_TYPE_EXEC_QUERY || type == QueryType.QUERY_TYPE_PING;
	}

	private static boolean isPragmaQueryOnlyStatement(String sql) {
		if (sql == null) {
			return false;
		}
		String normalized = HAUtils.stripLeadingComments(sql).trim().toLowerCase(Locale.ENGLISH);
		return normalized.startsWith("pragma query_only") && normalized.contains("=");
	}

	private void resetQueryCall(String reason, Throwable cause) {
		this.streamHasTransaction = false;
		if (this.queryCall != null) {
			try {
				this.queryCall.cancel(reason, cause);
			} catch (Exception ignored) {
			}
			this.queryCall = null;
		}
	}

	public String getReplicationID() {
		return replicationID;
	}

	public void setReplicationID(String replicationID) {
		this.replicationID = replicationID != null ? replicationID : "";
	}

	public long getTxseq() {
		return txseq;
	}

	@Override
	public void close() {
		this.closed = true;
		this.executor.shutdownNow();
		synchronized (this) {
			this.streamHasTransaction = false;
			if (this.queryCall != null) {
				try {
					this.queryCall.halfClose();
				} catch (Exception ignored) {
				}
				this.queryCall = null;
			}
		}
		if (this.channelLease != null) {
			this.channelLease.close();
		} else {
			HAChannelManager.shutdownChannel(this.channel);
		}
	}

}
