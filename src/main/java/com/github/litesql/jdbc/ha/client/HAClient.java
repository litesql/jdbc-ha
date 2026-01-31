package com.github.litesql.jdbc.ha.client;

import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import build.buf.gen.sql.v1.ReplicationIDsResponse;
import build.buf.gen.sql.v1.Row;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusException;
import io.grpc.stub.BlockingClientCall;
import io.grpc.stub.StreamObserver;

/**
 * The entry point to HA Database client API.
 */
public class HAClient {

	private String replicationID;

	private final StreamObserver<QueryRequest> requestObserver;

	private ManagedChannel channel;
	private DatabaseServiceBlockingV2Stub stub;
	private CountDownLatch latch;
	private QueryResponse responseRef;

	private long txseq;

	public HAClient(URL url, String token, boolean enableSSL) {
		this.replicationID = url.getPath();
		if (this.replicationID.startsWith("/")) {
			this.replicationID = this.replicationID.substring(1);
		}

		ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(url.getHost(), url.getPort());
		if (!enableSSL) {
			channelBuilder = channelBuilder.usePlaintext();
		}

		this.channel = channelBuilder.build();

		CallCredentials credentials = new Credentials(token);

		this.stub = DatabaseServiceGrpc.newBlockingV2Stub(channel).withCallCredentials(credentials);

		DatabaseServiceGrpc.DatabaseServiceStub asyncStub = DatabaseServiceGrpc.newStub(channel)
				.withCallCredentials(credentials);

		StreamObserver<QueryResponse> responseObserver = new StreamObserver<QueryResponse>() {
			@Override
			public void onNext(QueryResponse response) {
				responseRef = response;

				if (response.getTxseq() > 0) {
					txseq = response.getTxseq();
				}
				latch.countDown();
			}

			@Override
			public void onError(Throwable t) {
				responseRef = QueryResponse.newBuilder().setError(t.getMessage()).build();
				if (latch != null) {
					latch.countDown();
				}
			}

			@Override
			public void onCompleted() {
			}
		};

		this.requestObserver = asyncStub.query(responseObserver);
	}

	public void downloadCurrentReplica(String dir, String replicationID, boolean override)
			throws StatusException, InterruptedException {
		java.nio.file.Path path = java.nio.file.Paths.get(dir, replicationID);
		if (!override && java.nio.file.Files.exists(path)) {
			return;
		}

		BlockingClientCall<?, DownloadResponse> call = this.stub
				.download(DownloadRequest.newBuilder().setReplicationId(replicationID).build());
		long offset = 0;
		while (call.hasNext()) {
			DownloadResponse response = call.read();
			offset = HAUtils.writeFileChunk(dir, replicationID, response.getData().toByteArray(), offset);
		}
	}

	public void downloadAllCurrentReplicas(String dir, boolean override) throws StatusException, InterruptedException {
		ReplicationIDsResponse response = this.stub.replicationIDs(null);
		for (int i = 0; i < response.getReplicationIdCount(); i++) {
			downloadCurrentReplica(dir, response.getReplicationId(i), override);
		}
	}

	public void downloadLatestSnapshot(String dir, String replicationID, boolean override)
			throws StatusException, InterruptedException {
		java.nio.file.Path path = java.nio.file.Paths.get(dir, replicationID);
		if (!override && java.nio.file.Files.exists(path)) {
			return;
		}
		BlockingClientCall<?, LatestSnapshotResponse> call = this.stub
				.latestSnapshot(LatestSnapshotRequest.newBuilder().setReplicationId(replicationID).build());
		long offset = 0;
		while (call.hasNext()) {
			LatestSnapshotResponse response = call.read();
			offset = HAUtils.writeFileChunk(dir, replicationID, response.getData().toByteArray(), offset);
		}
	}

	public void downloadAllLatestSnapshots(String dir, boolean override) throws StatusException, InterruptedException {
		this.stub.download(DownloadRequest.newBuilder().setReplicationId(replicationID).build());
		ReplicationIDsResponse response = this.stub.replicationIDs(null);
		for (int i = 0; i < response.getReplicationIdCount(); i++) {
			downloadLatestSnapshot(dir, response.getReplicationId(i), override);
		}
	}

	public List<String> getReplicationIDs() throws Exception {
		ReplicationIDsResponse response = this.stub.replicationIDs(null);
		List<String> ids = new ArrayList<>();
		for (int i = 0; i < response.getReplicationIdCount(); i++) {
			ids.add(response.getReplicationId(i));
		}

		return ids;
	}

	public HAExecutionResult getReplicationIDsResultSet() throws Exception {
		ReplicationIDsResponse response = this.stub.replicationIDs(null);
		List<Object[]> rows = new ArrayList<>();
		for (int i = 0; i < response.getReplicationIdCount(); i++) {
			Object[] row = new Object[1];
			row[0] = response.getReplicationId(i);
			rows.add(row);
		}

		List<String> columns = new ArrayList<>();
		columns.add("TABLE_CAT");

		return new HAExecutionResult(columns, rows);
	}

	/**
	 * Execute a single SQL statement.
	 *
	 * @return The result set.
	 */
	public HAExecutionResult executeQuery(String stmt, Map<Object, Object> parameters, int timeout)
			throws SQLException {
		QueryResponse response = send(stmt, parameters, QueryType.QUERY_TYPE_EXEC_QUERY, timeout);

		List<String> columns = new ArrayList<String>(response.getResultSet().getColumnsList());
		List<Object[]> rows = new ArrayList<Object[]>();
		try {
			for (Row rowAny : response.getResultSet().getRowsList()) {
				Object[] row = new Object[rowAny.getValuesCount()];
				for (int i = 0; i < rowAny.getValuesCount(); i++) {
					row[i] = Converter.fromAny(rowAny.getValues(i));
				}
				rows.add(row);
			}
		} catch (InvalidProtocolBufferException e) {
			throw new SQLException(e);
		}
		return new HAExecutionResult(columns, rows);
	}

	public int executeUpdate(String stmt, Map<Object, Object> parameters, int timeout) throws SQLException {
		QueryResponse response = send(stmt, parameters, QueryType.QUERY_TYPE_EXEC_UPDATE, timeout);
		return (int) response.getRowsAffected();
	}

	public HAExecutionResult execute(String stmt, Map<Object, Object> parameters, int timeout) throws SQLException {
		QueryResponse response = send(stmt, parameters, QueryType.QUERY_TYPE_UNSPECIFIED, timeout);

		if (response.getResultSet() == null || response.getResultSet().getColumnsCount() == 0) {
			return new HAExecutionResult(response.getRowsAffected());
		}
		List<String> columns = new ArrayList<String>(response.getResultSet().getColumnsList());
		List<Object[]> rows = new ArrayList<Object[]>();
		try {
			for (Row rowAny : response.getResultSet().getRowsList()) {
				Object[] row = new Object[rowAny.getValuesCount()];
				for (int i = 0; i < rowAny.getValuesCount(); i++) {
					row[i] = Converter.fromAny(rowAny.getValues(i));
				}
				rows.add(row);
			}
		} catch (InvalidProtocolBufferException e) {
			throw new SQLException(e);
		}
		return new HAExecutionResult(columns, rows);
	}

	private synchronized QueryResponse send(String sql, Map<Object, Object> parameters, QueryType type, int timeout)
			throws SQLException {
		this.latch = new CountDownLatch(1);
		QueryRequest.Builder builder = QueryRequest.newBuilder().setReplicationId(this.replicationID).setSql(sql)
				.setType(type);

		if (parameters != null && !parameters.isEmpty()) {
			boolean indexParameters = isIndexedParams(parameters);
			List<NamedValue> namedValues = new ArrayList<>();
			AtomicInteger counter = new AtomicInteger(1);
			parameters.forEach((key, value) -> {
				NamedValue.Builder namedValueBuilder = NamedValue.newBuilder();
				Any anyValue = Converter.toAny(value);
				if (indexParameters) {
					namedValueBuilder.setOrdinal(((Number) key).longValue()).setValue(anyValue).build();
				} else {
					namedValueBuilder.setName(String.valueOf(key)).setOrdinal(counter.longValue()).setValue(anyValue)
							.build();
				}
				counter.incrementAndGet();
				namedValues.add(namedValueBuilder.build());
			});
			builder.addAllParams(namedValues);
		}

		this.requestObserver.onNext(builder.build());
		try {
			this.latch.await(timeout, TimeUnit.SECONDS); // Wait for onNext
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		if (!responseRef.getError().isEmpty()) {
			throw new SQLException(responseRef.getError());
		}

		return responseRef;
	}

	public String getReplicationID() {
		return replicationID;
	}

	public void setReplicationID(String replicationID) {
		this.replicationID = replicationID;
	}

	public long getTxseq() {
		return txseq;
	}

	private boolean isIndexedParams(Map<Object, Object> parameter) {
		if (!parameter.isEmpty()) {
			return parameter.keySet().iterator().next() instanceof Integer;
		}
		return false;
	}

	public void close() {
		this.requestObserver.onCompleted();
		if (this.channel != null) {
			this.channel.shutdown();
		}
	}

}