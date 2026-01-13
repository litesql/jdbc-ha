package com.github.litesql.jdbc.driver.ha.client;

import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;

import build.buf.gen.sql.v1.DatabaseServiceGrpc;
import build.buf.gen.sql.v1.NamedValue;
import build.buf.gen.sql.v1.QueryRequest;
import build.buf.gen.sql.v1.QueryResponse;
import build.buf.gen.sql.v1.QueryType;
import build.buf.gen.sql.v1.Row;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

/**
 * The entry point to HA Database client API.
 */
public class HAClient {
    
    private String replicationID;

    private final StreamObserver<QueryRequest> requestObserver;
    
    private CountDownLatch latch;
    private QueryResponse responseRef;

    public HAClient(URL url) {
        this.replicationID = url.getPath();
        if (this.replicationID.startsWith("/")) {
        	this.replicationID = this.replicationID.substring(1);
        }

        ManagedChannel channel = ManagedChannelBuilder.forAddress(url.getHost(), url.getPort())
                .usePlaintext()
                .build();

        DatabaseServiceGrpc.DatabaseServiceStub asyncStub = DatabaseServiceGrpc.newStub(channel);
        
        StreamObserver<QueryResponse> responseObserver = new StreamObserver<QueryResponse>() {
            @Override
            public void onNext(QueryResponse response) {                
                responseRef = response;
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
                responseRef = null;
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                System.out.println("Finished.");
            }
        };
        
        this.requestObserver  = asyncStub.query(responseObserver);        
    }

    /**
     * Execute a single SQL statement.
     *
     * @return The result set.
     */
    public HAExecutionResult executeQuery(String stmt, Map<Object, Object> parameters, int timeout) throws SQLException {
    	QueryResponse response = send(stmt, parameters, QueryType.QUERY_TYPE_EXEC_QUERY, timeout);
    	
    	List<String> columns = new ArrayList<String>(response.getResultSet().getColumnsList());
    	List<Object[]> rows = new ArrayList<Object[]>();
    	try {
    	for (Row rowAny: response.getResultSet().getRowsList()) {
    		Object[] row = new Object[rowAny.getValuesCount()];
    		for (int i = 0; i < rowAny.getValuesCount(); i++) {    		
    			row[i] = Converter.fromAny(rowAny.getValues(i));
    		}
    		rows.add(row);
    	}
    	} catch(InvalidProtocolBufferException e) {
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
    	for (Row rowAny: response.getResultSet().getRowsList()) {
    		Object[] row = new Object[rowAny.getValuesCount()];
    		for (int i = 0; i < rowAny.getValuesCount(); i++) {    		
    			row[i] = Converter.fromAny(rowAny.getValues(i));
    		}
    		rows.add(row);
    	}
    	} catch(InvalidProtocolBufferException e) {
    		throw new SQLException(e);
    	}
        return new HAExecutionResult(columns, rows);
    }
    
    private synchronized QueryResponse send(String sql, Map<Object, Object> parameters, QueryType type, int timeout) throws SQLException {
    	this.latch = new CountDownLatch(1);
    	QueryRequest.Builder builder = QueryRequest.newBuilder().setReplicationId(this.replicationID).setSql(sql).setType(type);
    	
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
    				namedValueBuilder.setName(String.valueOf(key)).setOrdinal(counter.longValue()).setValue(anyValue).build();
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

    private boolean isIndexedParams(Map<Object, Object> parameter) {
        if (!parameter.isEmpty()) {
            return parameter.keySet().iterator().next() instanceof Integer;
        }
        return false;
    }

    public void close() {
    	this.requestObserver.onCompleted();
    }

}