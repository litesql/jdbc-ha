package com.github.litesql.jdbc.ha.client;

import java.util.concurrent.Executor;

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;

public class Credentials extends CallCredentials {

	private String token;
	
	private static Metadata.Key<String> META_DATA_AUTH_KEY =
		    Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);
	
	public Credentials(String token) {
		if(token == null) {
			token = "";
		}
		this.token = token;
	}
	@Override
	public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
		appExecutor.execute(() -> {
            try {
                Metadata headers = new Metadata();
                headers.put(META_DATA_AUTH_KEY, token);
                applier.apply(headers);
            } catch (Throwable e) {
                applier.fail(Status.UNAUTHENTICATED.withCause(e));
            }
        });
		
	}
	
}
