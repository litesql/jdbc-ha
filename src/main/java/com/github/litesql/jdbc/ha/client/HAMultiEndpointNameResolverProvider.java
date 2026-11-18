package com.github.litesql.jdbc.ha.client;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.litesql.jdbc.ha.client.HAConnectionUrl.Endpoint;

import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.NameResolverRegistry;
import io.grpc.Status;
import io.grpc.StatusOr;
import io.grpc.SynchronizationContext;

/**
 * gRPC {@link NameResolverProvider} that resolves comma-separated LiteSQL HA
 * cluster endpoints into independent {@link EquivalentAddressGroup}s for
 * client-side load balancing.
 */
public final class HAMultiEndpointNameResolverProvider extends NameResolverProvider {

	private static final Logger LOGGER = Logger.getLogger(HAMultiEndpointNameResolverProvider.class.getName());

	private static final Executor FALLBACK_EXECUTOR = Executors.newCachedThreadPool(r -> {
		Thread t = new Thread(r, "ha-name-resolver");
		t.setDaemon(true);
		return t;
	});

	public HAMultiEndpointNameResolverProvider() {
	}

	public static void ensureRegistered() {
		NameResolverRegistry registry = NameResolverRegistry.getDefaultRegistry();
		if (registry.getProviderForScheme(HAConnectionUrl.MULTI_ENDPOINT_SCHEME) == null) {
			registry.register(new HAMultiEndpointNameResolverProvider());
		}
	}

	@Override
	protected boolean isAvailable() {
		return true;
	}

	@Override
	protected int priority() {
		return 5;
	}

	@Override
	public String getDefaultScheme() {
		return HAConnectionUrl.MULTI_ENDPOINT_SCHEME;
	}

	@Override
	public Collection<Class<? extends SocketAddress>> getProducedSocketAddressTypes() {
		return Collections.singleton(InetSocketAddress.class);
	}

	@Override
	public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
		if (!HAConnectionUrl.MULTI_ENDPOINT_SCHEME.equalsIgnoreCase(targetUri.getScheme())) {
			return null;
		}
		String path = targetUri.getPath();
		if (path == null) {
			throw new IllegalArgumentException("Missing endpoint path in target URI: " + targetUri);
		}
		if (path.startsWith("/")) {
			path = path.substring(1);
		}
		if (path.trim().isEmpty()) {
			throw new IllegalArgumentException("Empty endpoint list in target URI: " + targetUri);
		}

		int defaultPort = args.getDefaultPort() > 0 ? args.getDefaultPort() : 80;
		String[] parts = path.split(",", -1);
		List<Endpoint> endpoints = new ArrayList<>(parts.length);
		for (String part : parts) {
			String trimmed = part.trim();
			if (trimmed.isEmpty()) {
				throw new IllegalArgumentException("Empty endpoint in target URI: " + targetUri);
			}
			endpoints.add(HAConnectionUrl.parseEndpoint(trimmed, defaultPort, targetUri.toString()));
		}

		return new MultiEndpointNameResolver(endpoints, args);
	}

	private static final class MultiEndpointNameResolver extends NameResolver {

		private final List<Endpoint> endpoints;
		private final SynchronizationContext syncContext;
		private final Executor offloadExecutor;
		private final String authority;

		private Listener2 listener;
		private boolean shutdown;
		private boolean resolving;

		MultiEndpointNameResolver(List<Endpoint> endpoints, Args args) {
			this.endpoints = endpoints;
			this.syncContext = Objects.requireNonNull(args.getSynchronizationContext(), "syncContext");
			this.offloadExecutor = args.getOffloadExecutor() != null ? args.getOffloadExecutor() : FALLBACK_EXECUTOR;
			this.authority = endpoints.get(0).toHostPort();
		}

		@Override
		public String getServiceAuthority() {
			return authority;
		}

		@Override
		public void start(Listener2 listener) {
			this.listener = Objects.requireNonNull(listener, "listener");
			resolve();
		}

		@Override
		public void refresh() {
			resolve();
		}

		@Override
		public void shutdown() {
			this.shutdown = true;
		}

		private void resolve() {
			if (resolving || shutdown || listener == null) {
				return;
			}
			resolving = true;
			final Listener2 savedListener = this.listener;
			offloadExecutor.execute(() -> {
				List<EquivalentAddressGroup> addressGroups = new ArrayList<>();
				UnknownHostException lastError = null;

				for (Endpoint endpoint : endpoints) {
					try {
						InetAddress[] resolved = InetAddress.getAllByName(endpoint.getHost());
						Attributes eagAttrs = Attributes.newBuilder()
								.set(EquivalentAddressGroup.ATTR_AUTHORITY_OVERRIDE, endpoint.toHostPort()).build();
						for (InetAddress addr : resolved) {
							addressGroups.add(new EquivalentAddressGroup(
									new InetSocketAddress(addr, endpoint.getPort()), eagAttrs));
						}
					} catch (UnknownHostException e) {
						lastError = e;
						LOGGER.log(Level.FINE, "Failed to resolve HA cluster endpoint " + endpoint, e);
					}
				}

				final UnknownHostException error = lastError;
				syncContext.execute(() -> {
					resolving = false;
					if (shutdown) {
						return;
					}
					if (addressGroups.isEmpty()) {
						Status status = Status.UNAVAILABLE
								.withDescription("Unable to resolve any endpoint in HA cluster: " + endpoints);
						if (error != null) {
							status = status.withCause(error);
						}
						savedListener.onResult2(ResolutionResult.newBuilder()
								.setAddressesOrError(StatusOr.fromStatus(status)).build());
					} else {
						savedListener.onResult2(ResolutionResult.newBuilder()
								.setAddressesOrError(StatusOr.fromValue(addressGroups)).build());
					}
				});
			});
		}
	}
}
