package com.github.litesql.jdbc.ha.client;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.github.litesql.jdbc.ha.HAConstants;

import io.grpc.ConnectivityState;
import io.grpc.LoadBalancerProvider;
import io.grpc.LoadBalancerRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Manages shared, reference-counted {@link ManagedChannel} instances keyed by
 * target URI, transport security mode, and load-balancing policy so multiple
 * JDBC connections distribute streams across cluster endpoints.
 */
public final class HAChannelManager {

	private static final long IDLE_EVICTION_SECONDS = 30L;
	private static final long INITIAL_CONNECT_WAIT_MS = 150L;
	private static final long SUBCHANNEL_SETTLE_MS = 25L;

	private static final Object LOCK = new Object();
	private static final Map<ChannelKey, ChannelEntry> CHANNELS = new HashMap<>();

	private static final ScheduledExecutorService REAPER = Executors.newSingleThreadScheduledExecutor(r -> {
		Thread t = new Thread(r, "ha-channel-reaper");
		t.setDaemon(true);
		return t;
	});

	private HAChannelManager() {
	}

	public static final class Lease implements AutoCloseable {
		private final ChannelEntry entry;
		private final AtomicBoolean closed = new AtomicBoolean(false);

		private Lease(ChannelEntry entry) {
			this.entry = entry;
		}

		public ManagedChannel getChannel() {
			return entry.channel;
		}

		public String getTarget() {
			return entry.key.target;
		}

		public String getLoadBalancerPolicy() {
			return entry.key.loadBalancerPolicy;
		}

		public boolean isPlaintext() {
			return entry.key.plaintext;
		}

		@Override
		public void close() {
			if (closed.compareAndSet(false, true)) {
				release(entry);
			}
		}
	}

	private static final class ChannelKey {
		final String target;
		final boolean plaintext;
		final String loadBalancerPolicy;

		ChannelKey(String target, boolean plaintext, String loadBalancerPolicy) {
			this.target = target;
			this.plaintext = plaintext;
			this.loadBalancerPolicy = loadBalancerPolicy;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof ChannelKey)) {
				return false;
			}
			ChannelKey that = (ChannelKey) o;
			return plaintext == that.plaintext && target.equals(that.target)
					&& loadBalancerPolicy.equals(that.loadBalancerPolicy);
		}

		@Override
		public int hashCode() {
			return Objects.hash(target, plaintext, loadBalancerPolicy);
		}
	}

	private static final class ChannelEntry {
		final ChannelKey key;
		final ManagedChannel channel;
		final CountDownLatch warmupLatch = new CountDownLatch(1);
		int refCount;
		ScheduledFuture<?> idleEvictionTask;
		boolean evicted;

		ChannelEntry(ChannelKey key, ManagedChannel channel) {
			this.key = key;
			this.channel = channel;
			this.refCount = 1;
		}
	}

	public static String normalizeAndValidatePolicy(String policy) {
		String normalized = (policy == null || policy.trim().isEmpty()) ? HAConstants.DEFAULT_LOAD_BALANCER_POLICY
				: policy.trim().toLowerCase(Locale.ENGLISH);
		LoadBalancerProvider provider = LoadBalancerRegistry.getDefaultRegistry().getProvider(normalized);
		if (provider == null) {
			throw new IllegalArgumentException("Unsupported gRPC loadBalancerPolicy: '" + policy + "'");
		}
		return normalized;
	}

	public static Lease acquire(String target, boolean plaintext, String loadBalancerPolicy) {
		Objects.requireNonNull(target, "target");
		String normalizedPolicy = normalizeAndValidatePolicy(loadBalancerPolicy);
		HAMultiEndpointNameResolverProvider.ensureRegistered();

		ChannelKey key = new ChannelKey(target, plaintext, normalizedPolicy);
		ChannelEntry entry;
		boolean newlyCreated = false;

		synchronized (LOCK) {
			entry = CHANNELS.get(key);
			if (entry != null && !entry.evicted && !entry.channel.isShutdown() && !entry.channel.isTerminated()) {
				if (entry.idleEvictionTask != null) {
					entry.idleEvictionTask.cancel(false);
					entry.idleEvictionTask = null;
				}
				entry.refCount++;
			} else {
				if (entry != null) {
					entry.evicted = true;
					CHANNELS.remove(key);
				}
				ManagedChannel channel = buildChannel(key);
				entry = new ChannelEntry(key, channel);
				CHANNELS.put(key, entry);
				newlyCreated = true;
			}
		}

		if (newlyCreated) {
			try {
				warmupChannel(entry.channel, normalizedPolicy);
			} finally {
				entry.warmupLatch.countDown();
			}
		} else {
			try {
				entry.warmupLatch.await(INITIAL_CONNECT_WAIT_MS + SUBCHANNEL_SETTLE_MS, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		return new Lease(entry);
	}

	private static ManagedChannel buildChannel(ChannelKey key) {
		ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(key.target)
				.defaultLoadBalancingPolicy(key.loadBalancerPolicy)
				.keepAliveTime(25, TimeUnit.SECONDS)
				.keepAliveTimeout(10, TimeUnit.SECONDS)
				.keepAliveWithoutCalls(true);
		if (key.plaintext) {
			builder.usePlaintext();
		}
		ManagedChannel channel = builder.build();
		channel.getState(true);
		return channel;
	}

	private static void warmupChannel(ManagedChannel channel, String policy) {
		ConnectivityState state = channel.getState(true);
		if (state != ConnectivityState.READY && state != ConnectivityState.SHUTDOWN) {
			CountDownLatch readyLatch = new CountDownLatch(1);
			observeUntilReady(channel, state, readyLatch);
			try {
				readyLatch.await(INITIAL_CONNECT_WAIT_MS, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
		if ("round_robin".equals(policy) && channel.getState(false) == ConnectivityState.READY) {
			try {
				Thread.sleep(SUBCHANNEL_SETTLE_MS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	private static void observeUntilReady(ManagedChannel channel, ConnectivityState current, CountDownLatch latch) {
		if (current == ConnectivityState.READY || current == ConnectivityState.SHUTDOWN) {
			latch.countDown();
			return;
		}
		channel.notifyWhenStateChanged(current, () -> {
			ConnectivityState next = channel.getState(false);
			if (next == ConnectivityState.READY || next == ConnectivityState.SHUTDOWN
					|| next == ConnectivityState.TRANSIENT_FAILURE) {
				latch.countDown();
			} else {
				observeUntilReady(channel, next, latch);
			}
		});
	}

	private static void release(ChannelEntry entry) {
		synchronized (LOCK) {
			if (entry.evicted) {
				return;
			}
			entry.refCount = Math.max(0, entry.refCount - 1);
			if (entry.refCount == 0 && entry.idleEvictionTask == null) {
				entry.idleEvictionTask = REAPER.schedule(() -> evictIfIdle(entry), IDLE_EVICTION_SECONDS,
						TimeUnit.SECONDS);
			}
		}
	}

	private static void evictIfIdle(ChannelEntry entry) {
		ManagedChannel toShutdown = null;
		synchronized (LOCK) {
			if (!entry.evicted && entry.refCount == 0) {
				entry.evicted = true;
				entry.idleEvictionTask = null;
				CHANNELS.remove(entry.key, entry);
				toShutdown = entry.channel;
			}
		}
		if (toShutdown != null) {
			shutdownChannel(toShutdown);
		}
	}

	public static void shutdownChannel(ManagedChannel channel) {
		if (channel != null && !channel.isShutdown()) {
			channel.shutdown();
			try {
				if (!channel.awaitTermination(2, TimeUnit.SECONDS)) {
					channel.shutdownNow();
				}
			} catch (InterruptedException e) {
				channel.shutdownNow();
				Thread.currentThread().interrupt();
			}
		}
	}
}
