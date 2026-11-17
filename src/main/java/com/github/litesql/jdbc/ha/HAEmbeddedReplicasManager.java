package com.github.litesql.jdbc.ha;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConfig.JournalMode;
import org.sqlite.SQLiteConfig.TempStore;
import org.sqlite.SQLiteDataSource;

public class HAEmbeddedReplicasManager {

	private static final Logger LOGGER = Logger.getLogger(HAEmbeddedReplicasManager.class.getName());

	public static class ReplicaConn {
		final String dsn;
		final Connection conn;
		volatile long txseq;

		protected ReplicaConn(String dsn, Connection conn, long txseq) {
			this.dsn = dsn;
			this.conn = conn;
			this.txseq = txseq;
		}

		public Connection createConn() throws SQLException {
			SQLiteConfig config = new SQLiteConfig();
			config.setJournalMode(JournalMode.WAL);
			config.setTempStore(TempStore.MEMORY);
			config.setBusyTimeout(5000);
			SQLiteDataSource ds = new SQLiteDataSource(config);
			ds.setUrl(this.dsn);
			Connection c = ds.getConnection();
			try (Statement st = c.createStatement()) {
				st.execute("PRAGMA query_only = ON");
			}
			return c;
		}

		public Connection getConn() {
			return this.conn;
		}

		public long getTxSeq() {
			return this.txseq;
		}
	}

	private static final Map<String, ReplicaConn> conns = new ConcurrentHashMap<>();

	private static File tempExt;
	private static ScheduledExecutorService scheduler;
	private static boolean shutdownHookRegistered = false;

	private static synchronized File ensureExtensionExtracted() throws SQLException {
		if (tempExt != null && tempExt.exists()) {
			return tempExt;
		}

		String os = System.getProperty("os.name", "").toLowerCase(Locale.ENGLISH);
		String osDir;
		String extension;
		if (os.startsWith("linux")) {
			osDir = "linux";
			extension = ".so";
		} else if (os.startsWith("win")) {
			osDir = "windows";
			extension = ".dll";
		} else if (os.startsWith("mac") || os.startsWith("darwin")) {
			osDir = "darwin";
			extension = ".dylib";
		} else {
			throw new HAException("Unsupported operating system for embedded replicas: " + os);
		}

		String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ENGLISH);
		String archDir;
		if (arch.equals("arm64") || arch.equals("aarch64")) {
			archDir = "arm64";
		} else if (arch.equals("x86_64") || arch.equals("amd64")) {
			archDir = "x86_64";
		} else {
			throw new HAException("Unsupported CPU architecture for embedded replicas: " + arch);
		}

		String resourcePath = "/ha-sync/" + osDir + "/" + archDir + "/ha-sync" + extension;
		try (InputStream is = HAEmbeddedReplicasManager.class.getResourceAsStream(resourcePath)) {
			if (is == null) {
				throw new HAException("Embedded replica native extension not found in classpath: " + resourcePath);
			}
			Path extracted = Files.createTempFile("ha-sync-", extension);
			Files.copy(is, extracted, StandardCopyOption.REPLACE_EXISTING);
			tempExt = extracted.toFile();
			tempExt.deleteOnExit();
			return tempExt;
		} catch (IOException e) {
			throw new HAException("Failed to extract ha-sync native extension", e);
		}
	}

	public static synchronized void load(String dir, String url, String stream, String durable) throws SQLException {
		File directory = new File(dir);
		if (!directory.exists() || !directory.isDirectory()) {
			throw new IllegalArgumentException("Invalid directory: " + dir);
		}

		File[] files = directory.listFiles();
		if (files == null || files.length == 0) {
			return;
		}

		File extFile = null;
		for (File file : files) {
			if (!file.isFile() || conns.containsKey(file.getName()) || !isSqliteFile(file)) {
				continue;
			}
			if (extFile == null) {
				extFile = ensureExtensionExtracted();
				ensureSchedulerStarted();
			}

			String dsn = "jdbc:sqlite:" + file.getAbsolutePath();
			SQLiteDataSource ds = new SQLiteDataSource();
			ds.setJournalMode(JournalMode.WAL.getValue());
			ds.setBusyTimeout(5000);
			ds.setTempStore(TempStore.MEMORY.getValue());
			ds.setUrl(dsn);
			ds.setLoadExtension(true);

			Connection conn = ds.getConnection();
			boolean success = false;
			try {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("SELECT load_extension('" + HAUtils.escape(extFile.getAbsolutePath()) + "')");
					stmt.execute("CREATE VIRTUAL TABLE temp.ha USING HA(servers='" + HAUtils.escape(url)
							+ "', timeout=5000)");
				}
				try (PreparedStatement pstmt = conn
						.prepareStatement("INSERT INTO temp.ha(subject, durable) VALUES(?, ?)")) {
					pstmt.setString(1, stream + "." + file.getName().replaceAll("\\.", "_"));
					pstmt.setString(2, durable);
					pstmt.execute();
				}
				long txseq = 0;
				try (Statement stmt = conn.createStatement();
						ResultSet rs = stmt
								.executeQuery("SELECT received_seq FROM ha_stats ORDER BY updated_at DESC LIMIT 1")) {
					if (rs.next()) {
						txseq = rs.getLong(1);
					}
				}
				conns.put(file.getName(), new ReplicaConn(dsn, conn, txseq));
				success = true;
			} finally {
				if (!success) {
					try {
						conn.close();
					} catch (SQLException ignored) {
					}
				}
			}
		}
	}

	public static ReplicaConn getReplica(String dbName) {
		if (conns.isEmpty()) {
			return null;
		}
		if (dbName == null || dbName.isEmpty()) {
			return conns.size() == 1 ? conns.values().iterator().next() : null;
		}
		ReplicaConn direct = conns.get(dbName);
		if (direct != null) {
			return direct;
		}
		Path fileName = Paths.get(dbName).getFileName();
		return fileName != null ? conns.get(fileName.toString()) : null;
	}

	static void registerReplica(String dbName, ReplicaConn replica) {
		conns.put(dbName, replica);
	}

	static void unregisterReplica(String dbName) {
		ReplicaConn removed = conns.remove(dbName);
		if (removed != null && removed.conn != null) {
			try {
				removed.conn.close();
			} catch (SQLException ignored) {
			}
		}
	}

	private static synchronized void ensureSchedulerStarted() {
		if (!shutdownHookRegistered) {
			Runtime.getRuntime().addShutdownHook(new Thread(HAEmbeddedReplicasManager::stopScheduler));
			shutdownHookRegistered = true;
		}
		if (scheduler == null || scheduler.isShutdown()) {
			scheduler = Executors.newScheduledThreadPool(1, r -> {
				Thread t = new Thread(r, "ha-txseq-updater");
				t.setDaemon(true);
				return t;
			});
			scheduler.scheduleWithFixedDelay(HAEmbeddedReplicasManager::updateTxSeq, 5, 5, TimeUnit.SECONDS);
		}
	}

	private static void updateTxSeq() {
		for (Map.Entry<String, ReplicaConn> entry : conns.entrySet()) {
			ReplicaConn replica = entry.getValue();
			if (replica.conn == null) {
				continue;
			}
			synchronized (replica) {
				try (Statement stmt = replica.conn.createStatement();
						ResultSet rs = stmt
								.executeQuery("SELECT received_seq FROM ha_stats ORDER BY updated_at DESC LIMIT 1")) {
					if (rs.next()) {
						replica.txseq = rs.getLong(1);
					}
				} catch (SQLException e) {
					LOGGER.log(Level.WARNING, "Error updating txseq for replica " + entry.getKey(), e);
				}
			}
		}
	}

	public static synchronized void stopScheduler() {
		if (scheduler != null && !scheduler.isShutdown()) {
			scheduler.shutdown();
			try {
				if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
					scheduler.shutdownNow();
				}
			} catch (InterruptedException e) {
				scheduler.shutdownNow();
				Thread.currentThread().interrupt();
			}
		}
		for (Map.Entry<String, ReplicaConn> entry : conns.entrySet()) {
			ReplicaConn replica = entry.getValue();
			if (replica.conn != null) {
				try {
					replica.conn.close();
				} catch (SQLException e) {
					LOGGER.log(Level.FINE, "Error closing replica connection " + entry.getKey(), e);
				}
			}
		}
		conns.clear();
	}

	private static boolean isSqliteFile(File file) {
		if (file.length() < 100) {
			return false;
		}
		try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
			byte[] header = new byte[16];
			raf.readFully(header);
			String headerStr = new String(header, StandardCharsets.UTF_8);
			return headerStr.startsWith("SQLite format 3");
		} catch (Exception e) {
			return false;
		}
	}
}
