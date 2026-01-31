package com.github.litesql.jdbc.ha;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.sqlite.SQLiteConfig.JournalMode;
import org.sqlite.SQLiteConfig.TempStore;
import org.sqlite.SQLiteDataSource;

public class HAEmbeddedReplicasManager {

	public static class ReplicaConn {
		String dsn;
		Connection conn;
		long txseq;

		protected ReplicaConn(String dsn, Connection conn, long txseq) {
			this.dsn = dsn;
			this.conn = conn;
			this.txseq = txseq;
		}

		public Connection createConn() throws SQLException {
			SQLiteDataSource ds = new SQLiteDataSource();
			ds.setJournalMode(JournalMode.WAL.getValue());
			ds.setTempStore(TempStore.MEMORY.getValue());
			ds.setUrl(this.dsn);
			return ds.getConnection();
		}

		public Connection getConn() {
			return this.conn;
		}

		public long getTxSeq() {
			return this.txseq;
		}
	}

	private static Map<String, ReplicaConn> conns = new HashMap<>();

	private static File tempExt;

	private static ScheduledExecutorService scheduler;

	static {
		StringBuilder sb = new StringBuilder("/ha-sync/");
		String os = System.getProperty("os.name").toLowerCase();
		String extension = "";
		if (os.startsWith("linux")) {
			sb.append("linux/");
			extension = ".so";
		} else if (os.startsWith("win")) {
			sb.append("windows/");
			extension = ".dll";
		} else if (os.startsWith("mac")) {
			sb.append("darwin/");
			extension = ".dylib";
		}

		String arch = System.getProperty("os.arch").toLowerCase();
		if (arch.equals("arm64")) {
			sb.append("arm64/");
		} else {
			sb.append("x86_64/");
		}

		sb.append("ha-sync");
		sb.append(extension);

		InputStream is = HAEmbeddedReplicasManager.class.getResourceAsStream(sb.toString());
		tempExt = new File(System.getProperty("java.io.tmpdir"), "ha-sync" + extension);
		try {
			Files.copy(is, tempExt.toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (Exception e) {
			e.printStackTrace();
		}

		startScheduler();
		// ensure scheduler and connections are cleaned up on JVM shutdown
		Runtime.getRuntime().addShutdownHook(new Thread(HAEmbeddedReplicasManager::stopScheduler));
	}

	public static void load(String dir, String url, String stream, String durable) throws SQLException {
		File directory = new File(dir);
		if (!directory.exists() || !directory.isDirectory()) {
			throw new IllegalArgumentException("Invalid directory: " + dir);
		}
		File[] files = directory.listFiles();

		if (files != null) {
			for (File file : files) {
				if (!file.isFile() || conns.containsKey(file.getName())) {
					continue;
				}
				if (!isSqliteFile(file)) {
					continue;
				}
				String dsn = "jdbc:sqlite:" + file.getAbsolutePath();
				SQLiteDataSource ds = new SQLiteDataSource();
				ds.setJournalMode(JournalMode.WAL.getValue());
				ds.setBusyTimeout(5000);
				ds.setTempStore(TempStore.MEMORY.getValue());
				ds.setUrl(dsn);
				ds.setLoadExtension(true);
				Connection conn = ds.getConnection();

				try (Statement stmt = conn.createStatement()) {
					stmt.execute("SELECT load_extension('" + tempExt.getAbsolutePath() + "')");
					stmt.execute("CREATE VIRTUAL TABLE temp.ha USING HA(servers='" + url + "', timeout=5000)");
				}
				try (PreparedStatement pstmt = conn
						.prepareStatement("INSERT INTO temp.ha(subject, durable) VALUES(?, ?)")) {
					pstmt.setString(1, stream + "." + file.getName().replaceAll("\\.", "_"));
					pstmt.setString(2, durable);
					pstmt.execute();
				}
				long txseq = 0;
				try (Statement stmt = conn.createStatement()) {
					try (ResultSet rs = stmt
							.executeQuery("SELECT received_seq FROM ha_stats ORDER BY updated_at DESC LIMIT 1")) {
						while (rs.next()) {
							txseq = rs.getLong(1);
						}
					}
				}

				conns.put(file.getName(), new ReplicaConn(dsn, conn, txseq));
			}
		}
	}

	public static ReplicaConn getReplica(String dbName) {
		if (conns.size() == 1 && (dbName == null || dbName.isEmpty())) {
			return conns.values().iterator().next();
		}
		return conns.get(dbName);
	}

	private static void startScheduler() {
		if (scheduler == null || scheduler.isShutdown()) {
			scheduler = Executors.newScheduledThreadPool(1, r -> {
				Thread t = new Thread(r);
				// make scheduler thread a daemon so it doesn't prevent JVM exit
				t.setDaemon(true);
				t.setName("ha-txseq-updater");
				return t;
			});
			scheduler.scheduleAtFixedRate(HAEmbeddedReplicasManager::updateTxSeq, 5, 5, TimeUnit.SECONDS);
		}
	}

	private static void updateTxSeq() {
		for (Map.Entry<String, ReplicaConn> entry : conns.entrySet()) {
			ReplicaConn replica = entry.getValue();
			try (Statement stmt = replica.conn.createStatement()) {
				try (ResultSet rs = stmt
						.executeQuery("SELECT received_seq FROM ha_stats ORDER BY updated_at DESC LIMIT 1")) {
					while (rs.next()) {
						replica.txseq = rs.getLong(1);
					}
				}
			} catch (SQLException e) {
				System.err.println("Error updating txseq for replica " + entry.getKey() + ": " + e.getMessage());
			}
		}
	}

	public static void stopScheduler() {
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
			try {
				replica.conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private static boolean isSqliteFile(File file) {
		if (file.length() < 100) {
			return false;
		}
		try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, "r")) {
			byte[] header = new byte[16];
			raf.read(header);
			String headerStr = new String(header, "UTF-8");
			return headerStr.startsWith("SQLite format 3");
		} catch (Exception e) {
			return false;
		}
	}
}
