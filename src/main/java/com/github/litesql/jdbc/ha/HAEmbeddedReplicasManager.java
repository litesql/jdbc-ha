package com.github.litesql.jdbc.ha;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.sqlite.SQLiteConfig.JournalMode;
import org.sqlite.SQLiteConfig.TempStore;
import org.sqlite.SQLiteDataSource;

public class HAEmbeddedReplicasManager {

	private static Map<String, Connection> conns = new HashMap<>();

	private static File tempExt;

	static {
		String extensionName = "ha-sync.so"; // Platform specific
		InputStream is = HAEmbeddedReplicasManager.class.getResourceAsStream("/ha-sync/linux/x86_64/" + extensionName);
		tempExt = new File(System.getProperty("java.io.tmpdir"), extensionName);
		try {
			Files.copy(is, tempExt.toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (Exception e) {
			e.printStackTrace();
		}
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
				SQLiteDataSource ds = new SQLiteDataSource();
				ds.setJournalMode(JournalMode.WAL.getValue());
				ds.setBusyTimeout(5000);
				ds.setTempStore(TempStore.MEMORY.getValue());
				ds.setUrl("jdbc:sqlite:" + file.getAbsolutePath());
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
				
				conns.put(file.getName(), conn);
			}
		}
	}

	public static Connection getConnection(String dbName) {
		if (conns.size() == 1 && (dbName == null || dbName.isEmpty())) {
			return conns.values().iterator().next();
		}
		return conns.get(dbName);
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
